using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Serialization;
using Polly;
using Polly.Retry;

namespace MsgFlux.Core;

public partial class EngineService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ISerializer _serializer;
    private readonly Registry _registry;
    private readonly ILogger<EngineService> _logger;
    private static readonly ActivitySource ActivitySource = new("MsgFlux");
    private readonly ResiliencePipeline _pipeline;
    private readonly MsgFluxOptions _options;
    private readonly IMessageSource[] _sources;
    private readonly Dictionary<string, Type> _messageTypesByName;
    private readonly SemaphoreSlim _globalThrottle;

    public EngineService(
        IServiceProvider serviceProvider,
        ISerializer serializer,
        Registry registry,
        IEnumerable<IMessageSource> sources,
        ILogger<EngineService> logger,
        MsgFluxOptions options)
    {
        _serviceProvider = serviceProvider;
        _serializer = serializer;
        _registry = registry;
        _logger = logger;
        _options = options;
        _sources = sources.ToArray();
        _messageTypesByName = registry.GetMessageTypes().ToDictionary(t => t.Name, t => t);

        var maxConcurrency = options.MaxDegreeOfParallelism > 0
            ? options.MaxDegreeOfParallelism
            : Environment.ProcessorCount;
        _globalThrottle = new SemaphoreSlim(maxConcurrency);

        _pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = options.MaxRetryAttempts,
                Delay = options.RetryDelay,
                BackoffType = DelayBackoffType.Exponential,
                OnRetry = args =>
                {
                    LogRetryAttempt(_logger, args.AttemptNumber, args.Outcome.Exception?.Message ?? "Unknown error");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public override void Dispose()
    {
        _globalThrottle.Dispose();
        base.Dispose();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tasks = _sources.Select(src => ConsumeSourceAsync(src, stoppingToken)).ToArray();
        return Task.WhenAll(tasks);
    }

    private async Task ConsumeSourceAsync(IMessageSource source, CancellationToken ct)
    {
        var parallelOptions = new ParallelOptions
        {
            CancellationToken = ct,
            MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism > 0
                ? _options.MaxDegreeOfParallelism
                : Environment.ProcessorCount
        };

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Parallel.ForEachAsync(source.StreamAsync(ct), parallelOptions, async (item, token) =>
                {
                    await _globalThrottle.WaitAsync(token);
                    try
                    {
                        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                        timeoutCts.CancelAfter(_options.StaleProcessingTimeout);
                        await DispatchAsync(item, timeoutCts.Token, token);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        // shutdown
                    }
                    catch (Exception ex)
                    {
                        LogProcessingError(_logger, item.Message.MessageType, ex);
                    }
                    finally
                    {
                        _globalThrottle.Release();
                    }
                });
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                LogSourceLoopError(_logger, ex);
                try { await Task.Delay(_options.ReplayInterval, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task DispatchAsync(DispatchItem item, CancellationToken dispatchToken, CancellationToken engineToken)
    {
        var msg = item.Message;

        if (!_messageTypesByName.TryGetValue(msg.MessageType, out var messageType))
        {
            LogUnknownMessageType(_logger, msg.MessageType);
            return;
        }

        var reg = _registry.GetConsumers(messageType).FirstOrDefault(c => c.ConsumerId == msg.ConsumerId);
        if (reg is null)
        {
            LogUnknownConsumer(_logger, msg.ConsumerId, msg.MessageType);
            return;
        }

        if (item.OnProcessing is not null)
            await SafeInvoke(() => item.OnProcessing(engineToken), msg, "OnProcessing");

        var message = DeserializeMessage(msg, messageType);
        if (message is null)
        {
            if (item.OnFail is not null)
                await SafeInvoke(() => item.OnFail("Deserialization failed", engineToken), msg, "OnFail");
            return;
        }

        var parentContext = ExtractContext(msg.Headers);
        using var activity = ActivitySource.StartActivity("MsgFlux.Dispatch", ActivityKind.Consumer, parentContext);

        using var scope = _serviceProvider.CreateScope();
        var consumer = ResolveConsumer(scope, messageType, reg.ConsumerType);
        if (consumer is null)
        {
            LogConsumerNotResolved(_logger, reg.ConsumerType.Name);
            return;
        }

        var invoker = _registry.GetInvoker(messageType);
        var outcome = await InvokeConsumerAsync(consumer, message, invoker, dispatchToken, engineToken);

        switch (outcome)
        {
            case ConsumerOutcome.Success:
                if (item.OnAck is not null)
                    await SafeInvoke(() => item.OnAck(engineToken), msg, "OnAck");
                break;
            case ConsumerOutcome.TimedOut:
                LogProcessingTimeout(_logger, msg.MessageId, _options.StaleProcessingTimeout);
                if (item.OnFail is not null)
                    await SafeInvoke(() => item.OnFail($"Processing timed out after {_options.StaleProcessingTimeout}", engineToken), msg, "OnFail");
                break;
            case ConsumerOutcome.Failed:
                if (item.OnFail is not null)
                    await SafeInvoke(() => item.OnFail("Consumer failed after retries", engineToken), msg, "OnFail");
                break;
        }
    }

    private static object? ResolveConsumer(IServiceScope scope, Type messageType, Type consumerType)
    {
        var serviceType = typeof(MsgFlux.Abstractions.IConsume<>).MakeGenericType(messageType);
        foreach (var svc in scope.ServiceProvider.GetServices(serviceType))
        {
            if (svc?.GetType() == consumerType) return svc;
        }
        return null;
    }

    private object? DeserializeMessage(Message msg, Type messageType)
    {
        try
        {
            var result = _serializer.Deserialize(msg.Payload, messageType);
            if (result is null) LogDeserializationError(_logger, messageType.Name);
            return result;
        }
        catch (Exception ex)
        {
            LogDeserializationException(_logger, messageType.Name, ex);
            return null;
        }
    }

    private async Task<ConsumerOutcome> InvokeConsumerAsync(
        object consumer,
        object message,
        Func<object, object, CancellationToken, Task> invoker,
        CancellationToken dispatchToken,
        CancellationToken engineToken)
    {
        var consumerName = consumer.GetType().Name;
        try
        {
            await _pipeline.ExecuteAsync(async token =>
            {
                await invoker(consumer, message, token);
            }, dispatchToken);
            return ConsumerOutcome.Success;
        }
        catch (OperationCanceledException) when (engineToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (dispatchToken.IsCancellationRequested)
        {
            Activity.Current?.SetStatus(ActivityStatusCode.Error, "timeout");
            return ConsumerOutcome.TimedOut;
        }
        catch (Exception ex)
        {
            LogConsumerError(_logger, consumerName, ex);
            Activity.Current?.SetStatus(ActivityStatusCode.Error, ex.Message);
            return ConsumerOutcome.Failed;
        }
    }

    private async Task SafeInvoke(Func<Task> action, Message msg, string op)
    {
        try { await action(); }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogCallbackError(_logger, op, msg.MessageId, msg.ConsumerId, ex);
        }
    }

    private static ActivityContext ExtractContext(Dictionary<string, string> headers)
    {
        if (headers.TryGetValue("traceparent", out var traceparent) && !string.IsNullOrEmpty(traceparent))
        {
            var tracestate = headers.TryGetValue("tracestate", out var ts) ? ts : null;
            if (ActivityContext.TryParse(traceparent, tracestate, out var context))
                return context;
        }
        return default;
    }

    private enum ConsumerOutcome { Success, Failed, TimedOut }

    [LoggerMessage(LogLevel.Error, "Error processing message of type {messageType}")]
    static partial void LogProcessingError(ILogger<EngineService> logger, string messageType, Exception ex);

    [LoggerMessage(LogLevel.Critical, "Critical error in source loop")]
    static partial void LogSourceLoopError(ILogger<EngineService> logger, Exception ex);

    [LoggerMessage(LogLevel.Warning, "Unknown message type {messageType}; skipping")]
    static partial void LogUnknownMessageType(ILogger<EngineService> logger, string messageType);

    [LoggerMessage(LogLevel.Warning, "No consumer registered with id {consumerId} for {messageType}; skipping")]
    static partial void LogUnknownConsumer(ILogger<EngineService> logger, string consumerId, string messageType);

    [LoggerMessage(LogLevel.Error, "Consumer {consumerName} was not resolvable from DI")]
    static partial void LogConsumerNotResolved(ILogger<EngineService> logger, string consumerName);

    [LoggerMessage(LogLevel.Error, "Failed to deserialize message of type {messageType}")]
    static partial void LogDeserializationError(ILogger<EngineService> logger, string messageType);

    [LoggerMessage(LogLevel.Error, "Exception during deserialization of message type {messageType}")]
    static partial void LogDeserializationException(ILogger<EngineService> logger, string messageType, Exception ex);

    [LoggerMessage(LogLevel.Error, "Consumer {consumerName} failed after retries")]
    static partial void LogConsumerError(ILogger<EngineService> logger, string consumerName, Exception ex);

    [LoggerMessage(LogLevel.Warning, "Retry attempt {attemptNumber} due to: {errorMessage}")]
    static partial void LogRetryAttempt(ILogger<EngineService> logger, int attemptNumber, string errorMessage);

    [LoggerMessage(LogLevel.Warning, "Message {messageId} processing timed out after {timeout}")]
    static partial void LogProcessingTimeout(ILogger<EngineService> logger, string messageId, TimeSpan timeout);

    [LoggerMessage(LogLevel.Error, "Source callback {operation} failed for message {messageId} / consumer {consumerId}")]
    static partial void LogCallbackError(ILogger<EngineService> logger, string operation, string messageId, string consumerId, Exception ex);
}
