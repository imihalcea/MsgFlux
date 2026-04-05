using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;
using Polly;
using Polly.Retry;

namespace MsgFlux.Core;

public partial class EngineService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IChannelRxTx _channelRxTx;
    private readonly ISerializer _serializer;
    private readonly Registry _registry;
    private readonly ILogger<EngineService> _logger;
    private static readonly ActivitySource ActivitySource = new("MsgFlux");
    private readonly List<Task> _processingTasks = new();
    private readonly ResiliencePipeline _pipeline;
    private readonly MsgFluxOptions _options;
    private readonly IMessageStore _messageStore;

    public EngineService(
        IServiceProvider serviceProvider,
        IChannelRxTx channelRxTx,
        ISerializer serializer,
        Registry registry,
        ILogger<EngineService> logger,
        MsgFluxOptions options,
        IMessageStore? messageStore = null)
    {
        _serviceProvider = serviceProvider;
        _channelRxTx = channelRxTx;
        _serializer = serializer;
        _registry = registry;
        _logger = logger;
        _options = options;
        _messageStore = messageStore ?? NoOpMessageStore.Instance;

        _pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.FromMilliseconds(200),
                BackoffType = DelayBackoffType.Exponential,
                OnRetry = args =>
                {
                    LogRetryAttempt(_logger, args.AttemptNumber, args.Outcome.Exception?.Message ?? "Unknown error");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var messageTypes = _registry.GetMessageTypes();

        foreach (var messageType in messageTypes)
        {
            LogDiscoveredConsumerForMessageType(_logger, messageType.Name);
            _processingTasks.Add(ProcessChannelAsync(messageType, stoppingToken));
        }

        return Task.WhenAll(_processingTasks);
    }

    private async Task ProcessChannelAsync(Type messageType, CancellationToken ct)
    {
        var reader = _channelRxTx.GetReader(messageType);

        try
        {
            var parallelOptions = new ParallelOptions
            {
                CancellationToken = ct,
                MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism > 0 ? _options.MaxDegreeOfParallelism : Environment.ProcessorCount
            };

            await Parallel.ForEachAsync(reader.ReadAllAsync(ct), parallelOptions, async (envelope, token) =>
            {
                try
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                    timeoutCts.CancelAfter(_options.StaleProcessingTimeout);
                    await DispatchAsync(envelope, messageType, timeoutCts.Token, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // Engine is shutting down — swallow.
                }
                catch (Exception ex)
                {
                    LogProcessingError(_logger, messageType.Name, ex);
                }
            });
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown
        }
        catch (Exception ex)
        {
            LogChannelLoopError(_logger, messageType.Name, ex);
        }
    }

    private async Task DispatchAsync(Envelope envelope, Type messageType, CancellationToken dispatchToken, CancellationToken engineToken)
    {
        var registrations = _registry.GetConsumers(messageType);
        if (registrations.Count == 0) return;

        // Selective dispatch: if TargetConsumerId is set, filter to that single consumer (replay path).
        var targeted = envelope.TargetConsumerId is not null
            ? registrations.Where(r => r.ConsumerId == envelope.TargetConsumerId).ToArray()
            : registrations.ToArray();

        if (targeted.Length == 0) return;

        var message = DeserializeMessage(envelope, messageType);
        if (message == null)
        {
            // Deserialization failed → mark only the AtLeastOnce slices as failed.
            foreach (var reg in targeted.Where(r => r.Semantics == Semantics.AtLeastOnce))
            {
                await SafeStoreOperationAsync(envelope.MessageId, reg.ConsumerId, "MarkAsFailedAsync (deserialize)",
                    () => _messageStore.MarkAsFailedAsync(envelope.MessageId, reg.ConsumerId, "Deserialization failed", engineToken));
            }
            return;
        }

        var parentContext = ExtractContext(envelope.Headers);
        using var activity = ActivitySource.StartActivity("MsgFlux.Dispatch", ActivityKind.Consumer, parentContext);

        using var scope = _serviceProvider.CreateScope();
        var instancesByType = ResolveConsumerInstances(scope, messageType);
        var invoker = _registry.GetInvoker(messageType);

        var tasks = new List<Task>(targeted.Length);
        foreach (var reg in targeted)
        {
            if (!instancesByType.TryGetValue(reg.ConsumerType, out var instance)) continue;
            tasks.Add(DispatchToConsumerAsync(envelope, reg, instance, message, invoker, dispatchToken, engineToken));
        }
        await Task.WhenAll(tasks);
    }

    private async Task DispatchToConsumerAsync(
        Envelope envelope,
        ConsumerRegistration reg,
        object instance,
        object message,
        Func<object, object, CancellationToken, Task> invoker,
        CancellationToken dispatchToken,
        CancellationToken engineToken)
    {
        var isDurable = reg.Semantics == Semantics.AtLeastOnce;

        if (isDurable)
        {
            await SafeStoreOperationAsync(envelope.MessageId, reg.ConsumerId, "MarkAsProcessingAsync",
                () => _messageStore.MarkAsProcessingAsync(envelope.MessageId, reg.ConsumerId, engineToken));
        }

        var outcome = await SafeExecuteConsumerAsync(instance, message, invoker, dispatchToken, engineToken);

        if (!isDurable) return;

        string op;
        Func<Task> action;
        switch (outcome)
        {
            case ConsumerOutcome.Success:
                op = "AcknowledgeAsync";
                action = () => _messageStore.AcknowledgeAsync(envelope.MessageId, reg.ConsumerId, engineToken);
                break;
            case ConsumerOutcome.TimedOut:
                LogProcessingTimeout(_logger, envelope.MessageId, _options.StaleProcessingTimeout);
                op = "MarkAsFailedAsync (timeout)";
                action = () => _messageStore.MarkAsFailedAsync(envelope.MessageId, reg.ConsumerId,
                    $"Processing timed out after {_options.StaleProcessingTimeout}", engineToken);
                break;
            default:
                op = "MarkAsFailedAsync";
                action = () => _messageStore.MarkAsFailedAsync(envelope.MessageId, reg.ConsumerId,
                    "Consumer failed after retries", engineToken);
                break;
        }
        await SafeStoreOperationAsync(envelope.MessageId, reg.ConsumerId, op, action);
    }

    private enum ConsumerOutcome { Success, Failed, TimedOut }

    private Dictionary<Type, object> ResolveConsumerInstances(IServiceScope scope, Type messageType)
    {
        var consumerServiceType = _registry.GetConsumerServiceType(messageType);
        var services = scope.ServiceProvider.GetServices(consumerServiceType);
        var map = new Dictionary<Type, object>();
        foreach (var svc in services)
        {
            if (svc is null) continue;
            // Last-write-wins if duplicates (shouldn't happen with registry dedup).
            map[svc.GetType()] = svc;
        }
        return map;
    }

    private object? DeserializeMessage(Envelope envelope, Type messageType)
    {
        try
        {
            var message = _serializer.Deserialize(envelope.Payload, messageType);
            if (message == null)
                LogDeserializationError(_logger, messageType.Name);
            return message;
        }
        catch (Exception ex)
        {
            LogDeserializationException(_logger, messageType.Name, ex);
            return null;
        }
    }

    private async Task SafeStoreOperationAsync(string messageId, string consumerId, string operation, Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogMessageStoreError(_logger, messageId, consumerId, operation, ex);
        }
    }

    private async Task<ConsumerOutcome> SafeExecuteConsumerAsync(
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
            // Dispatch token cancelled but engine still running → timeout.
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

    private static ActivityContext ExtractContext(Dictionary<string, string> headers)
    {
        if (headers.TryGetValue("traceparent", out var traceparent) && !string.IsNullOrEmpty(traceparent))
        {
            var tracestate = headers.TryGetValue("tracestate", out var ts) ? ts : null;
            if (ActivityContext.TryParse(traceparent, tracestate, out var context))
            {
                return context;
            }
        }
        return default;
    }

    [LoggerMessage(LogLevel.Information, "Discovered consumer for message type: {messageType}")]
    static partial void LogDiscoveredConsumerForMessageType(ILogger<EngineService> logger, string messageType);

    [LoggerMessage(LogLevel.Error, "Error processing message of type {messageType}")]
    static partial void LogProcessingError(ILogger<EngineService> logger, string messageType, Exception ex);

    [LoggerMessage(LogLevel.Critical, "Critical error in channel loop for {messageType}")]
    static partial void LogChannelLoopError(ILogger<EngineService> logger, string messageType, Exception ex);

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

    [LoggerMessage(LogLevel.Error, "Message store operation failed for message {messageId} / consumer {consumerId}: {operation}")]
    static partial void LogMessageStoreError(ILogger<EngineService> logger, string messageId, string consumerId, string operation, Exception ex);
}
