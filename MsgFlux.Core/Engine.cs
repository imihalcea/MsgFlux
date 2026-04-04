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

public partial class Engine : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IChannelRxTx _channelRxTx;
    private readonly ISerializer _serializer;
    private readonly Registry _registry;
    private readonly ILogger<Engine> _logger;
    private static readonly ActivitySource ActivitySource = new("MsgFlux");
    private readonly List<Task> _processingTasks = new();
    private readonly ResiliencePipeline _pipeline;
    private readonly MsgFluxOptions _options;
    private readonly IMessageStore? _messageStore;

    public Engine(
        IServiceProvider serviceProvider,
        IChannelRxTx channelRxTx,
        ISerializer serializer,
        Registry registry,
        ILogger<Engine> logger,
        MsgFluxOptions options,
        IMessageStore? messageStore = null)
    {
        _serviceProvider = serviceProvider;
        _channelRxTx = channelRxTx;
        _serializer = serializer;
        _registry = registry;
        _logger = logger;
        _options = options;
        _messageStore = messageStore;

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
            // Use Parallel.ForEachAsync to process messages concurrently
            // MaxDegreeOfParallelism can be configured via options or defaulted to ProcessorCount
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
                    try
                    {
                        await DispatchAsync(envelope, messageType, timeoutCts.Token);
                    }
                    catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !token.IsCancellationRequested)
                    {
                        LogProcessingTimeout(_logger, envelope.MessageId, _options.StaleProcessingTimeout);
                        if (_messageStore != null)
                            await _messageStore.MarkAsFailedAsync(envelope.MessageId,
                                $"Processing timed out after {_options.StaleProcessingTimeout}", token);
                    }
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

    private async Task DispatchAsync(Envelope envelope, Type messageType, CancellationToken ct)
    {
        if (_messageStore != null)
        {
            await _messageStore.MarkAsProcessingAsync(envelope.MessageId, ct);
        }

        using var scope = _serviceProvider.CreateScope();
        var consumerServiceType = _registry.GetConsumerServiceType(messageType);
        var consumers = scope.ServiceProvider.GetServices(consumerServiceType).ToArray();

        if (consumers.Length == 0)
        {
            if (_messageStore != null)
                await _messageStore.AcknowledgeAsync(envelope.MessageId, ct);
            return;
        }

        object? message;
        try
        {
            message = _serializer.Deserialize(envelope.Payload, messageType);
            if (message == null)
            {
                LogDeserializationError(_logger, messageType.Name);
                if (_messageStore != null)
                    await _messageStore.MarkAsFailedAsync(envelope.MessageId, "Deserialization returned null", ct);
                return;
            }
        }
        catch (Exception ex)
        {
            LogDeserializationException(_logger, messageType.Name, ex);
            if (_messageStore != null)
                await _messageStore.MarkAsFailedAsync(envelope.MessageId, ex.Message, ct);
            return;
        }

        // Link OTel Context
        var parentContext = ExtractContext(envelope.Headers);
        using var activity = ActivitySource.StartActivity("MsgFlux.Dispatch", ActivityKind.Consumer, parentContext);

        var invoker = _registry.GetInvoker(messageType);
        var results = new List<Task<bool>>(consumers.Length);
        foreach (var consumer in consumers)
        {
            if (consumer != null) results.Add(SafeExecuteConsumerAsync(consumer, message, invoker, ct));
        }

        await Task.WhenAll(results);

        if (_messageStore != null)
        {
            var allSucceeded = results.All(t => t.Result);
            if (allSucceeded)
            {
                await _messageStore.AcknowledgeAsync(envelope.MessageId, ct);
            }
            else
            {
                await _messageStore.MarkAsFailedAsync(envelope.MessageId,
                    "One or more consumers failed after retries", ct);
            }
        }
    }

    private async Task<bool> SafeExecuteConsumerAsync(
        object consumer,
        object message,
        Func<object, object, CancellationToken, Task> invoker,
        CancellationToken ct)
    {
        var consumerName = consumer.GetType().Name;
        try
        {
            await _pipeline.ExecuteAsync(async token =>
            {
                await invoker(consumer, message, token);
            }, ct);
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogConsumerError(_logger, consumerName, ex);
            Activity.Current?.SetStatus(ActivityStatusCode.Error, ex.Message);
            return false;
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
    static partial void LogDiscoveredConsumerForMessageType(ILogger<Engine> logger, string messageType);

    [LoggerMessage(LogLevel.Error, "Error processing message of type {messageType}")]
    static partial void LogProcessingError(ILogger<Engine> logger, string messageType, Exception ex);

    [LoggerMessage(LogLevel.Critical, "Critical error in channel loop for {messageType}")]
    static partial void LogChannelLoopError(ILogger<Engine> logger, string messageType, Exception ex);
    
    [LoggerMessage(LogLevel.Error, "Failed to deserialize message of type {messageType}")]
    static partial void LogDeserializationError(ILogger<Engine> logger, string messageType);

    [LoggerMessage(LogLevel.Error, "Exception during deserialization of message type {messageType}")]
    static partial void LogDeserializationException(ILogger<Engine> logger, string messageType, Exception ex);

    [LoggerMessage(LogLevel.Error, "Consumer {consumerName} failed after retries")]
    static partial void LogConsumerError(ILogger<Engine> logger, string consumerName, Exception ex);

    [LoggerMessage(LogLevel.Warning, "Retry attempt {attemptNumber} due to: {errorMessage}")]
    static partial void LogRetryAttempt(ILogger<Engine> logger, int attemptNumber, string errorMessage);

    [LoggerMessage(LogLevel.Warning, "Message {messageId} moved to dead-letter")]
    static partial void LogMessageDeadLettered(ILogger<Engine> logger, string messageId);

    [LoggerMessage(LogLevel.Warning, "Message {messageId} processing timed out after {timeout}")]
    static partial void LogProcessingTimeout(ILogger<Engine> logger, string messageId, TimeSpan timeout);
}
