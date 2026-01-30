using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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

    public Engine(
        IServiceProvider serviceProvider,
        IChannelRxTx channelRxTx,
        ISerializer serializer,
        Registry registry,
        ILogger<Engine> logger,
        MsgFluxOptions options)
    {
        _serviceProvider = serviceProvider;
        _channelRxTx = channelRxTx;
        _serializer = serializer;
        _registry = registry;
        _logger = logger;
        _options = options;

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
                    await DispatchAsync(envelope, messageType, token);
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
        using var scope = _serviceProvider.CreateScope();
        var consumerType = typeof(IConsume<>).MakeGenericType(messageType);
        var consumers = scope.ServiceProvider.GetServices(consumerType).ToArray();
        
        if (consumers.Length == 0) return;

        object? message;
        try 
        {
            message = _serializer.Deserialize(envelope.Payload, messageType);
            if (message == null) 
            {
                LogDeserializationError(_logger, messageType.Name);
                return;
            }
        }
        catch (Exception ex)
        {
            LogDeserializationException(_logger, messageType.Name, ex);
            return;
        }

        // Link OTel Context
        var parentContext = ExtractContext(envelope.Headers);
        using var activity = ActivitySource.StartActivity("MsgFlux.Dispatch", ActivityKind.Consumer, parentContext);

        var tasks = new List<Task>(consumers.Length);
        foreach (var consumer in consumers)
        {
            if (consumer != null) tasks.Add(SafeExecuteConsumerAsync(consumer, message, consumerType, ct));
        }

        await Task.WhenAll(tasks);
    }

    private async Task SafeExecuteConsumerAsync(
        object consumer, 
        object message, 
        Type consumerType, 
        CancellationToken ct)
    {
        var consumerName = consumer.GetType().Name;
        try
        {
            await _pipeline.ExecuteAsync(async token =>
            {
                var method = consumerType.GetMethod("HandleAsync");
                if (method != null)
                {
                    try 
                    {
                        await (Task)method.Invoke(consumer, [message, token])!;
                    }
                    catch (TargetInvocationException ex)
                    {
                        if (ex.InnerException != null)
                        {
                            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                        }
                        throw;
                    }
                }
            }, ct);
        }
        catch (Exception ex)
        {
             LogConsumerError(_logger, consumerName, ex);
             Activity.Current?.SetStatus(ActivityStatusCode.Error, ex.Message);
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
}
