using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public partial class Engine(
    IServiceProvider serviceProvider,
    IChannelRxTx channelRxTx,
    ISerializer serializer,
    Registry registry,
    ILogger<Engine> logger) : BackgroundService
{
    private static readonly ActivitySource ActivitySource = new("Flux");
    private readonly List<Task> _processingTasks = new();

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var messageTypes = registry.GetMessageTypes();

        foreach (var messageType in messageTypes)
        {
            LogDiscoveredConsumerForMessageType(logger, messageType.Name);
            _processingTasks.Add(ProcessChannelAsync(messageType, stoppingToken));
        }

        return Task.WhenAll(_processingTasks);
    }

    private async Task ProcessChannelAsync(Type messageType, CancellationToken ct)
    {
        var reader = channelRxTx.GetReader(messageType);

        try
        {
            while (await reader.WaitToReadAsync(ct))
            {
                while (reader.TryRead(out var envelope))
                {
                    try
                    {
                        await DispatchAsync(envelope, messageType, ct);
                    }
                    catch (Exception ex)
                    {
                        LogProcessingError(logger, messageType.Name, ex);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown
        }
        catch (Exception ex)
        {
             LogChannelLoopError(logger, messageType.Name, ex);
        }
    }

    private async Task DispatchAsync(Envelope envelope, Type messageType, CancellationToken ct)
    {
        using var scope = serviceProvider.CreateScope();
        var consumerType = typeof(IConsume<>).MakeGenericType(messageType);
        var consumers = scope.ServiceProvider.GetServices(consumerType).ToArray();
        
        if (consumers.Length == 0) return;

        object? message;
        try 
        {
            message = serializer.Deserialize(envelope.Payload, messageType);
            if (message == null) 
            {
                LogDeserializationError(logger, messageType.Name);
                return;
            }
        }
        catch (Exception ex)
        {
            LogDeserializationException(logger, messageType.Name, ex);
            return;
        }

        // Link OTel Context
        var parentContext = ExtractContext(envelope.Headers);
        using var activity = ActivitySource.StartActivity("Flux.Process", ActivityKind.Consumer, parentContext);

        var tasks = new List<Task>(consumers.Length);
        foreach (var consumer in consumers)
        {
            // We wrap each consumer execution in a safe block to ensure one failure doesn't stop others
            // This is a preliminary step before Polly
            tasks.Add(SafeExecuteConsumerAsync(consumer, message, consumerType, ct));
        }

        await Task.WhenAll(tasks);
    }

    private async Task SafeExecuteConsumerAsync(object consumer, object message, Type consumerType, CancellationToken ct)
    {
        try
        {
            var method = consumerType.GetMethod("HandleAsync");
            if (method != null)
            {
                await (Task)method.Invoke(consumer, [message, ct])!;
            }
        }
        catch (Exception ex)
        {
             LogConsumerError(logger, consumer.GetType().Name, ex);
             // In Step 4, Polly will handle retries here.
             // For now, we swallow the exception to protect other consumers.
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

    [LoggerMessage(LogLevel.Error, "Consumer {consumerName} failed")]
    static partial void LogConsumerError(ILogger<Engine> logger, string consumerName, Exception ex);
}
