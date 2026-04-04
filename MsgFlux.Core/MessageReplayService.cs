using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public partial class MessageReplayService(
    IMessageStore messageStore,
    IChannelRxTx channelRxTx,
    Registry registry,
    MsgFluxOptions options,
    ILogger<MessageReplayService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var messageTypes = registry.GetMessageTypes().ToDictionary(t => t.Name, t => t);

        if (messageTypes.Count == 0) return;

        var messages = await messageStore.FetchUnprocessedAsync(
            messageType: null,
            maxCount: 1000,
            staleProcessingTimeout: options.StaleProcessingTimeout,
            ct: stoppingToken);

        if (messages.Count == 0)
        {
            LogNoMessagesToReplay(logger);
            return;
        }

        LogReplayingMessages(logger, messages.Count);

        foreach (var message in messages)
        {
            if (stoppingToken.IsCancellationRequested) break;

            if (!messageTypes.TryGetValue(message.MessageType, out var type))
            {
                LogUnknownMessageType(logger, message.MessageType);
                continue;
            }

            // Dead-letter messages that exceeded max retries
            if (message.RetryCount >= options.MaxDeadLetterRetries)
            {
                await messageStore.DeadLetterAsync(message.MessageId,
                    $"Max retries ({options.MaxDeadLetterRetries}) exceeded", stoppingToken);
                LogMessageDeadLettered(logger, message.MessageId);
                continue;
            }

            var envelope = new Envelope(
                MessageId: message.MessageId,
                Payload: message.Payload,
                Headers: message.Headers,
                MessageType: message.MessageType);

            var writer = channelRxTx.GetWriter(type);
            await writer.WriteAsync(envelope, stoppingToken);
            LogMessageReplayed(logger, message.MessageId, message.MessageType);
        }
    }

    [LoggerMessage(LogLevel.Information, "No unprocessed messages to replay")]
    static partial void LogNoMessagesToReplay(ILogger logger);

    [LoggerMessage(LogLevel.Information, "Replaying {Count} unprocessed messages")]
    static partial void LogReplayingMessages(ILogger logger, int count);

    [LoggerMessage(LogLevel.Warning, "Unknown message type {MessageType} during replay, skipping")]
    static partial void LogUnknownMessageType(ILogger logger, string messageType);

    [LoggerMessage(LogLevel.Debug, "Replayed message {MessageId} of type {MessageType}")]
    static partial void LogMessageReplayed(ILogger logger, string messageId, string messageType);

    [LoggerMessage(LogLevel.Warning, "Message {MessageId} moved to dead-letter during replay")]
    static partial void LogMessageDeadLettered(ILogger logger, string messageId);
}
