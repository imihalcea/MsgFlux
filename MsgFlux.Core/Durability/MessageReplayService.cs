using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.RxTx;

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

        while (!stoppingToken.IsCancellationRequested)
        {
            await ReplayCycleAsync(messageTypes, stoppingToken);
            await Task.Delay(options.ReplayInterval, stoppingToken);
        }
    }

    private async Task ReplayCycleAsync(Dictionary<string, Type> messageTypes, CancellationToken ct)
    {
        var messages = await messageStore.FetchUnprocessedAsync(
            messageType: null,
            maxCount: 1000,
            staleProcessingTimeout: options.StaleProcessingTimeout,
            ct: ct);

        if (messages.Count == 0) return;

        LogReplayingMessages(logger, messages.Count);

        foreach (var message in messages)
        {
            if (ct.IsCancellationRequested) break;

            if (!messageTypes.TryGetValue(message.MessageType, out var type))
            {
                LogUnknownMessageType(logger, message.MessageType);
                continue;
            }

            if (message.RetryCount >= options.MaxDeadLetterRetries)
            {
                await messageStore.DeadLetterAsync(message.MessageId, message.ConsumerId,
                    $"Max retries ({options.MaxDeadLetterRetries}) exceeded", ct);
                LogMessageDeadLettered(logger, message.MessageId, message.ConsumerId);
                continue;
            }

            var envelope = new Envelope(
                MessageId: message.MessageId,
                Payload: message.Payload,
                Headers: message.Headers,
                MessageType: message.MessageType,
                TargetConsumerId: message.ConsumerId);

            var writer = channelRxTx.GetWriter(type);

            // TryWrite + abandon on full channel: the replay cycle will retry on the next tick.
            // Avoids breaking backpressure of the in-memory channel.
            if (!writer.TryWrite(envelope))
            {
                LogReplayBackpressure(logger, message.MessageId, type.Name);
                break;
            }
            LogMessageReplayed(logger, message.MessageId, message.MessageType, message.ConsumerId);
        }
    }

    [LoggerMessage(LogLevel.Information, "Replaying {Count} unprocessed messages")]
    static partial void LogReplayingMessages(ILogger logger, int count);

    [LoggerMessage(LogLevel.Warning, "Unknown message type {MessageType} during replay, skipping")]
    static partial void LogUnknownMessageType(ILogger logger, string messageType);

    [LoggerMessage(LogLevel.Debug, "Replayed message {MessageId} of type {MessageType} for consumer {ConsumerId}")]
    static partial void LogMessageReplayed(ILogger logger, string messageId, string messageType, string consumerId);

    [LoggerMessage(LogLevel.Warning, "Message {MessageId} / consumer {ConsumerId} moved to dead-letter during replay")]
    static partial void LogMessageDeadLettered(ILogger logger, string messageId, string consumerId);

    [LoggerMessage(LogLevel.Debug, "Channel full, stopping replay batch at message {MessageId} for {MessageType}")]
    static partial void LogReplayBackpressure(ILogger logger, string messageId, string messageType);
}
