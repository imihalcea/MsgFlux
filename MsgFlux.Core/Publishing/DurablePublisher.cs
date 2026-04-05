using System.Diagnostics;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

/// <summary>
/// Publisher that persists one inbox row per AtLeastOnce consumer before enqueuing.
/// NOTE: this implementation writes synchronously per publish; it will be replaced by a buffered
/// bulk-flush variant with WAL in the next iteration (point 2).
/// </summary>
public partial class DurablePublisher(
    IChannelRxTx channelRxTx,
    IMessageStore messageStore,
    ISerializer serializer,
    Registry registry,
    MsgFluxOptions options,
    ILogger<DurablePublisher> logger) : IPublish
{
    private static readonly ActivitySource ActivitySource = new("MsgFlux");

    public async Task PublishAsync<T>(T message, CancellationToken ct = default)
    {
        using var activity = ActivitySource.StartActivity(nameof(PublishAsync), ActivityKind.Producer);

        var headers = new Dictionary<string, string>();
        if (activity != null)
        {
            headers["traceparent"] = activity.Id ?? string.Empty;
            if (activity.TraceStateString != null)
                headers["tracestate"] = activity.TraceStateString;
        }

        var payload = serializer.Serialize(message);
        var messageId = Guid.NewGuid().ToString();
        var messageType = typeof(T);

        if (payload.Length > options.MaxPayloadSizeKb * 1024)
        {
            LogPayloadTooLarge(logger, payload.Length, messageType.Name, options.MaxPayloadSizeKb);
        }

        var consumers = registry.GetConsumers(messageType);
        var durableConsumers = consumers.Where(c => c.Semantics == Semantics.AtLeastOnce).ToList();

        // Persist one row per AtLeastOnce consumer (inbox pattern). If there are none, nothing to persist.
        if (durableConsumers.Count > 0)
        {
            var now = DateTimeOffset.UtcNow;
            var rows = durableConsumers
                .Select(c => new PersistedMessage
                {
                    MessageId = messageId,
                    ConsumerId = c.ConsumerId,
                    Payload = payload,
                    Headers = headers,
                    MessageType = messageType.Name,
                    CreatedAt = now
                })
                .ToList();

            await messageStore.PersistAsync(rows, ct);
            LogMessagePersisted(logger, messageId, messageType.Name, rows.Count);
        }

        // Enqueue once for in-memory dispatch (to all consumers — each consumer's semantic decides
        // whether the Engine touches the store for ACK/fail).
        var envelope = new Envelope(
            MessageId: messageId,
            Payload: payload,
            Headers: headers,
            MessageType: messageType.Name);

        var writer = channelRxTx.GetWriter(messageType);
        await writer.WriteAsync(envelope, ct);
    }

    [LoggerMessage(LogLevel.Debug, "Message {MessageId} of type {MessageType} persisted as {RowCount} inbox rows")]
    static partial void LogMessagePersisted(ILogger logger, string messageId, string messageType, int rowCount);

    [LoggerMessage(LogLevel.Warning, "Payload size {PayloadSize} for message {MessageType} exceeds {MaxPayloadSize}KB")]
    static partial void LogPayloadTooLarge(ILogger logger, int payloadSize, string messageType, int maxPayloadSize);
}
