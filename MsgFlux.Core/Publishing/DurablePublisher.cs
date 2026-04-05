using System.Diagnostics;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public partial class DurablePublisher(
    IChannelRxTx channelRxTx,
    IMessageStore messageStore,
    ISerializer serializer,
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

        if (payload.Length > options.MaxPayloadSizeKb * 1024)
        {
            LogPayloadTooLarge(logger, payload.Length, typeof(T).Name, options.MaxPayloadSizeKb);
        }

        var persisted = new PersistedMessage
        {
            MessageId = messageId,
            Payload = payload,
            Headers = headers,
            MessageType = typeof(T).Name,
            CreatedAt = DateTimeOffset.UtcNow
        };

        // Persist first — if store fails, message is NOT enqueued
        await messageStore.PersistAsync(persisted, ct);
        LogMessagePersisted(logger, messageId, typeof(T).Name);

        // Enqueue with the same MessageId
        var envelope = new Envelope(
            MessageId: messageId,
            Payload: payload,
            Headers: headers,
            MessageType: typeof(T).Name);

        var writer = channelRxTx.GetWriter(typeof(T));
        await writer.WriteAsync(envelope, ct);
    }

    [LoggerMessage(LogLevel.Debug, "Message {MessageId} of type {MessageType} persisted before enqueue")]
    static partial void LogMessagePersisted(ILogger logger, string messageId, string messageType);

    [LoggerMessage(LogLevel.Warning, "Payload size {PayloadSize} for message {MessageType} exceeds {MaxPayloadSize}KB")]
    static partial void LogPayloadTooLarge(ILogger logger, int payloadSize, string messageType, int maxPayloadSize);
}
