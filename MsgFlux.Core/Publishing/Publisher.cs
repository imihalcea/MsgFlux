using System.Diagnostics;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

/// <summary>
/// Single publisher that routes each message to the store corresponding to the consumer's semantic:
/// AtMostOnce → in-memory source; AtLeastOnce → durable IMessageStore.
/// One inbox row is created per registered consumer.
/// </summary>
public sealed partial class Publisher(
    InMemoryMessageSource inMemory,
    Registry registry,
    ISerializer serializer,
    MsgFluxOptions options,
    ILogger<Publisher> logger,
    DurableBuffer durableBuffer) : IPublish
{
    private static readonly ActivitySource ActivitySource = new("MsgFlux");

    public async Task PublishAsync<T>(T payload, CancellationToken ct = default)
    {
        using var activity = ActivitySource.StartActivity(nameof(PublishAsync), ActivityKind.Producer);

        var headers = new Dictionary<string, string>();
        if (activity != null)
        {
            headers["traceparent"] = activity.Id ?? string.Empty;
            if (activity.TraceStateString != null)
                headers["tracestate"] = activity.TraceStateString;
        }

        var payloadBytes = serializer.Serialize(payload);
        var messageType = typeof(T);

        if (payloadBytes.Length > options.MaxPayloadSizeKb * 1024)
        {
            LogPayloadTooLarge(logger, payloadBytes.Length, messageType.Name, options.MaxPayloadSizeKb);
        }

        var consumers = registry.GetConsumers(messageType);
        if (consumers.Count == 0)
        {
            LogNoConsumers(logger, messageType.Name);
            return;
        }

        var messageId = Guid.NewGuid().ToString();
        var now = DateTimeOffset.UtcNow;

        List<Message>? inMemoryRows = null;
        List<Message>? durableRows = null;

        foreach (var c in consumers)
        {
            var row = new Message
            {
                MessageId = messageId,
                ConsumerId = c.ConsumerId,
                Payload = payloadBytes,
                Headers = headers,
                MessageType = messageType.Name,
                CreatedAt = now
            };

            if (c.Semantics == Semantics.AtLeastOnce)
                (durableRows ??= new()).Add(row);
            else
                (inMemoryRows ??= new()).Add(row);
        }

        // Durable first: if it throws, we don't enqueue in-memory either (the caller sees the failure
        // and can retry; no partial delivery on the AtMostOnce side).
        if (durableRows is { Count: > 0 })
        {
            await durableBuffer.AddAsync(durableRows);
        }

        if (inMemoryRows is { Count: > 0 })
        {
            await inMemory.PersistAsync(inMemoryRows, ct);
        }
    }

    [LoggerMessage(LogLevel.Warning, "Payload size {PayloadSize} for message {MessageType} exceeds {MaxPayloadSize}KB")]
    static partial void LogPayloadTooLarge(ILogger logger, int payloadSize, string messageType, int maxPayloadSize);

    [LoggerMessage(LogLevel.Warning, "No consumer registered for {MessageType}; message dropped")]
    static partial void LogNoConsumers(ILogger logger, string messageType);
}
