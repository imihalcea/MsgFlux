using System.Diagnostics;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;
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
    public async Task PublishAsync<T>(T payload, CancellationToken ct = default)
    {
        using var activity = MessageContent.ActivitySource.StartActivity(nameof(PublishAsync), ActivityKind.Producer);

        var payloadBytes = MessageContent.SerializeValidated(serializer, options, payload);
        var headers = MessageContent.CaptureTraceHeaders(activity);
        var messageType = typeof(T);

        var consumers = registry.GetConsumers(messageType);
        if (consumers.Count == 0)
        {
            LogNoConsumers(logger, messageType.Name);
            return;
        }

        var messageId = Guid.CreateVersion7();
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
                MessageType = messageType.FullName!,
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
            await durableBuffer.AddAsync(durableRows, ct);
        }

        if (inMemoryRows is { Count: > 0 })
        {
            await inMemory.PersistAsync(inMemoryRows, ct);
        }
    }

    [LoggerMessage(LogLevel.Warning, "No consumer registered for {MessageType}; message dropped")]
    static partial void LogNoConsumers(ILogger logger, string messageType);
}
