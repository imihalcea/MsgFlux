using System.Diagnostics;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

/// <summary>
/// Schedules messages for deferred delivery. Persists a single opaque row per scheduled publish to
/// the <see cref="IScheduleStore"/>; fan-out across consumers is deferred to the
/// <see cref="SchedulePromoter"/> at the due date. Scheduling is durable-only: it requires at least
/// one AtLeastOnce consumer registered for the message type.
/// </summary>
public sealed class Scheduler(
    Registry registry,
    ISerializer serializer,
    MsgFluxOptions options,
    IScheduleStore scheduleStore) : ISchedule
{
    public async Task<Guid> ScheduleAsync<T>(T payload, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        using var activity = MessageContent.ActivitySource.StartActivity(nameof(ScheduleAsync), ActivityKind.Producer);

        var messageType = typeof(T);
        if (!registry.GetConsumers(messageType).Any(c => c.Semantics == Semantics.AtLeastOnce))
        {
            throw new InvalidOperationException(
                $"Cannot schedule {messageType.Name}: scheduling requires durability, but no AtLeastOnce consumer " +
                "is registered for this message type. Register the consumer with Semantics.AtLeastOnce.");
        }

        var payloadBytes = MessageContent.SerializeValidated(serializer, options, payload);
        var headers = MessageContent.CaptureTraceHeaders(activity);
        var blob = serializer.Serialize(new ScheduledEnvelope(payloadBytes, headers));

        var id = Guid.CreateVersion7();
        var scheduled = new ScheduledMessage
        {
            Id = id,
            Type = messageType.FullName!,
            Msg = blob,
            DeliverAt = deliverAt.ToUniversalTime(),
            CreatedAt = DateTimeOffset.UtcNow
        };

        await scheduleStore.ScheduleAsync([scheduled], ct);
        return id;
    }

    public Task<bool> CancelScheduledAsync(Guid messageId, CancellationToken ct = default)
        => scheduleStore.CancelScheduledAsync(messageId, ct);
}
