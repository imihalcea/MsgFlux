namespace MsgFlux.Abstractions;

/// <summary>
/// Schedules messages for deferred delivery at a precise date. Distinct from <see cref="IPublish"/>:
/// "deliver now" vs "deliver at a date". Requires a durable path (an <see cref="IScheduleStore"/>);
/// scheduling for at-most-once-only message types is rejected.
/// </summary>
public interface ISchedule
{
    /// <summary>
    /// Schedules <paramref name="payload"/> for delivery at <paramref name="deliverAt"/>.
    /// A past date delivers as soon as possible. Returns the generated MessageId, usable for cancellation.
    /// </summary>
    Task<Guid> ScheduleAsync<T>(T payload, DateTimeOffset deliverAt, CancellationToken ct = default);

    /// <summary>
    /// Cancels a scheduled message that has not yet been promoted into the delivery pipeline.
    /// Best-effort: returns true if at least one row was cancelled, false if already promoted or unknown.
    /// </summary>
    Task<bool> CancelScheduledAsync(Guid messageId, CancellationToken ct = default);
}
