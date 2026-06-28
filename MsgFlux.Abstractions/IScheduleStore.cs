namespace MsgFlux.Abstractions;

/// <summary>
/// Cold storage for scheduled messages, kept distinct from <see cref="IMessageStore"/> so the
/// delivery engine stays unaware of scheduling. A scheduled message is persisted here on schedule,
/// then promoted into the hot path (<see cref="IMessageStore"/>) once due. One row per scheduled
/// publish, keyed by <see cref="ScheduledMessage.Id"/>; fan-out happens at promotion time.
/// </summary>
public interface IScheduleStore
{
    /// <summary>
    /// Persists scheduled messages. The store must enforce uniqueness on <see cref="ScheduledMessage.Id"/>.
    /// </summary>
    Task ScheduleAsync(IReadOnlyList<ScheduledMessage> messages, CancellationToken ct = default);

    /// <summary>
    /// Atomically claims and returns messages that are now due (DeliverAt &lt;= now), so a single
    /// promoter transfers each due row once across concurrent instances.
    /// </summary>
    Task<IReadOnlyList<ScheduledMessage>> FetchDueAsync(int maxCount = 100, CancellationToken ct = default);

    /// <summary>
    /// Marks rows transferred into the delivery path as promoted (history retained until purge).
    /// </summary>
    Task MarkPromotedAsync(IReadOnlyList<Guid> ids, CancellationToken ct = default);

    /// <summary>
    /// Cancels a scheduled message that has not yet been promoted. Returns true if it was cancelled.
    /// </summary>
    Task<bool> CancelScheduledAsync(Guid id, CancellationToken ct = default);

    /// <summary>
    /// Purges terminal rows (promoted or cancelled) older than <paramref name="olderThan"/>.
    /// </summary>
    Task<int> PurgeAsync(TimeSpan olderThan, CancellationToken ct = default);
}
