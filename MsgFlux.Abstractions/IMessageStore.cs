namespace MsgFlux.Abstractions;

public interface IMessageStore
{
    /// <summary>
    /// Persists one inbox row per target consumer. The store must enforce uniqueness on (MessageId, ConsumerId).
    /// </summary>
    Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default);

    Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default);
    Task AcknowledgeAsync(Guid messageId, string consumerId, CancellationToken ct = default);
    Task AcknowledgeBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default);
    Task MarkAsFailedAsync(Guid messageId, string consumerId, string errorDetails, CancellationToken ct = default);
    Task DeadLetterAsync(Guid messageId, string consumerId, string reason, CancellationToken ct = default);

    /// <summary>
    /// Returns unprocessed rows (Pending or stuck Processing) across all consumers.
    /// Each returned PersistedMessage carries its ConsumerId so the caller can dispatch selectively.
    /// </summary>
    Task<IReadOnlyList<Message>> FetchUnprocessedAsync(
        string? messageType = null,
        int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null,
        CancellationToken ct = default);

    Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default);
}
