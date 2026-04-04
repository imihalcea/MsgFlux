namespace MsgFlux.Abstractions;

public interface IMessageStore
{
    Task<string> PersistAsync(PersistedMessage message, CancellationToken ct = default);
    Task MarkAsProcessingAsync(string messageId, CancellationToken ct = default);
    Task AcknowledgeAsync(string messageId, CancellationToken ct = default);
    Task MarkAsFailedAsync(string messageId, string errorDetails, CancellationToken ct = default);
    Task DeadLetterAsync(string messageId, string reason, CancellationToken ct = default);
    Task<IReadOnlyList<PersistedMessage>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default);
    Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default);
}
