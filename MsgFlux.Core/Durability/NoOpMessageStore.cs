using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public sealed class NoOpMessageStore : IMessageStore
{
    public static readonly NoOpMessageStore Instance = new();

    public Task<string> PersistAsync(PersistedMessage message, CancellationToken ct = default) =>
        Task.FromResult(string.Empty);

    public Task MarkAsProcessingAsync(string messageId, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task AcknowledgeAsync(string messageId, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task MarkAsFailedAsync(string messageId, string errorDetails, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task DeadLetterAsync(string messageId, string reason, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task<IReadOnlyList<PersistedMessage>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<PersistedMessage>>(Array.Empty<PersistedMessage>());

    public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default) =>
        Task.FromResult(0);
}
