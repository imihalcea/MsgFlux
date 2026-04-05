using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public sealed class NoOpMessageStore : IMessageStore
{
    public static readonly NoOpMessageStore Instance = new();

    public Task PersistAsync(IReadOnlyList<PersistedMessage> messages, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task MarkAsProcessingAsync(string messageId, string consumerId, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task AcknowledgeAsync(string messageId, string consumerId, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task MarkAsFailedAsync(string messageId, string consumerId, string errorDetails, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task DeadLetterAsync(string messageId, string consumerId, string reason, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task<IReadOnlyList<PersistedMessage>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<PersistedMessage>>(Array.Empty<PersistedMessage>());

    public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default) =>
        Task.FromResult(0);
}
