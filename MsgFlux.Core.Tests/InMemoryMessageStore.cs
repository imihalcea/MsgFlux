using System.Collections.Concurrent;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

/// <summary>
/// In-memory IMessageStore implementation for unit testing.
/// Keyed by (MessageId, ConsumerId).
/// </summary>
public class InMemoryMessageStore : IMessageStore
{
    public ConcurrentDictionary<(Guid MessageId, string ConsumerId), Message> Messages { get; } = new();

    public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
    {
        foreach (var m in messages)
        {
            Messages.TryAdd((m.MessageId, m.ConsumerId), m);
        }
        return Task.CompletedTask;
    }

    public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
    {
        var key = (messageId, consumerId);
        if (Messages.TryGetValue(key, out var msg))
        {
            Messages[key] = msg with { State = MessageState.Processing, ProcessedAt = DateTimeOffset.UtcNow };
        }
        return Task.CompletedTask;
    }

    public async Task MarkAsProcessingBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
    {
        foreach (var (messageId, consumerId) in items)
            await MarkAsProcessingAsync(messageId, consumerId, ct);
    }

    public Task AcknowledgeAsync(Guid messageId, string consumerId, CancellationToken ct = default)
    {
        var key = (messageId, consumerId);
        if (Messages.TryGetValue(key, out var msg))
        {
            Messages[key] = msg with { State = MessageState.Completed, ProcessedAt = DateTimeOffset.UtcNow };
        }
        return Task.CompletedTask;
    }

    public async Task AcknowledgeBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
    {
        foreach (var (messageId, consumerId) in items)
            await AcknowledgeAsync(messageId, consumerId, ct);
    }

    public Task MarkAsFailedAsync(Guid messageId, string consumerId, string errorDetails, CancellationToken ct = default)
    {
        var key = (messageId, consumerId);
        if (Messages.TryGetValue(key, out var msg))
        {
            Messages[key] = msg with
            {
                State = MessageState.Failed,
                ErrorDetails = errorDetails,
                RetryCount = msg.RetryCount + 1
            };
        }
        return Task.CompletedTask;
    }

    public Task DeadLetterAsync(Guid messageId, string consumerId, string reason, CancellationToken ct = default)
    {
        var key = (messageId, consumerId);
        if (Messages.TryGetValue(key, out var msg))
        {
            Messages[key] = msg with { State = MessageState.DeadLettered, ErrorDetails = reason };
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<Message>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow;
        var results = Messages.Values
            .Where(m =>
                m.State == MessageState.Pending ||
                m.State == MessageState.Failed ||
                (m.State == MessageState.Processing && staleProcessingTimeout.HasValue &&
                 m.ProcessedAt.HasValue && now - m.ProcessedAt.Value > staleProcessingTimeout.Value))
            .Where(m => messageType == null || m.MessageType == messageType)
            .OrderBy(m => m.CreatedAt)
            .Take(maxCount)
            .ToList();

        return Task.FromResult<IReadOnlyList<Message>>(results);
    }

    public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
    {
        var cutoff = DateTimeOffset.UtcNow - olderThan;
        var toRemove = Messages.Where(kv => kv.Value.State == MessageState.Completed && kv.Value.CreatedAt < cutoff)
            .Select(kv => kv.Key).ToList();
        foreach (var key in toRemove)
            Messages.TryRemove(key, out _);
        return Task.FromResult(toRemove.Count);
    }
}

/// <summary>
/// IMessageStore that always throws, for testing store failure scenarios.
/// </summary>
public class FailingMessageStore : IMessageStore
{
    public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task MarkAsProcessingBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task AcknowledgeAsync(Guid messageId, string consumerId, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task AcknowledgeBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task MarkAsFailedAsync(Guid messageId, string consumerId, string errorDetails, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task DeadLetterAsync(Guid messageId, string consumerId, string reason, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task<IReadOnlyList<Message>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");
}
