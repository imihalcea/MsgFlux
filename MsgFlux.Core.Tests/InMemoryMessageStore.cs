using System.Collections.Concurrent;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

/// <summary>
/// In-memory IMessageStore implementation for unit testing.
/// </summary>
public class InMemoryMessageStore : IMessageStore
{
    public ConcurrentDictionary<string, PersistedMessage> Messages { get; } = new();

    public Task<string> PersistAsync(PersistedMessage message, CancellationToken ct = default)
    {
        Messages.TryAdd(message.MessageId, message);
        return Task.FromResult(message.MessageId);
    }

    public Task MarkAsProcessingAsync(string messageId, CancellationToken ct = default)
    {
        if (Messages.TryGetValue(messageId, out var msg))
        {
            Messages[messageId] = msg with { State = MessageState.Processing, ProcessedAt = DateTimeOffset.UtcNow };
        }
        return Task.CompletedTask;
    }

    public Task AcknowledgeAsync(string messageId, CancellationToken ct = default)
    {
        if (Messages.TryGetValue(messageId, out var msg))
        {
            Messages[messageId] = msg with { State = MessageState.Completed, ProcessedAt = DateTimeOffset.UtcNow };
        }
        return Task.CompletedTask;
    }

    public Task MarkAsFailedAsync(string messageId, string errorDetails, CancellationToken ct = default)
    {
        if (Messages.TryGetValue(messageId, out var msg))
        {
            Messages[messageId] = msg with
            {
                State = MessageState.Failed,
                ErrorDetails = errorDetails,
                RetryCount = msg.RetryCount + 1
            };
        }
        return Task.CompletedTask;
    }

    public Task DeadLetterAsync(string messageId, string reason, CancellationToken ct = default)
    {
        if (Messages.TryGetValue(messageId, out var msg))
        {
            Messages[messageId] = msg with { State = MessageState.DeadLettered, ErrorDetails = reason };
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<PersistedMessage>> FetchUnprocessedAsync(
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

        return Task.FromResult<IReadOnlyList<PersistedMessage>>(results);
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
    public Task<string> PersistAsync(PersistedMessage message, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task MarkAsProcessingAsync(string messageId, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task AcknowledgeAsync(string messageId, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task MarkAsFailedAsync(string messageId, string errorDetails, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task DeadLetterAsync(string messageId, string reason, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task<IReadOnlyList<PersistedMessage>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");

    public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
        => throw new InvalidOperationException("Store unavailable");
}
