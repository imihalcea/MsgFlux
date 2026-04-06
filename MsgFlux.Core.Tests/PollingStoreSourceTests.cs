using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class PollingStoreSourceTests
{
    [Test]
    public async Task Should_Not_Dispatch_When_MarkAsProcessing_Fails()
    {
        // Arrange — store that fails on MarkAsProcessingAsync
        var store = new FailMarkAsProcessingStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(50))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        // Seed a pending message
        await store.PersistAsync([new Message
        {
            MessageId = "msg-1",
            ConsumerId = "consumer-1",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }]);

        // Act — consume from the source for a short window
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var dispatched = new List<DispatchItem>();
        try
        {
            await foreach (var item in source.StreamAsync(cts.Token))
            {
                dispatched.Add(item);
            }
        }
        catch (OperationCanceledException) { }

        // Assert — message should NOT have been dispatched since claim failed
        Assert.That(dispatched, Has.Count.EqualTo(0));
    }

    [Test]
    public async Task Should_Backoff_Between_NonEmpty_Batches()
    {
        // Arrange — store with a message that will be fetched repeatedly
        var store = new FetchCountingStore();
        var pollInterval = TimeSpan.FromMilliseconds(200);
        var options = new MsgFluxOptions()
            .WithReplayInterval(pollInterval)
            .WithMaxDeadLetterRetries(100);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([new Message
        {
            MessageId = "msg-1",
            ConsumerId = "consumer-1",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }]);

        // Act — consume for 500ms (with 200ms interval, expect ~2-3 fetches, not dozens)
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        try
        {
            await foreach (var _ in source.StreamAsync(cts.Token)) { }
        }
        catch (OperationCanceledException) { }

        // Assert — without backoff this would be 50+ fetches; with backoff expect ≤ 5
        Assert.That(store.FetchCount, Is.LessThanOrEqualTo(5));
    }

    /// <summary>
    /// Wraps InMemoryMessageStore but throws on MarkAsProcessingAsync to simulate a DB failure at claim time.
    /// </summary>
    private class FailMarkAsProcessingStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();

        public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
            => _inner.PersistAsync(messages, ct);

        public Task MarkAsProcessingAsync(string messageId, string consumerId, CancellationToken ct = default)
            => throw new InvalidOperationException("DB connection lost");

        public Task AcknowledgeAsync(string messageId, string consumerId, CancellationToken ct = default)
            => _inner.AcknowledgeAsync(messageId, consumerId, ct);

        public Task MarkAsFailedAsync(string messageId, string consumerId, string errorDetails, CancellationToken ct = default)
            => _inner.MarkAsFailedAsync(messageId, consumerId, errorDetails, ct);

        public Task DeadLetterAsync(string messageId, string consumerId, string reason, CancellationToken ct = default)
            => _inner.DeadLetterAsync(messageId, consumerId, reason, ct);

        public Task<IReadOnlyList<Message>> FetchUnprocessedAsync(string? messageType = null, int maxCount = 100,
            TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
            => _inner.FetchUnprocessedAsync(messageType, maxCount, staleProcessingTimeout, ct);

        public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
            => _inner.PurgeCompletedAsync(olderThan, ct);
    }

    private class FetchCountingStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();
        public int FetchCount;

        public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
            => _inner.PersistAsync(messages, ct);
        public Task MarkAsProcessingAsync(string messageId, string consumerId, CancellationToken ct = default)
            => _inner.MarkAsProcessingAsync(messageId, consumerId, ct);
        public Task AcknowledgeAsync(string messageId, string consumerId, CancellationToken ct = default)
            => _inner.AcknowledgeAsync(messageId, consumerId, ct);
        public Task MarkAsFailedAsync(string messageId, string consumerId, string errorDetails, CancellationToken ct = default)
            => _inner.MarkAsFailedAsync(messageId, consumerId, errorDetails, ct);
        public Task DeadLetterAsync(string messageId, string consumerId, string reason, CancellationToken ct = default)
            => _inner.DeadLetterAsync(messageId, consumerId, reason, ct);
        public Task<IReadOnlyList<Message>> FetchUnprocessedAsync(string? messageType = null, int maxCount = 100,
            TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
        {
            Interlocked.Increment(ref FetchCount);
            return _inner.FetchUnprocessedAsync(messageType, maxCount, staleProcessingTimeout, ct);
        }
        public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
            => _inner.PurgeCompletedAsync(olderThan, ct);
    }
}
