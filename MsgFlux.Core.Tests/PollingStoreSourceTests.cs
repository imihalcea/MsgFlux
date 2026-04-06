using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class PollingStoreSourceTests
{
    [Test]
    public async Task Should_Batch_MarkAsProcessing_Via_OnProcessing_Callback()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(50))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([new Message
        {
            MessageId = Guid.Empty,
            ConsumerId = "consumer-1",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }]);

        // Act — get the first yielded item and call OnProcessing
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        DispatchItem? item = null;
        await foreach (var d in source.StreamAsync(cts.Token))
        {
            item = d;
            break;
        }

        Assert.That(item, Is.Not.Null);
        Assert.That(item!.OnProcessing, Is.Not.Null);

        // OnProcessing enqueues (does not write to store immediately)
        await item.OnProcessing!(CancellationToken.None);
        var msg = store.Messages[(Guid.Empty, "consumer-1")];
        Assert.That(msg.State, Is.EqualTo(MessageState.Pending), "Claim should be deferred, not immediate");

        // Flush via Complete — claim is now persisted
        source.Complete();
        msg = store.Messages[(Guid.Empty, "consumer-1")];
        Assert.That(msg.State, Is.EqualTo(MessageState.Processing), "Claim should be flushed on Complete");
    }

    [Test]
    public async Task Should_Not_Reyield_InFlight_Messages_On_Rapid_Polls()
    {
        // Arrange — a message that stays Pending (no OnAck) across rapid poll cycles
        var store = new FetchCountingStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(50))
            .WithMaxDeadLetterRetries(100);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([new Message
        {
            MessageId = Guid.Empty,
            ConsumerId = "consumer-1",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }]);

        // Act — consume for 300ms with rapid polling (50ms). Multiple polls will
        // fetch the same message, but in-flight dedup should yield it only once.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var yieldCount = 0;
        try
        {
            await foreach (var _ in source.StreamAsync(cts.Token))
                yieldCount++;
        }
        catch (OperationCanceledException) { }

        // Assert — message yielded only once despite multiple poll cycles
        Assert.That(yieldCount, Is.EqualTo(1));
        Assert.That(store.FetchCount, Is.GreaterThan(1), "Store should have been polled multiple times");
    }

    /// <summary>
    /// Wraps InMemoryMessageStore but throws on MarkAsProcessingAsync to simulate a DB failure at claim time.
    /// </summary>
    private class FailMarkAsProcessingStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();

        public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
            => _inner.PersistAsync(messages, ct);

        public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
            => throw new InvalidOperationException("DB connection lost");

        public Task MarkAsProcessingBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
            => throw new InvalidOperationException("DB connection lost");

        public Task AcknowledgeAsync(Guid messageId, string consumerId, CancellationToken ct = default)
            => _inner.AcknowledgeAsync(messageId, consumerId, ct);

        public Task AcknowledgeBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
            => _inner.AcknowledgeBatchAsync(items, ct);

        public Task MarkAsFailedAsync(Guid messageId, string consumerId, string errorDetails, CancellationToken ct = default)
            => _inner.MarkAsFailedAsync(messageId, consumerId, errorDetails, ct);

        public Task DeadLetterAsync(Guid messageId, string consumerId, string reason, CancellationToken ct = default)
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
        public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
            => _inner.MarkAsProcessingAsync(messageId, consumerId, ct);
        public Task MarkAsProcessingBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
            => _inner.MarkAsProcessingBatchAsync(items, ct);
        public Task AcknowledgeAsync(Guid messageId, string consumerId, CancellationToken ct = default)
            => _inner.AcknowledgeAsync(messageId, consumerId, ct);
        public Task AcknowledgeBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
            => _inner.AcknowledgeBatchAsync(items, ct);
        public Task MarkAsFailedAsync(Guid messageId, string consumerId, string errorDetails, CancellationToken ct = default)
            => _inner.MarkAsFailedAsync(messageId, consumerId, errorDetails, ct);
        public Task DeadLetterAsync(Guid messageId, string consumerId, string reason, CancellationToken ct = default)
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
