using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class PollingStoreSourceTests
{
    [Test]
    public async Task Fetched_Message_Is_Already_Claimed_By_The_Store()
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

        // Act — get the first yielded item
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        DispatchItem? item = null;
        await foreach (var d in source.StreamAsync(cts.Token))
        {
            item = d;
            break;
        }

        Assert.That(item, Is.Not.Null);
        // Claim is now done atomically inside FetchUnprocessedAsync — no deferred OnProcessing hook.
        Assert.That(item!.OnProcessing, Is.Null);
        var msg = store.Messages[(Guid.Empty, "consumer-1")];
        Assert.That(msg.State, Is.EqualTo(MessageState.Processing), "Row should be claimed on fetch");
    }

    [Test]
    public async Task Should_Not_Reyield_InFlight_Messages_On_Rapid_Polls()
    {
        // Arrange — a message that stays Pending (no OnAck) across rapid poll cycles
        var store = new FetchCountingStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(50))
            // Pin MaxDOP > 1 so the single in-flight message never exhausts the fetch capacity
            // (capacity = MaxDOP - in-flight) regardless of the host core count; this test asserts
            // the store is polled repeatedly while one message stays in-flight.
            .WithMaxDegreeOfParallelism(4)
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

    [Test]
    public async Task SingleSlot_Claims_Only_One_Row_While_It_Is_InFlight()
    {
        // Capacity-gating proof on a single slot (MaxDOP = 1, independent of host core count):
        // FetchUnprocessedAsync claims rows on fetch, so without bounding the claim to free capacity
        // the source would flip ALL pending rows to Processing on the first poll, and the queued ones
        // would age toward StaleProcessingTimeout before ever being dispatched. With capacity-gating,
        // while the single slot is occupied (one message in-flight, never acked) NO further row is
        // claimed — the rest stay Pending until the slot frees.
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(30))
            .WithMaxDegreeOfParallelism(1)
            .WithMaxDeadLetterRetries(100);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync(Enumerable.Range(0, 3).Select(_ => new Message
        {
            MessageId = Guid.NewGuid(),
            ConsumerId = "consumer-1",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }).ToList());

        // Consume across several poll cycles, holding the first item in-flight (never ack/fail).
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var yieldCount = 0;
        try
        {
            await foreach (var _ in source.StreamAsync(cts.Token))
                yieldCount++;
        }
        catch (OperationCanceledException) { }

        var processing = store.Messages.Values.Count(m => m.State == MessageState.Processing);
        var pending = store.Messages.Values.Count(m => m.State == MessageState.Pending);

        Assert.That(yieldCount, Is.EqualTo(1), "only one row should be dispatched while the single slot is busy");
        Assert.That(processing, Is.EqualTo(1), "only the in-flight row should be claimed");
        Assert.That(pending, Is.EqualTo(2), "remaining rows must NOT be claimed beyond available capacity");
    }

    private class FetchCountingStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();
        public int FetchCount;

        public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
            => _inner.PersistAsync(messages, ct);
        public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
            => _inner.MarkAsProcessingAsync(messageId, consumerId, ct);
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
