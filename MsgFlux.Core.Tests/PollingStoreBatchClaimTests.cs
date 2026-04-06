using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class PollingStoreBatchClaimTests
{
    [Test]
    public async Task Should_Batch_MarkAsProcessing_Calls()
    {
        // Arrange — store that counts MarkAsProcessing calls
        var store = new ClaimCountingStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(50))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        // Seed 20 messages
        var messages = Enumerable.Range(0, 20).Select(i => new Message
        {
            MessageId = Guid.NewGuid(),
            ConsumerId = "consumer-1",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }).ToList();
        await store.PersistAsync(messages);

        // Act — consume all messages, calling OnProcessing for each
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var dispatched = 0;
        try
        {
            await foreach (var item in source.StreamAsync(cts.Token))
            {
                await item.OnProcessing!(CancellationToken.None);
                dispatched++;
                if (dispatched == 20) break;
            }
        }
        catch (OperationCanceledException) { }

        Assert.That(dispatched, Is.EqualTo(20));

        // Flush pending claims (normally done at next poll cycle)
        source.Complete();

        // Assert — individual MarkAsProcessingAsync should not be called.
        // Claims are flushed via MarkAsProcessingBatchAsync.
        Assert.That(store.MarkAsProcessingCount, Is.EqualTo(0),
            "Expected no individual claim calls");
        Assert.That(store.MarkAsProcessingBatchCount, Is.GreaterThan(0),
            "Expected at least one batch claim call");
    }

    private class ClaimCountingStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();
        public int MarkAsProcessingCount;
        public int MarkAsProcessingBatchCount;

        public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
            => _inner.PersistAsync(messages, ct);
        public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
        {
            Interlocked.Increment(ref MarkAsProcessingCount);
            return _inner.MarkAsProcessingAsync(messageId, consumerId, ct);
        }
        public Task MarkAsProcessingBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
        {
            Interlocked.Increment(ref MarkAsProcessingBatchCount);
            return _inner.MarkAsProcessingBatchAsync(items, ct);
        }
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
}
