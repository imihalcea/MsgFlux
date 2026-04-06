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
}
