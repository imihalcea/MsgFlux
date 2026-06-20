using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;

namespace MsgFlux.Core.Tests;

public class DurableBufferTests
{
    [Test]
    public async Task Should_Flush_When_Threshold_Reached()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 3);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        await buffer.AddAsync(MakeMessages(3));

        Assert.That(store.Messages, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Should_Flush_On_Timer_Before_Threshold()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromMilliseconds(50), flushThreshold: 100);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        await buffer.AddAsync(MakeMessages(2));

        // Not yet flushed (threshold=100)
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        // Wait for timer flush
        await Task.Delay(200);

        Assert.That(store.Messages, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Should_Flush_Remaining_On_Dispose()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 100);
        var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        await buffer.AddAsync(MakeMessages(5));
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        await buffer.DisposeAsync();
        Assert.That(store.Messages, Has.Count.EqualTo(5));
    }

    [Test]
    public async Task Should_Restore_Buffer_When_Flush_Fails()
    {
        // Arrange — store that fails on first PersistAsync, then succeeds
        var store = new FailOncePersistStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromMilliseconds(50), flushThreshold: 3);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        // Act — add 3 messages, triggering a threshold flush that will fail
        await buffer.AddAsync(MakeMessages(3));

        // Assert — store has nothing (persist failed), but messages should NOT be lost
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        // Act — wait for the timer flush (store succeeds this time)
        await Task.Delay(200);

        // Assert — all 3 messages recovered and persisted on retry
        Assert.That(store.Messages, Has.Count.EqualTo(3));
    }

    private class FailOncePersistStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();
        private int _callCount;

        public ConcurrentDictionary<(Guid, string), Message> Messages => _inner.Messages;

        public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref _callCount) == 1)
                throw new InvalidOperationException("DB unavailable");
            return _inner.PersistAsync(messages, ct);
        }

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
            => _inner.FetchUnprocessedAsync(messageType, maxCount, staleProcessingTimeout, ct);
        public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
            => _inner.PurgeCompletedAsync(olderThan, ct);
    }

    [Test]
    public void AddAsync_Should_Honor_CancellationToken()
    {
        var store = new SlowPersistStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 1);
        using var cts = new CancellationTokenSource();

        var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        // Cancel immediately
        cts.Cancel();

        // AddAsync should throw OperationCanceledException since the token is already cancelled
        Assert.ThrowsAsync<OperationCanceledException>(
            () => buffer.AddAsync(MakeMessages(1), cts.Token));
    }

    private class SlowPersistStore : IMessageStore
    {
        public async Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
            => await Task.Delay(TimeSpan.FromSeconds(5), ct);
        public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
            => Task.CompletedTask;
        public Task AcknowledgeAsync(Guid messageId, string consumerId, CancellationToken ct = default)
            => Task.CompletedTask;
        public Task AcknowledgeBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
            => Task.CompletedTask;
        public Task MarkAsFailedAsync(Guid messageId, string consumerId, string errorDetails, CancellationToken ct = default)
            => Task.CompletedTask;
        public Task DeadLetterAsync(Guid messageId, string consumerId, string reason, CancellationToken ct = default)
            => Task.CompletedTask;
        public Task<IReadOnlyList<Message>> FetchUnprocessedAsync(string? messageType = null, int maxCount = 100,
            TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<Message>>([]);
        public Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
            => Task.FromResult(0);
    }

    private static List<Message> MakeMessages(int count) =>
        Enumerable.Range(0, count).Select(i => new Message
        {
            MessageId = Guid.NewGuid(),
            ConsumerId = "test-consumer",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            CreatedAt = DateTimeOffset.UtcNow
        }).ToList();
}
