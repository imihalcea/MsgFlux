using System.Collections.Concurrent;
using System.Diagnostics;
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

        var add = buffer.AddAsync(MakeMessages(2));

        // Below threshold: nothing flushed synchronously.
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        // The timer flush completes the publish.
        await add;
        Assert.That(store.Messages, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task AddAsync_Completes_Only_After_Persist()
    {
        // Group-commit: the publisher is not acknowledged until the write reaches the store.
        var store = new GatedPersistStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 1);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        var add = buffer.AddAsync(MakeMessages(1));
        await WaitUntil(() => Volatile.Read(ref store.PersistCalls) == 1);

        Assert.That(add.IsCompleted, Is.False);          // persist in flight, not yet acked
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        store.Open();
        await add;
        Assert.That(store.Messages, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Concurrent_Adds_During_Flush_Are_Coalesced()
    {
        // While one flush is in flight, concurrent publishes accumulate into a single next batch.
        var store = new GatedPersistStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 1);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        var first = buffer.AddAsync(MakeMessages(1));    // triggers flush #1, blocks in PersistAsync
        await WaitUntil(() => Volatile.Read(ref store.PersistCalls) == 1);

        var rest = Task.WhenAll(
            buffer.AddAsync(MakeMessages(1)),
            buffer.AddAsync(MakeMessages(1)),
            buffer.AddAsync(MakeMessages(1)));           // accumulate into batch #2

        store.Open();
        await first;
        await rest;

        Assert.That(store.Messages, Has.Count.EqualTo(4));
        Assert.That(store.PersistCalls, Is.EqualTo(2));  // 4 messages coalesced into 2 persist calls
    }

    [Test]
    public async Task AddAsync_Applies_Backpressure_When_Capacity_Reached()
    {
        var store = new GatedPersistStore();
        var options = new MsgFluxOptions()
            .WithBufferedPublishing(flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 1)
            .WithMaxBufferedMessages(2);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        var first = buffer.AddAsync(MakeMessages(2));    // fills capacity (2); flush blocks in PersistAsync
        await WaitUntil(() => Volatile.Read(ref store.PersistCalls) == 1);

        var blocked = buffer.AddAsync(MakeMessages(1));  // no free slot → must wait on capacity
        await Task.Delay(50);
        Assert.That(blocked.IsCompleted, Is.False);

        store.Open();                                    // releases first → frees capacity → blocked proceeds
        await first;
        await blocked;
        Assert.That(store.Messages, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Should_Flush_Remaining_On_Dispose()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 100);
        var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        var add = buffer.AddAsync(MakeMessages(5));      // below threshold; not flushed yet
        await Task.Delay(50);
        Assert.That(add.IsCompleted, Is.False);
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        await buffer.DisposeAsync();                     // final flush during shutdown
        await add;
        Assert.That(store.Messages, Has.Count.EqualTo(5));
    }

    [Test]
    public async Task AddAsync_Propagates_Persist_Failure_To_Caller()
    {
        // A1: no silent internal retry — the store error surfaces so the caller can republish.
        var store = new FailingMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 3);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        Assert.ThrowsAsync<InvalidOperationException>(() => buffer.AddAsync(MakeMessages(3)));
    }

    [Test]
    public void AddAsync_Should_Honor_CancellationToken()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 1);
        using var cts = new CancellationTokenSource();

        var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            () => buffer.AddAsync(MakeMessages(1), cts.Token));
    }

    /// <summary>
    /// Store whose first PersistAsync blocks until <see cref="Open"/> is called, counting calls.
    /// Lets a test hold a flush in flight to observe group-commit and backpressure.
    /// </summary>
    private class GatedPersistStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int PersistCalls;

        public ConcurrentDictionary<(Guid, string), Message> Messages => _inner.Messages;
        public void Open() => _gate.TrySetResult();

        public async Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref PersistCalls) == 1)
                await _gate.Task;
            await _inner.PersistAsync(messages, ct);
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

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 2000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs)
                Assert.Fail("Condition was not met within the timeout.");
            await Task.Delay(10);
        }
    }

    private static List<Message> MakeMessages(int count) =>
        Enumerable.Range(0, count).Select(_ => new Message
        {
            MessageId = Guid.NewGuid(),
            ConsumerId = "test-consumer",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            CreatedAt = DateTimeOffset.UtcNow
        }).ToList();
}
