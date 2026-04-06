using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class PollingStoreDeduplicationTests
{
    [Test]
    public async Task Should_Not_Yield_Same_Message_Twice_While_InFlight()
    {
        // Arrange — a message that stays Pending in the store (no MarkAsProcessing before yield).
        // Without deduplication, each poll cycle re-fetches and re-yields it.
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(30))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([SeedMessage()]);

        // Act — consume for 200ms (multiple poll cycles at 30ms interval)
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var yielded = new List<DispatchItem>();
        try
        {
            await foreach (var item in source.StreamAsync(cts.Token))
            {
                yielded.Add(item);
                // Don't call OnAck/OnFail — message stays in-flight
            }
        }
        catch (OperationCanceledException) { }

        // Assert — without deduplication this would be 3+ (one per poll cycle).
        // With deduplication, the same message is yielded only once.
        Assert.That(yielded, Has.Count.EqualTo(1),
            $"Message was yielded {yielded.Count} times — duplicate dispatch detected");
    }

    [Test]
    public async Task Should_Allow_Reyield_After_OnFail_Clears_InFlight()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(30))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([SeedMessage()]);

        // Act — consume, fail the first item, then let it be re-yielded
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var yieldCount = 0;
        try
        {
            await foreach (var item in source.StreamAsync(cts.Token))
            {
                yieldCount++;
                if (yieldCount == 1)
                {
                    // Fail the message — clears in-flight, allows re-fetch
                    await item.OnFail!("simulated failure", CancellationToken.None);
                }
                else
                {
                    break; // Got the re-yield, done
                }
            }
        }
        catch (OperationCanceledException) { }

        // Assert — message was yielded at least twice: once initially, once after OnFail
        Assert.That(yieldCount, Is.GreaterThanOrEqualTo(2),
            "Message was not re-yielded after OnFail cleared in-flight tracking");
    }

    [Test]
    public async Task Should_Clear_InFlight_After_OnAck()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(30))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([SeedMessage()]);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var yieldCount = 0;
        try
        {
            await foreach (var item in source.StreamAsync(cts.Token))
            {
                yieldCount++;
                if (yieldCount == 1)
                {
                    await item.OnAck!(CancellationToken.None);
                    // Seed a new message — the original is acked (batch pending),
                    // this verifies that _inFlight tracking doesn't block new messages.
                    await store.PersistAsync([SeedMessage(Guid.NewGuid())]);
                }
                else
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }

        Assert.That(yieldCount, Is.GreaterThanOrEqualTo(2),
            "Message was not re-yielded after OnAck cleared in-flight tracking");
    }

    [Test]
    public async Task Should_Clear_InFlight_After_OnDeadLetter()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(30))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([SeedMessage()]);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var yieldCount = 0;
        try
        {
            await foreach (var item in source.StreamAsync(cts.Token))
            {
                yieldCount++;
                if (yieldCount == 1)
                {
                    await item.OnDeadLetter!("dead", CancellationToken.None);
                    // Re-seed with same key — force back to Pending
                    store.Messages[(Guid.Empty, "consumer-1")] = SeedMessage();
                }
                else
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }

        Assert.That(yieldCount, Is.GreaterThanOrEqualTo(2),
            "Message was not re-yielded after OnDeadLetter cleared in-flight tracking");
    }

    [Test]
    public async Task Should_Clear_InFlight_When_OnProcessing_Fails()
    {
        // Arrange — store that fails on MarkAsProcessingAsync
        var store = new FailMarkAsProcessingStore();
        var options = new MsgFluxOptions()
            .WithReplayInterval(TimeSpan.FromMilliseconds(30))
            .WithMaxDeadLetterRetries(10);

        var source = new PollingStoreSource(store, options, NullLogger<PollingStoreSource>.Instance);

        await store.PersistAsync([SeedMessage()]);

        // Act — consume for 200ms. OnProcessing will fail on every dispatch.
        // Without the fix, the message is stuck in _inFlight after the first yield
        // and is never re-yielded (blocked forever).
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var yieldCount = 0;
        try
        {
            await foreach (var item in source.StreamAsync(cts.Token))
            {
                yieldCount++;
                // Simulate the Engine calling OnProcessing — it fails
                try { await item.OnProcessing!(CancellationToken.None); }
                catch { /* Engine would skip dispatch */ }
            }
        }
        catch (OperationCanceledException) { }

        // Assert — message should be re-yielded on subsequent polls since
        // OnProcessing failure clears the in-flight tracking.
        Assert.That(yieldCount, Is.GreaterThanOrEqualTo(2),
            $"Message was yielded only {yieldCount} time(s) — stuck in in-flight set after OnProcessing failure");
    }

    private class FailMarkAsProcessingStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new();
        public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
            => _inner.PersistAsync(messages, ct);
        public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
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

    private static Message SeedMessage(Guid? id = null) => new()
    {
        MessageId = id ?? Guid.Empty,
        ConsumerId = "consumer-1",
        Payload = [0x01],
        Headers = new Dictionary<string, string>(),
        MessageType = "Test",
        State = MessageState.Pending,
        CreatedAt = DateTimeOffset.UtcNow
    };
}
