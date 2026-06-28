using MsgFlux.Abstractions;

namespace MsgFlux.Postgres.Tests;

/// <summary>
/// Proves the competing-consumers contract for horizontal scaling: when N instances of the
/// same consumer poll a shared store concurrently, a single Pending message must be handed
/// out to exactly ONE of them. Today <see cref="PostgresMessageStore.FetchUnprocessedAsync"/>
/// reads rows under <c>FOR UPDATE SKIP LOCKED</c> but does NOT claim them in the same
/// transaction (the lock is released as soon as the SELECT commits, and the claim is a
/// separate, deferred, unconditional UPDATE). As a result every concurrent poller sees the
/// same Pending row and the message is delivered N times instead of once.
/// </summary>
public class ConcurrentDeliveryTests
{
    private PostgresMessageStore _store = null!;
    private FakeClock _clock = null!;

    private const string ConsumerId = "consumer-a";
    private const int InstanceCount = 8;

    [SetUp]
    public async Task SetUp()
    {
        _clock = new FakeClock();
        _store = new PostgresMessageStore(PostgresFixture.DataSource, _clock, new PostgresOptions());

        await using var cmd = PostgresFixture.DataSource.CreateCommand("DELETE FROM msgflux.messages");
        await cmd.ExecuteNonQueryAsync();
    }

    private Message CreateMessage() => new()
    {
        MessageId = Guid.NewGuid(),
        ConsumerId = ConsumerId,
        Payload = [0x01, 0x02, 0x03],
        Headers = new Dictionary<string, string>(),
        MessageType = "TestMessage",
        State = MessageState.Pending,
        CreatedAt = _clock.UtcNow
    };

    [Test]
    public async Task Concurrent_Pollers_Should_Each_Claim_A_Single_Pending_Message_Once()
    {
        // One Pending message, shared by N concurrent instances of the same consumer.
        var msg = CreateMessage();
        await _store.PersistAsync(new[] { msg });

        // Start gate: line all instances up so their fetches genuinely overlap,
        // mirroring N independent pollers hitting the store at the same time.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var fetches = Enumerable.Range(0, InstanceCount).Select(async _ =>
        {
            await gate.Task;
            var batch = await _store.FetchUnprocessedAsync(maxCount: 10);
            return batch.Any(m => m.MessageId == msg.MessageId);
        }).ToArray();

        gate.SetResult();
        var results = await Task.WhenAll(fetches);

        var instancesThatGotTheMessage = results.Count(gotIt => gotIt);

        // Competing-consumers invariant: exactly one instance may take the message.
        // Today this is InstanceCount (every poller gets it) -> duplicate delivery.
        Assert.That(instancesThatGotTheMessage, Is.EqualTo(1),
            $"{instancesThatGotTheMessage}/{InstanceCount} instances received the same Pending message: " +
            "fetch is not atomic with the claim, so the message would be processed multiple times " +
            "under horizontal scaling.");
    }
}
