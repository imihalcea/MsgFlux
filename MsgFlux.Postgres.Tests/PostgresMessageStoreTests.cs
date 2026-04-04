using MsgFlux.Abstractions;

namespace MsgFlux.Postgres.Tests;

public class PostgresMessageStoreTests
{
    private PostgresMessageStore _store = null!;
    private FakeClock _clock = null!;

    [SetUp]
    public async Task SetUp()
    {
        _clock = new FakeClock();
        _store = new PostgresMessageStore(PostgresFixture.DataSource, _clock);

        await using var cmd = PostgresFixture.DataSource.CreateCommand("DELETE FROM msgflux_messages");
        await cmd.ExecuteNonQueryAsync();
    }

    private PersistedMessage CreateMessage(
        string? id = null,
        string messageType = "TestMessage",
        MessageState state = MessageState.Pending,
        DateTimeOffset? createdAt = null) => new()
    {
        MessageId = id ?? Guid.NewGuid().ToString(),
        Payload = [0x01, 0x02, 0x03],
        Headers = new Dictionary<string, string> { ["key"] = "value" },
        MessageType = messageType,
        State = state,
        CreatedAt = createdAt ?? _clock.UtcNow
    };

    // --- PersistAsync ---

    [Test]
    public async Task PersistAsync_Should_Insert_Message_And_Return_Id()
    {
        var msg = CreateMessage();

        var returnedId = await _store.PersistAsync(msg);

        Assert.That(returnedId, Is.EqualTo(msg.MessageId));

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(1));
        Assert.That(fetched[0].MessageId, Is.EqualTo(msg.MessageId));
        Assert.That(fetched[0].MessageType, Is.EqualTo("TestMessage"));
        Assert.That(fetched[0].Headers["key"], Is.EqualTo("value"));
        Assert.That(fetched[0].Payload, Is.EqualTo(new byte[] { 0x01, 0x02, 0x03 }));
    }

    [Test]
    public async Task PersistAsync_Should_Be_Idempotent_On_Conflict()
    {
        var msg = CreateMessage();

        await _store.PersistAsync(msg);
        await _store.PersistAsync(msg);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(1));
    }

    // --- MarkAsProcessingAsync ---

    [Test]
    public async Task MarkAsProcessingAsync_Should_Update_State()
    {
        var msg = CreateMessage();
        await _store.PersistAsync(msg);

        await _store.MarkAsProcessingAsync(msg.MessageId);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(0));
    }

    // --- AcknowledgeAsync ---

    [Test]
    public async Task AcknowledgeAsync_Should_Mark_As_Completed()
    {
        var msg = CreateMessage();
        await _store.PersistAsync(msg);

        await _store.AcknowledgeAsync(msg.MessageId);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(0));
    }

    // --- MarkAsFailedAsync ---

    [Test]
    public async Task MarkAsFailedAsync_Should_Update_State_And_Increment_RetryCount()
    {
        var msg = CreateMessage();
        await _store.PersistAsync(msg);

        await _store.MarkAsFailedAsync(msg.MessageId, "boom");
        await _store.MarkAsFailedAsync(msg.MessageId, "boom again");

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(1));
        Assert.That(fetched[0].State, Is.EqualTo(MessageState.Failed));
        Assert.That(fetched[0].RetryCount, Is.EqualTo(2));
        Assert.That(fetched[0].ErrorDetails, Is.EqualTo("boom again"));
    }

    // --- DeadLetterAsync ---

    [Test]
    public async Task DeadLetterAsync_Should_Update_State()
    {
        var msg = CreateMessage();
        await _store.PersistAsync(msg);

        await _store.DeadLetterAsync(msg.MessageId, "max retries exceeded");

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(0));
    }

    // --- FetchUnprocessedAsync ---

    [Test]
    public async Task FetchUnprocessedAsync_Should_Return_Pending_And_Failed()
    {
        var pending = CreateMessage(messageType: "A");
        var failed = CreateMessage(messageType: "B");

        await _store.PersistAsync(pending);
        await _store.PersistAsync(failed);
        await _store.MarkAsFailedAsync(failed.MessageId, "err");

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(2));

        var states = fetched.Select(m => m.State).ToHashSet();
        Assert.That(states, Does.Contain(MessageState.Pending));
        Assert.That(states, Does.Contain(MessageState.Failed));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Filter_By_MessageType()
    {
        await _store.PersistAsync(CreateMessage(messageType: "OrderCreated"));
        await _store.PersistAsync(CreateMessage(messageType: "UserCreated"));

        var fetched = await _store.FetchUnprocessedAsync(messageType: "OrderCreated", maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(1));
        Assert.That(fetched[0].MessageType, Is.EqualTo("OrderCreated"));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Respect_MaxCount()
    {
        for (var i = 0; i < 5; i++)
            await _store.PersistAsync(CreateMessage());

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 3);
        Assert.That(fetched, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Return_Stale_Processing_Messages()
    {
        var msg = CreateMessage();
        await _store.PersistAsync(msg);

        // MarkAsProcessing stamps processed_at = clock.UtcNow (T0)
        await _store.MarkAsProcessingAsync(msg.MessageId);

        // Advance clock by 10 minutes — message is now stale (threshold = 5 min)
        _clock.Advance(TimeSpan.FromMinutes(10));

        var fetched = await _store.FetchUnprocessedAsync(
            maxCount: 10,
            staleProcessingTimeout: TimeSpan.FromMinutes(5));

        Assert.That(fetched, Has.Count.EqualTo(1));
        Assert.That(fetched[0].State, Is.EqualTo(MessageState.Processing));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Not_Return_Recent_Processing_Messages()
    {
        var msg = CreateMessage();
        await _store.PersistAsync(msg);
        await _store.MarkAsProcessingAsync(msg.MessageId);

        // Clock not advanced — processed_at is still recent
        var fetched = await _store.FetchUnprocessedAsync(
            maxCount: 10,
            staleProcessingTimeout: TimeSpan.FromMinutes(5));

        Assert.That(fetched, Has.Count.EqualTo(0));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Order_By_CreatedAt_Asc()
    {
        var first = CreateMessage(id: "msg-1", createdAt: _clock.UtcNow);
        _clock.Advance(TimeSpan.FromSeconds(1));
        var second = CreateMessage(id: "msg-2", createdAt: _clock.UtcNow);

        await _store.PersistAsync(first);
        await _store.PersistAsync(second);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched[0].MessageId, Is.EqualTo("msg-1"));
        Assert.That(fetched[1].MessageId, Is.EqualTo("msg-2"));
    }

    // --- PurgeCompletedAsync ---

    [Test]
    public async Task PurgeCompletedAsync_Should_Delete_Old_Completed_Messages()
    {
        var msg = CreateMessage(createdAt: _clock.UtcNow);
        await _store.PersistAsync(msg);
        await _store.AcknowledgeAsync(msg.MessageId);

        // Advance clock by 2 hours — message is now old (threshold = 1 hour)
        _clock.Advance(TimeSpan.FromHours(2));

        var purged = await _store.PurgeCompletedAsync(TimeSpan.FromHours(1));
        Assert.That(purged, Is.EqualTo(1));
    }

    [Test]
    public async Task PurgeCompletedAsync_Should_Not_Delete_Recent_Completed_Messages()
    {
        var msg = CreateMessage();
        await _store.PersistAsync(msg);
        await _store.AcknowledgeAsync(msg.MessageId);

        var purged = await _store.PurgeCompletedAsync(TimeSpan.FromHours(1));
        Assert.That(purged, Is.EqualTo(0));
    }

    [Test]
    public async Task PurgeCompletedAsync_Should_Not_Delete_Non_Completed_Messages()
    {
        var msg = CreateMessage(createdAt: _clock.UtcNow);
        await _store.PersistAsync(msg);

        _clock.Advance(TimeSpan.FromHours(2));

        var purged = await _store.PurgeCompletedAsync(TimeSpan.FromHours(1));
        Assert.That(purged, Is.EqualTo(0));
    }
}
