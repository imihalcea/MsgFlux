using MsgFlux.Abstractions;

namespace MsgFlux.Postgres.Tests;

public class PostgresMessageStoreTests
{
    private PostgresMessageStore _store = null!;
    private FakeClock _clock = null!;

    private const string DefaultConsumerId = "consumer-a";

    [SetUp]
    public async Task SetUp()
    {
        _clock = new FakeClock();
        _store = new PostgresMessageStore(PostgresFixture.DataSource, _clock);

        await using var cmd = PostgresFixture.DataSource.CreateCommand("DELETE FROM msgflux_messages");
        await cmd.ExecuteNonQueryAsync();
    }

    private Message CreateMessage(
        string? id = null,
        string consumerId = DefaultConsumerId,
        string messageType = "TestMessage",
        MessageState state = MessageState.Pending,
        DateTimeOffset? createdAt = null) => new()
    {
        MessageId = id ?? Guid.NewGuid().ToString(),
        ConsumerId = consumerId,
        Payload = [0x01, 0x02, 0x03],
        Headers = new Dictionary<string, string> { ["key"] = "value" },
        MessageType = messageType,
        State = state,
        CreatedAt = createdAt ?? _clock.UtcNow
    };

    private Task PersistOne(Message msg) => _store.PersistAsync(new[] { msg });

    // --- PersistAsync ---

    [Test]
    public async Task PersistAsync_Should_Insert_Message()
    {
        var msg = CreateMessage();

        await PersistOne(msg);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(1));
        Assert.That(fetched[0].MessageId, Is.EqualTo(msg.MessageId));
        Assert.That(fetched[0].ConsumerId, Is.EqualTo(DefaultConsumerId));
        Assert.That(fetched[0].MessageType, Is.EqualTo("TestMessage"));
        Assert.That(fetched[0].Headers["key"], Is.EqualTo("value"));
        Assert.That(fetched[0].Payload, Is.EqualTo(new byte[] { 0x01, 0x02, 0x03 }));
    }

    [Test]
    public async Task PersistAsync_Should_Be_Idempotent_On_Conflict()
    {
        var msg = CreateMessage();

        await PersistOne(msg);
        await PersistOne(msg);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task PersistAsync_Should_Allow_Same_MessageId_For_Different_Consumers()
    {
        var messageId = Guid.NewGuid().ToString();
        await PersistOne(CreateMessage(id: messageId, consumerId: "consumer-a"));
        await PersistOne(CreateMessage(id: messageId, consumerId: "consumer-b"));

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task PersistAsync_Bulk_Should_Insert_All_Rows()
    {
        var rows = Enumerable.Range(0, 5).Select(_ => CreateMessage()).ToList();
        await _store.PersistAsync(rows);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(5));
    }

    [Test]
    public async Task PersistAsync_Large_Bulk_Should_Insert_500_Rows()
    {
        var rows = Enumerable.Range(0, 500).Select(_ => CreateMessage()).ToList();
        await _store.PersistAsync(rows);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 600);
        Assert.That(fetched, Has.Count.EqualTo(500));
    }

    [Test]
    public async Task PersistAsync_Concurrent_Should_Not_Interfere()
    {
        // Two concurrent PersistAsync calls use separate connections with separate temp tables.
        var batch1 = Enumerable.Range(0, 50)
            .Select(i => CreateMessage(id: $"batch1-{i}")).ToList();
        var batch2 = Enumerable.Range(0, 50)
            .Select(i => CreateMessage(id: $"batch2-{i}")).ToList();

        await Task.WhenAll(
            _store.PersistAsync(batch1),
            _store.PersistAsync(batch2));

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 200);
        Assert.That(fetched, Has.Count.EqualTo(100));
    }

    [Test]
    public async Task PersistAsync_Sequential_On_Pooled_Connection_Should_Not_Leak()
    {
        // Two sequential PersistAsync calls may reuse the same pooled connection.
        // The temp table TRUNCATE must prevent stale rows from the first call.
        var batch1 = Enumerable.Range(0, 10)
            .Select(i => CreateMessage(id: $"seq1-{i}")).ToList();
        var batch2 = Enumerable.Range(0, 10)
            .Select(i => CreateMessage(id: $"seq2-{i}")).ToList();

        await _store.PersistAsync(batch1);
        await _store.PersistAsync(batch2);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 30);
        Assert.That(fetched, Has.Count.EqualTo(20));
    }

    // --- MarkAsProcessingAsync ---

    [Test]
    public async Task MarkAsProcessingAsync_Should_Update_State()
    {
        var msg = CreateMessage();
        await PersistOne(msg);

        await _store.MarkAsProcessingAsync(msg.MessageId, msg.ConsumerId);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(0));
    }

    // --- AcknowledgeAsync ---

    [Test]
    public async Task AcknowledgeAsync_Should_Mark_As_Completed()
    {
        var msg = CreateMessage();
        await PersistOne(msg);

        await _store.AcknowledgeAsync(msg.MessageId, msg.ConsumerId);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(0));
    }

    // --- MarkAsFailedAsync ---

    [Test]
    public async Task MarkAsFailedAsync_Should_Update_State_And_Increment_RetryCount()
    {
        var msg = CreateMessage();
        await PersistOne(msg);

        await _store.MarkAsFailedAsync(msg.MessageId, msg.ConsumerId, "boom");
        await _store.MarkAsFailedAsync(msg.MessageId, msg.ConsumerId, "boom again");

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
        await PersistOne(msg);

        await _store.DeadLetterAsync(msg.MessageId, msg.ConsumerId, "max retries exceeded");

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(0));
    }

    // --- FetchUnprocessedAsync ---

    [Test]
    public async Task FetchUnprocessedAsync_Should_Return_Pending_And_Failed()
    {
        var pending = CreateMessage(messageType: "A");
        var failed = CreateMessage(messageType: "B");

        await PersistOne(pending);
        await PersistOne(failed);
        await _store.MarkAsFailedAsync(failed.MessageId, failed.ConsumerId, "err");

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(2));

        var states = fetched.Select(m => m.State).ToHashSet();
        Assert.That(states, Does.Contain(MessageState.Pending));
        Assert.That(states, Does.Contain(MessageState.Failed));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Filter_By_MessageType()
    {
        await PersistOne(CreateMessage(messageType: "OrderCreated"));
        await PersistOne(CreateMessage(messageType: "UserCreated"));

        var fetched = await _store.FetchUnprocessedAsync(messageType: "OrderCreated", maxCount: 10);
        Assert.That(fetched, Has.Count.EqualTo(1));
        Assert.That(fetched[0].MessageType, Is.EqualTo("OrderCreated"));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Respect_MaxCount()
    {
        for (var i = 0; i < 5; i++)
            await PersistOne(CreateMessage());

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 3);
        Assert.That(fetched, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task FetchUnprocessedAsync_Should_Return_Stale_Processing_Messages()
    {
        var msg = CreateMessage();
        await PersistOne(msg);

        await _store.MarkAsProcessingAsync(msg.MessageId, msg.ConsumerId);

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
        await PersistOne(msg);
        await _store.MarkAsProcessingAsync(msg.MessageId, msg.ConsumerId);

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

        await PersistOne(first);
        await PersistOne(second);

        var fetched = await _store.FetchUnprocessedAsync(maxCount: 10);
        Assert.That(fetched[0].MessageId, Is.EqualTo("msg-1"));
        Assert.That(fetched[1].MessageId, Is.EqualTo("msg-2"));
    }

    // --- PurgeCompletedAsync ---

    [Test]
    public async Task PurgeCompletedAsync_Should_Delete_Old_Completed_Messages()
    {
        var msg = CreateMessage(createdAt: _clock.UtcNow);
        await PersistOne(msg);
        await _store.AcknowledgeAsync(msg.MessageId, msg.ConsumerId);

        _clock.Advance(TimeSpan.FromHours(2));

        var purged = await _store.PurgeCompletedAsync(TimeSpan.FromHours(1));
        Assert.That(purged, Is.EqualTo(1));
    }

    [Test]
    public async Task PurgeCompletedAsync_Should_Not_Delete_Recent_Completed_Messages()
    {
        var msg = CreateMessage();
        await PersistOne(msg);
        await _store.AcknowledgeAsync(msg.MessageId, msg.ConsumerId);

        var purged = await _store.PurgeCompletedAsync(TimeSpan.FromHours(1));
        Assert.That(purged, Is.EqualTo(0));
    }

    [Test]
    public async Task PurgeCompletedAsync_Should_Not_Delete_Non_Completed_Messages()
    {
        var msg = CreateMessage(createdAt: _clock.UtcNow);
        await PersistOne(msg);

        _clock.Advance(TimeSpan.FromHours(2));

        var purged = await _store.PurgeCompletedAsync(TimeSpan.FromHours(1));
        Assert.That(purged, Is.EqualTo(0));
    }
}
