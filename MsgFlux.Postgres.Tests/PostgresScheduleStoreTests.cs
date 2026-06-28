using MsgFlux.Abstractions;

namespace MsgFlux.Postgres.Tests;

public class PostgresScheduleStoreTests
{
    private PostgresScheduleStore _store = null!;
    private FakeClock _clock = null!;

    [SetUp]
    public async Task SetUp()
    {
        _clock = new FakeClock();
        _store = new PostgresScheduleStore(PostgresFixture.DataSource, _clock, new PostgresOptions());

        await using var cmd = PostgresFixture.DataSource.CreateCommand("DELETE FROM msgflux.scheduled_messages");
        await cmd.ExecuteNonQueryAsync();
    }

    private ScheduledMessage CreateScheduled(
        Guid? id = null,
        string type = "TestMessage",
        DateTimeOffset? deliverAt = null,
        ScheduledState status = ScheduledState.Scheduled,
        DateTimeOffset? createdAt = null) => new()
    {
        Id = id ?? Guid.NewGuid(),
        Type = type,
        Msg = [0x01, 0x02, 0x03],
        DeliverAt = deliverAt ?? _clock.UtcNow.AddHours(1),
        Status = status,
        CreatedAt = createdAt ?? _clock.UtcNow
    };

    private Task Schedule(params ScheduledMessage[] msgs) => _store.ScheduleAsync(msgs);

    private async Task<long> CountAll()
    {
        await using var cmd = PostgresFixture.DataSource.CreateCommand("SELECT count(*) FROM msgflux.scheduled_messages");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    // --- FetchDueAsync ---

    [Test]
    public async Task FetchDue_Returns_Only_Due_Messages()
    {
        var dueId = Guid.NewGuid();
        await Schedule(
            CreateScheduled(id: dueId, deliverAt: _clock.UtcNow.AddMinutes(-1)),
            CreateScheduled(deliverAt: _clock.UtcNow.AddHours(1)));

        var due = await _store.FetchDueAsync();

        Assert.That(due, Has.Count.EqualTo(1));
        Assert.That(due[0].Id, Is.EqualTo(dueId));
        Assert.That(due[0].Type, Is.EqualTo("TestMessage"));
        Assert.That(due[0].Msg, Is.EqualTo(new byte[] { 0x01, 0x02, 0x03 }));
        Assert.That(due[0].Status, Is.EqualTo(ScheduledState.Scheduled));
    }

    [Test]
    public async Task FetchDue_Returns_Message_With_Past_DeliverAt()
    {
        await Schedule(CreateScheduled(deliverAt: _clock.UtcNow.AddHours(-2)));

        var due = await _store.FetchDueAsync();

        Assert.That(due, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task FetchDue_Excludes_Promoted_Rows()
    {
        var id = Guid.NewGuid();
        await Schedule(CreateScheduled(id: id, deliverAt: _clock.UtcNow.AddMinutes(-1)));

        await _store.MarkPromotedAsync([id]);

        var due = await _store.FetchDueAsync();
        Assert.That(due, Is.Empty);
    }

    [Test]
    public async Task ScheduleAsync_Is_Idempotent_On_Conflict()
    {
        var msg = CreateScheduled(deliverAt: _clock.UtcNow.AddMinutes(-1));

        await Schedule(msg);
        await Schedule(msg);

        Assert.That(await CountAll(), Is.EqualTo(1));
    }

    [Test]
    public async Task ScheduleAsync_Bulk_Inserts_All_Rows()
    {
        var rows = Enumerable.Range(0, 6)
            .Select(_ => CreateScheduled(deliverAt: _clock.UtcNow.AddMinutes(-1)))
            .ToArray();

        await _store.ScheduleAsync(rows);

        var due = await _store.FetchDueAsync(maxCount: 10);
        Assert.That(due, Has.Count.EqualTo(6));
    }

    // --- CancelScheduledAsync ---

    [Test]
    public async Task Cancel_Cancels_A_Scheduled_Message()
    {
        var id = Guid.NewGuid();
        await Schedule(CreateScheduled(id: id, deliverAt: _clock.UtcNow.AddMinutes(-1)));

        var cancelled = await _store.CancelScheduledAsync(id);

        Assert.That(cancelled, Is.True);
        Assert.That(await _store.FetchDueAsync(), Is.Empty); // no longer due
    }

    [Test]
    public async Task Cancel_Returns_False_For_Already_Promoted_Message()
    {
        var id = Guid.NewGuid();
        await Schedule(CreateScheduled(id: id, deliverAt: _clock.UtcNow.AddMinutes(-1)));
        await _store.MarkPromotedAsync([id]);

        var cancelled = await _store.CancelScheduledAsync(id);

        Assert.That(cancelled, Is.False);
    }

    [Test]
    public async Task Cancel_Returns_False_For_Unknown_Message()
    {
        Assert.That(await _store.CancelScheduledAsync(Guid.NewGuid()), Is.False);
    }

    // --- PurgeAsync ---

    [Test]
    public async Task Purge_Removes_Promoted_And_Cancelled_Older_Than_Cutoff()
    {
        var old = _clock.UtcNow.AddHours(-5);

        await Schedule(
            CreateScheduled(status: ScheduledState.Promoted, createdAt: old, deliverAt: old),
            CreateScheduled(status: ScheduledState.Cancelled, createdAt: old, deliverAt: old),
            CreateScheduled(status: ScheduledState.Scheduled, createdAt: old, deliverAt: _clock.UtcNow.AddHours(1)));

        var purged = await _store.PurgeAsync(TimeSpan.FromHours(4));

        Assert.That(purged, Is.EqualTo(2));        // promoted + cancelled
        Assert.That(await CountAll(), Is.EqualTo(1)); // the scheduled row survives
    }
}
