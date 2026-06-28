using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

public class SchedulerTests
{
    private static Scheduler CreateScheduler(Registry registry, FakeScheduleStore store) =>
        new(registry, new JsonSerializer(), new MsgFluxOptions(), store);

    [Test]
    public async Task ScheduleAsync_Persists_Single_Row_And_Returns_Id()
    {
        var registry = new Registry();
        registry.Register<SchedTestMessage, SchedConsumer>(Semantics.AtLeastOnce);
        var store = new FakeScheduleStore();
        var scheduler = CreateScheduler(registry, store);

        var deliverAt = DateTimeOffset.UtcNow.AddHours(1);
        var id = await scheduler.ScheduleAsync(new SchedTestMessage { Content = "hi" }, deliverAt);

        Assert.That(store.Items, Has.Count.EqualTo(1));
        var scheduled = store.Items[id];
        Assert.That(scheduled.Id, Is.EqualTo(id));
        Assert.That(scheduled.Type, Is.EqualTo(typeof(SchedTestMessage).FullName));
        Assert.That(scheduled.Msg, Is.Not.Empty);
        Assert.That(scheduled.DeliverAt, Is.EqualTo(deliverAt.ToUniversalTime()));
        Assert.That(scheduled.Status, Is.EqualTo(ScheduledState.Scheduled));
    }

    [Test]
    public void ScheduleAsync_Throws_When_No_AtLeastOnce_Consumer()
    {
        var registry = new Registry();
        registry.Register<SchedTestMessage, SchedConsumer>(Semantics.AtMostOnce); // not durable
        var scheduler = CreateScheduler(registry, new FakeScheduleStore());

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => scheduler.ScheduleAsync(new SchedTestMessage(), DateTimeOffset.UtcNow.AddMinutes(5)));
        Assert.That(ex!.Message, Does.Contain("requires durability"));
    }

    [Test]
    public void ScheduleAsync_Throws_When_No_Consumer_Registered()
    {
        var scheduler = CreateScheduler(new Registry(), new FakeScheduleStore());

        Assert.ThrowsAsync<InvalidOperationException>(
            () => scheduler.ScheduleAsync(new SchedTestMessage(), DateTimeOffset.UtcNow.AddMinutes(5)));
    }

    [Test]
    public async Task CancelScheduledAsync_Cancels_A_Scheduled_Message()
    {
        var registry = new Registry();
        registry.Register<SchedTestMessage, SchedConsumer>(Semantics.AtLeastOnce);
        var store = new FakeScheduleStore();
        var scheduler = CreateScheduler(registry, store);

        var id = await scheduler.ScheduleAsync(new SchedTestMessage(), DateTimeOffset.UtcNow.AddHours(1));
        var cancelled = await scheduler.CancelScheduledAsync(id);

        Assert.That(cancelled, Is.True);
        Assert.That(store.Items[id].Status, Is.EqualTo(ScheduledState.Cancelled));
    }

    public class SchedTestMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    public class SchedConsumer : IConsume<SchedTestMessage>
    {
        public Task HandleAsync(SchedTestMessage message, CancellationToken ct) => Task.CompletedTask;
    }
}
