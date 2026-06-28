using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

public class SchedulePromoterTests
{
    private static Registry BuildRegistry()
    {
        var registry = new Registry();
        registry.Register<PromoTestMessage, ConsumerA>(Semantics.AtLeastOnce);
        registry.Register<PromoTestMessage, ConsumerB>(Semantics.AtLeastOnce);
        return registry;
    }

    private static SchedulePromoter CreatePromoter(
        Registry registry, FakeScheduleStore schedStore, InMemoryMessageStore msgStore) =>
        new(schedStore, msgStore, registry, new JsonSerializer(),
            new MsgFluxOptions { PromotionInterval = TimeSpan.FromMilliseconds(20) },
            NullLogger<SchedulePromoter>.Instance);

    [Test]
    public async Task Promotes_Due_Message_With_FanOut_To_All_Durable_Consumers()
    {
        var registry = BuildRegistry();
        var schedStore = new FakeScheduleStore();
        var msgStore = new InMemoryMessageStore();
        var scheduler = new Scheduler(registry, new JsonSerializer(), new MsgFluxOptions(), schedStore);

        // Due in the past so the first promotion cycle picks it up.
        var id = await scheduler.ScheduleAsync(new PromoTestMessage { Content = "x" }, DateTimeOffset.UtcNow.AddSeconds(-1));

        var promoter = CreatePromoter(registry, schedStore, msgStore);
        await promoter.StartAsync(CancellationToken.None);
        await WaitUntil(() => msgStore.Messages.Count >= 2, TimeSpan.FromSeconds(2));
        await promoter.StopAsync(CancellationToken.None);

        Assert.That(msgStore.Messages.Count, Is.EqualTo(2)); // one row per durable consumer
        Assert.That(msgStore.Messages.Values.Select(m => m.MessageId), Is.All.EqualTo(id));
        Assert.That(msgStore.Messages.Values.Select(m => m.MessageType),
            Is.All.EqualTo(typeof(PromoTestMessage).FullName));
        Assert.That(msgStore.Messages.Values.Select(m => m.State), Is.All.EqualTo(MessageState.Pending));
        Assert.That(schedStore.Items[id].Status, Is.EqualTo(ScheduledState.Promoted));
    }

    [Test]
    public async Task Promotion_Is_Idempotent_When_Mark_Fails_Once()
    {
        var registry = BuildRegistry();
        var schedStore = new FakeScheduleStore { MarkPromotedThrowTimes = 1 };
        var msgStore = new InMemoryMessageStore();
        var scheduler = new Scheduler(registry, new JsonSerializer(), new MsgFluxOptions(), schedStore);

        var id = await scheduler.ScheduleAsync(new PromoTestMessage(), DateTimeOffset.UtcNow.AddSeconds(-1));

        var promoter = CreatePromoter(registry, schedStore, msgStore);
        await promoter.StartAsync(CancellationToken.None);
        // First cycle persists the 2 rows then fails the mark; a later cycle re-persists (no-op) and marks.
        await WaitUntil(() => schedStore.Items[id].Status == ScheduledState.Promoted, TimeSpan.FromSeconds(2));
        await promoter.StopAsync(CancellationToken.None);

        Assert.That(msgStore.Messages.Count, Is.EqualTo(2)); // no duplicate rows despite re-promotion
    }

    [Test]
    public async Task Does_Not_Promote_Not_Yet_Due_Message()
    {
        var registry = BuildRegistry();
        var schedStore = new FakeScheduleStore();
        var msgStore = new InMemoryMessageStore();
        var scheduler = new Scheduler(registry, new JsonSerializer(), new MsgFluxOptions(), schedStore);

        await scheduler.ScheduleAsync(new PromoTestMessage(), DateTimeOffset.UtcNow.AddHours(1)); // future

        var promoter = CreatePromoter(registry, schedStore, msgStore);
        await promoter.StartAsync(CancellationToken.None);
        await Task.Delay(150);
        await promoter.StopAsync(CancellationToken.None);

        Assert.That(msgStore.Messages, Is.Empty);
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (!condition() && sw.Elapsed < timeout)
            await Task.Delay(20);
    }

    public class PromoTestMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    public class ConsumerA : IConsume<PromoTestMessage>
    {
        public Task HandleAsync(PromoTestMessage message, CancellationToken ct) => Task.CompletedTask;
    }

    public class ConsumerB : IConsume<PromoTestMessage>
    {
        public Task HandleAsync(PromoTestMessage message, CancellationToken ct) => Task.CompletedTask;
    }
}
