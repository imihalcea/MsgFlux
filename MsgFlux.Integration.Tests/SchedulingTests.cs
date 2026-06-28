using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core;
using MsgFlux.Postgres;

namespace MsgFlux.Integration.Tests;

/// <summary>
/// End-to-end proof of scheduled (deferred) delivery against a real PostgreSQL store: a message
/// scheduled for a future date is persisted to the dedicated scheduled_messages table, withheld
/// until its due date, then promoted into the hot path and delivered through the normal engine
/// pipeline. Also covers cancellation before promotion and fan-out at the due date.
/// </summary>
public class SchedulingTests
{
    [SetUp]
    public async Task SetUp()
    {
        ScheduleTracker.Reset();
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand(
            "DELETE FROM msgflux.messages; DELETE FROM msgflux.scheduled_messages;");
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    public async Task Scheduled_Message_Is_Withheld_Until_Due_Then_Delivered()
    {
        var provider = BuildInstance();
        try
        {
            await StartAllAsync(provider);
            var scheduler = provider.GetRequiredService<ISchedule>();

            var id = Guid.NewGuid();
            var deliverAt = DateTimeOffset.UtcNow.AddSeconds(1);
            var returnedId = await scheduler.ScheduleAsync(new ScheduledEvent(id), deliverAt);

            // Before the due date: persisted as Scheduled, not promoted, not delivered.
            // 300ms is several promotion cycles (100ms) yet well short of the 1s due date.
            await Task.Delay(300);
            Assert.That(ScheduleTracker.Handled("sched", id), Is.False,
                "A scheduled message must not be delivered before its due date.");
            Assert.That(await CountScheduledByState(ScheduledState.Scheduled), Is.EqualTo(1));
            Assert.That(await CountMessages(), Is.EqualTo(0),
                "Nothing should reach the hot path before the due date.");

            // After the due date: promoted and delivered exactly once.
            await WaitUntil(() => ScheduleTracker.Handled("sched", id));

            await WaitUntilAsync(async () => await CountScheduledByState(ScheduledState.Promoted) == 1);
            await WaitUntilAsync(async () => await CountMessagesByState(MessageState.Completed) == 1);
            Assert.That(ScheduleTracker.TotalFor("sched", id), Is.EqualTo(1), "Expected exactly one delivery.");
            Assert.That(returnedId, Is.EqualTo(id).Or.Not.EqualTo(Guid.Empty)); // id is returned for cancellation

            await StopAllAsync(provider);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Test]
    public async Task Cancelled_Scheduled_Message_Is_Never_Delivered()
    {
        var provider = BuildInstance();
        try
        {
            await StartAllAsync(provider);
            var scheduler = provider.GetRequiredService<ISchedule>();

            var id = Guid.NewGuid();
            var scheduledId = await scheduler.ScheduleAsync(new ScheduledEvent(id), DateTimeOffset.UtcNow.AddMilliseconds(500));

            var cancelled = await scheduler.CancelScheduledAsync(scheduledId);
            Assert.That(cancelled, Is.True, "Cancellation should succeed while the message is still pending.");

            // Wait well past the 500ms due date and many promotion cycles (100ms): had it not been
            // cancelled, it would certainly have been promoted and delivered by now.
            await Task.Delay(1200);

            Assert.That(ScheduleTracker.Handled("sched", id), Is.False, "A cancelled message must never be delivered.");
            Assert.That(await CountScheduledByState(ScheduledState.Cancelled), Is.EqualTo(1));
            Assert.That(await CountMessages(), Is.EqualTo(0));

            await StopAllAsync(provider);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Test]
    public async Task Due_Message_Fans_Out_To_All_Durable_Consumers_At_Promotion()
    {
        var provider = BuildInstance();
        try
        {
            await StartAllAsync(provider);
            var scheduler = provider.GetRequiredService<ISchedule>();

            var id = Guid.NewGuid();
            // Due almost immediately: exercise the promotion fan-out path quickly.
            await scheduler.ScheduleAsync(new FanOutEvent(id), DateTimeOffset.UtcNow.AddMilliseconds(200));

            await WaitUntil(() => ScheduleTracker.Handled("fan-a", id) && ScheduleTracker.Handled("fan-b", id));
            await WaitUntilAsync(async () => await CountMessagesByState(MessageState.Completed) == 2);

            Assert.That(ScheduleTracker.TotalFor("fan-a", id), Is.EqualTo(1));
            Assert.That(ScheduleTracker.TotalFor("fan-b", id), Is.EqualTo(1));
            Assert.That(await CountScheduledByState(ScheduledState.Promoted), Is.EqualTo(1),
                "A single scheduled row fans out to N hot-path rows at promotion.");

            await StopAllAsync(provider);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    private static ServiceProvider BuildInstance()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFluxPostgres(PostgresContainerFixture.ConnectionString);
        services.AddMsgFlux(options =>
        {
            options.WithReplayInterval(TimeSpan.FromMilliseconds(100));
            options.WithPromotionInterval(TimeSpan.FromMilliseconds(100));
            options.WithBufferedPublishing(TimeSpan.FromMilliseconds(50), flushThreshold: 1);
            options.AddConsumer<ScheduledEventConsumer>(Semantics.AtLeastOnce);
            options.AddConsumer<FanOutConsumerA>(Semantics.AtLeastOnce);
            options.AddConsumer<FanOutConsumerB>(Semantics.AtLeastOnce);
        });
        return services.BuildServiceProvider();
    }

    private static async Task StartAllAsync(ServiceProvider provider)
    {
        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StartAsync(CancellationToken.None);
    }

    private static async Task StopAllAsync(ServiceProvider provider)
    {
        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StopAsync(CancellationToken.None);
    }

    private static async Task<long> CountMessages()
    {
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand("SELECT COUNT(*) FROM msgflux.messages");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<long> CountMessagesByState(MessageState state)
    {
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand(
            "SELECT COUNT(*) FROM msgflux.messages WHERE state = $1");
        cmd.Parameters.AddWithValue((short)state);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<long> CountScheduledByState(ScheduledState state)
    {
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand(
            "SELECT COUNT(*) FROM msgflux.scheduled_messages WHERE state = $1");
        cmd.Parameters.AddWithValue((short)state);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 15000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs)
                Assert.Fail("Condition was not met within the timeout.");
            await Task.Delay(25);
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, int timeoutMs = 15000)
    {
        var sw = Stopwatch.StartNew();
        while (!await condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs)
                Assert.Fail("Condition was not met within the timeout.");
            await Task.Delay(25);
        }
    }
}

public record ScheduledEvent(Guid Id);
public record FanOutEvent(Guid Id);

public sealed class ScheduledEventConsumer : IConsume<ScheduledEvent>
{
    public Task HandleAsync(ScheduledEvent message, CancellationToken ct)
    {
        ScheduleTracker.Record("sched", message.Id);
        return Task.CompletedTask;
    }
}

public sealed class FanOutConsumerA : IConsume<FanOutEvent>
{
    public Task HandleAsync(FanOutEvent message, CancellationToken ct)
    {
        ScheduleTracker.Record("fan-a", message.Id);
        return Task.CompletedTask;
    }
}

public sealed class FanOutConsumerB : IConsume<FanOutEvent>
{
    public Task HandleAsync(FanOutEvent message, CancellationToken ct)
    {
        ScheduleTracker.Record("fan-b", message.Id);
        return Task.CompletedTask;
    }
}

/// <summary>Process-wide delivery sink shared by the scheduling test consumers, tagged per consumer.</summary>
public static class ScheduleTracker
{
    private static readonly ConcurrentBag<(string Tag, Guid Id)> Handled_ = new();

    public static void Record(string tag, Guid id) => Handled_.Add((tag, id));
    public static void Reset() => Handled_.Clear();
    public static bool Handled(string tag, Guid id) => Handled_.Any(h => h.Tag == tag && h.Id == id);
    public static int TotalFor(string tag, Guid id) => Handled_.Count(h => h.Tag == tag && h.Id == id);
}
