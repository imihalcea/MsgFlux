using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core;
using MsgFlux.Postgres;

namespace MsgFlux.Integration.Tests;

/// <summary>
/// Proves the regression introduced by claim-on-fetch: FetchUnprocessedAsync claims a whole batch
/// at once with a single processed_at = T0, but the engine dispatches those rows gradually (gated by
/// MaxDegreeOfParallelism). A row that waits in the dispatch queue — or is processed slowly — can pass
/// StaleProcessingTimeout while still owned by the claiming instance. Another instance then sees it as
/// stale, re-claims it, and the consumer runs twice.
///
/// Each test starts a single "worker" first so it claims the entire batch and begins draining it
/// slowly; the "stealer" instances are started a moment later and poll empty until the worker's rows
/// go stale, at which point they steal and re-process them. Every consumer invocation records the
/// message Id; duplicate delivery shows up as Total &gt; Distinct.
///
/// Handle durations are kept below StaleProcessingTimeout in all scenarios, so the duplication is NOT
/// "the consumer is slower than the timeout" (which would be legitimate re-delivery) — it is the
/// claim/dispatch decoupling. The trigger is guaranteed with margin: (messageCount / maxDop) * handle
/// is several times the stale timeout, so the worker cannot drain its batch before its rows go stale.
///
/// Timings are small (stale = 400ms) and the wait is active: we block until every message has been
/// handled at least once (bounded), then a short grace window, instead of a fixed multi-second sleep.
/// These tests fail today and should pass once the reservation window is decoupled from batch-claim time.
/// </summary>
public class StaleReclaimTests
{
    private static readonly TimeSpan Stale = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan ReplayInterval = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan StartGap = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan CoverageTimeout = TimeSpan.FromSeconds(8);
    private static readonly TimeSpan DrainGrace = TimeSpan.FromMilliseconds(1500);

    [SetUp]
    public async Task SetUp()
    {
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand("DELETE FROM msgflux_messages");
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    public async Task Scenario1_LargeBatch_With_Limited_Parallelism_Causes_Duplicate_Delivery()
    {
        // Worker claims the whole batch (processed_at = T0) and drains it serially (MaxDOP = 1).
        // Drain time ≈ 8 * 200ms = 1.6s ≫ 400ms stale, so the tail rows go stale while still
        // queued/owned and the stealer re-claims them. Each handle (200ms) is under the timeout.
        var (total, distinct) = await RunStaleScenario(
            stealerCount: 1, messageCount: 8, maxDop: 1, handleDelay: TimeSpan.FromMilliseconds(200));

        AssertExactlyOnce(total, distinct, 8);
    }

    [Test]
    public async Task Scenario2_QueueWait_Alone_Makes_A_Fast_Consumer_Reprocess()
    {
        // Each HandleAsync is short (100ms, a quarter of the 400ms stale timeout): the consumer is
        // NOT slow. The duplication comes purely from time spent waiting in the dispatch queue
        // behind earlier messages of the same claimed batch (drain ≈ 8 * 100ms = 800ms ≫ 400ms).
        var (total, distinct) = await RunStaleScenario(
            stealerCount: 1, messageCount: 8, maxDop: 1, handleDelay: TimeSpan.FromMilliseconds(100));

        AssertExactlyOnce(total, distinct, 8);
    }

    [Test]
    public async Task Scenario3_PublishSpike_Is_Reclaimed_En_Masse()
    {
        // A burst is claimed in bulk by the worker; while it drains (MaxDOP = 2, ≈ 16/2 * 150ms =
        // 1.2s ≫ 400ms) the rows go stale and the two stealers grab them en masse.
        var (total, distinct) = await RunStaleScenario(
            stealerCount: 2, messageCount: 16, maxDop: 2, handleDelay: TimeSpan.FromMilliseconds(150));

        AssertExactlyOnce(total, distinct, 16);
    }

    private static void AssertExactlyOnce(int total, int distinct, int messageCount)
    {
        Assert.That(distinct, Is.EqualTo(messageCount),
            $"Expected all {messageCount} messages handled at least once (distinct={distinct}); " +
            "coverage was not reached within the timeout.");
        Assert.That(total, Is.EqualTo(messageCount),
            $"{total - distinct} message(s) were processed more than once: a batch was claimed at once, " +
            "its queued rows passed StaleProcessingTimeout before dispatch, and another instance " +
            $"re-claimed them. (total handled={total}, distinct={distinct})");
    }

    private async Task<(int Total, int Distinct)> RunStaleScenario(
        int stealerCount, int messageCount, int maxDop, TimeSpan handleDelay)
    {
        StaleScenarioState.Reset(handleDelay);

        var worker = BuildInstance(maxDop);
        var stealers = Enumerable.Range(0, stealerCount).Select(_ => BuildInstance(maxDop)).ToArray();
        var all = new[] { worker }.Concat(stealers).ToArray();

        try
        {
            // Publish the whole batch before any engine runs.
            var publisher = worker.GetRequiredService<IPublish>();
            for (var i = 0; i < messageCount; i++)
                await publisher.PublishAsync(new StaleOrder(Guid.NewGuid()));

            // Start only the worker so it claims the entire batch and begins draining it slowly.
            await StartHosted(worker);
            await Task.Delay(StartGap);

            // Bring up the stealers: they poll empty until the worker's rows go stale, then grab them.
            foreach (var s in stealers)
                await StartHosted(s);

            // Wait until every message has been handled at least once (bounded), then a short grace
            // window so stealers finish the duplicate work they have already started.
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < CoverageTimeout && StaleScenarioState.Distinct < messageCount)
                await Task.Delay(25);
            await Task.Delay(DrainGrace);

            foreach (var p in all)
                await StopHosted(p);

            return (StaleScenarioState.Total, StaleScenarioState.Distinct);
        }
        finally
        {
            foreach (var p in all)
                await p.DisposeAsync();
        }
    }

    private static ServiceProvider BuildInstance(int maxDop)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFluxPostgres(PostgresContainerFixture.ConnectionString);
        services.AddMsgFlux(options =>
        {
            options.WithMaxDegreeOfParallelism(maxDop);
            options.WithStaleProcessingTimeout(Stale);
            options.WithReplayInterval(ReplayInterval);
            options.WithBufferedPublishing(flushInterval: TimeSpan.FromMilliseconds(50), flushThreshold: 1);
            options.AddConsumer<StaleConsumer>(Semantics.AtLeastOnce);
        });
        return services.BuildServiceProvider();
    }

    private static async Task StartHosted(ServiceProvider provider)
    {
        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StartAsync(CancellationToken.None);
    }

    private static async Task StopHosted(ServiceProvider provider)
    {
        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StopAsync(CancellationToken.None);
    }
}

public record StaleOrder(Guid Id);

public sealed class StaleConsumer : IConsume<StaleOrder>
{
    public async Task HandleAsync(StaleOrder message, CancellationToken ct)
    {
        await Task.Delay(StaleScenarioState.ProcessingDelay, ct);
        StaleScenarioState.Record(message.Id);
    }
}

/// <summary>Process-wide sink shared by the consumer across all instances.</summary>
public static class StaleScenarioState
{
    public static TimeSpan ProcessingDelay { get; private set; } = TimeSpan.Zero;
    private static readonly ConcurrentBag<Guid> Handled = new();

    public static void Reset(TimeSpan delay)
    {
        ProcessingDelay = delay;
        Handled.Clear();
    }

    public static void Record(Guid id) => Handled.Add(id);
    public static int Total => Handled.Count;
    public static int Distinct => Handled.Distinct().Count();
}
