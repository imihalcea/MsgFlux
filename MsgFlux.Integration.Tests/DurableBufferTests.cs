using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core;
using MsgFlux.Postgres;

namespace MsgFlux.Integration.Tests;

/// <summary>
/// End-to-end proof of the durable buffer's group-commit contract against a real PostgreSQL store.
/// These tests exercise the boundary the unit tests can only simulate with an in-memory store:
/// that a buffered publish is acknowledged only once the row is actually committed to Postgres,
/// that batched publishing still delivers each message exactly once, and — the scenario that
/// motivated this work — that messages sitting in the buffer at shutdown are flushed, not lost.
/// </summary>
public class DurableBufferTests
{
    [SetUp]
    public async Task SetUp()
    {
        BufferTracker.Reset();
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand("DELETE FROM msgflux_messages");
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    public async Task Buffered_Publish_Is_Acknowledged_Only_After_Row_Is_Committed()
    {
        // High threshold + long interval: the batch only flushes when the threshold is hit, so we can
        // observe that publishes below it are NOT yet acknowledged and NOT yet in Postgres.
        var provider = BuildInstance(flushThreshold: 5, flushInterval: TimeSpan.FromSeconds(30));
        try
        {
            var publisher = provider.GetRequiredService<IPublish>();

            // Four publishes below the threshold — group-commit keeps them pending, nothing committed.
            var pending = Enumerable.Range(0, 4)
                .Select(_ => publisher.PublishAsync(new BufferedEvent(Guid.NewGuid())))
                .ToList();

            await Task.Delay(300);
            Assert.That(await CountRows(), Is.EqualTo(0),
                "Below threshold: no row should be committed to Postgres yet.");
            Assert.That(pending.All(t => !t.IsCompleted), Is.True,
                "Group-commit: a publisher must NOT be acknowledged before its row is persisted.");

            // Fifth publish reaches the threshold and flushes the whole batch.
            pending.Add(publisher.PublishAsync(new BufferedEvent(Guid.NewGuid())));
            await Task.WhenAll(pending);

            Assert.That(await CountRows(), Is.EqualTo(5),
                "Once the flush completes, every acknowledged publish must be durable in Postgres.");
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Test]
    public async Task Batched_Publishing_Delivers_Each_Message_Exactly_Once_EndToEnd()
    {
        const int messageCount = 20;
        // Threshold 10: the burst is committed in batches rather than one row at a time.
        var provider = BuildInstance(flushThreshold: 10, flushInterval: TimeSpan.FromMilliseconds(100));
        try
        {
            await StartAllAsync(provider);

            var publisher = provider.GetRequiredService<IPublish>();
            var publishes = Enumerable.Range(0, messageCount)
                .Select(_ => publisher.PublishAsync(new BufferedEvent(Guid.NewGuid())));
            await Task.WhenAll(publishes);

            // Every acknowledged publish is already durable.
            Assert.That(await CountRows(), Is.EqualTo(messageCount));

            // The engine drains the batches; each message is handled exactly once.
            await WaitUntil(() => BufferTracker.DistinctHandled == messageCount);
            await WaitUntilAsync(async () => await CountByState(MessageState.Completed) == messageCount);

            Assert.That(BufferTracker.TotalHandled, Is.EqualTo(messageCount),
                $"Expected {messageCount} deliveries with no duplicates, got {BufferTracker.TotalHandled}.");

            await StopAllAsync(provider);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Test]
    public async Task Messages_In_Buffer_Are_Flushed_On_Shutdown_Not_Lost()
    {
        // The original concern: a stop while messages are still buffered. High threshold + long
        // interval mean nothing flushes on its own — only the ordered shutdown flush can persist it.
        var provider = BuildInstance(flushThreshold: 1000, flushInterval: TimeSpan.FromSeconds(30));
        try
        {
            var publisher = provider.GetRequiredService<IPublish>();

            var publish = publisher.PublishAsync(new BufferedEvent(Guid.NewGuid()));
            await Task.Delay(300);

            Assert.That(await CountRows(), Is.EqualTo(0),
                "Precondition: the message is still in the buffer, not yet committed.");
            Assert.That(publish.IsCompleted, Is.False);

            // Ordered shutdown of the buffer (as a hosted service) must flush what remains.
            var buffer = provider.GetRequiredService<DurableBuffer>();
            await buffer.StopAsync(CancellationToken.None);

            await publish; // completes once the shutdown flush commits the row
            Assert.That(await CountRows(), Is.EqualTo(1),
                "Buffered message must be flushed to Postgres on shutdown — never silently dropped.");
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    private static ServiceProvider BuildInstance(int flushThreshold, TimeSpan flushInterval)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFluxPostgres(PostgresContainerFixture.ConnectionString);
        services.AddMsgFlux(options =>
        {
            options.WithReplayInterval(TimeSpan.FromMilliseconds(100));
            options.WithBufferedPublishing(flushInterval, flushThreshold);
            options.AddConsumer<BufferedEventConsumer>(Semantics.AtLeastOnce);
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

    private static async Task<long> CountRows()
    {
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand("SELECT COUNT(*) FROM msgflux_messages");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<long> CountByState(MessageState state)
    {
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand(
            "SELECT COUNT(*) FROM msgflux_messages WHERE state = $1");
        cmd.Parameters.AddWithValue((int)state);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 10000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs)
                Assert.Fail("Condition was not met within the timeout.");
            await Task.Delay(25);
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, int timeoutMs = 10000)
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

public record BufferedEvent(Guid Id);

public sealed class BufferedEventConsumer : IConsume<BufferedEvent>
{
    public Task HandleAsync(BufferedEvent message, CancellationToken ct)
    {
        BufferTracker.Record(message.Id);
        return Task.CompletedTask;
    }
}

/// <summary>Process-wide delivery sink shared by <see cref="BufferedEventConsumer"/>.</summary>
public static class BufferTracker
{
    private static readonly ConcurrentBag<Guid> Handled = new();

    public static void Record(Guid id) => Handled.Add(id);
    public static void Reset() => Handled.Clear();
    public static int TotalHandled => Handled.Count;
    public static int DistinctHandled => Handled.Distinct().Count();
}
