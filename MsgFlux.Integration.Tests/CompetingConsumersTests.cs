using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core;
using MsgFlux.Postgres;
using Npgsql;

namespace MsgFlux.Integration.Tests;

/// <summary>
/// End-to-end proof of the horizontal-scaling defect: N application instances running the same
/// AtLeastOnce consumer against a shared store must deliver each message to the group exactly once
/// (competing-consumers). Each instance here is a fully wired ServiceProvider (its own
/// EngineService + PollingStoreSource + NpgsqlDataSource) pointing at the same PostgreSQL database,
/// mirroring N pods behind a load balancer.
///
/// Because <see cref="PostgresMessageStore.FetchUnprocessedAsync"/> does not claim rows atomically,
/// every instance fetches the same Pending row on its first poll and the message is handled N times.
/// This test asserts the message is handled exactly once; it fails today and will pass once fetch
/// becomes a claim-on-fetch (FOR UPDATE SKIP LOCKED ... RETURNING within one transaction).
/// </summary>
public class CompetingConsumersTests
{
    private const int InstanceCount = 8;

    [SetUp]
    public async Task SetUp()
    {
        DeliveryTracker.Reset();
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand("DELETE FROM msgflux_messages");
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    public async Task Single_Message_Should_Be_Handled_Once_Across_N_Instances()
    {
        var providers = Enumerable.Range(0, InstanceCount)
            .Select(_ => BuildInstance())
            .ToArray();

        try
        {
            // Publish ONE message before any engine starts. With BufferFlushThreshold = 1 the
            // DurableBuffer flushes inline, so exactly one Pending row lands in the shared store.
            var publisher = providers[0].GetRequiredService<IPublish>();
            await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid()));

            Assert.That(await CountRows(), Is.EqualTo(1),
                "Precondition: exactly one durable row should exist before the instances start.");

            // Start all instances at once so their first poll races for the same Pending row.
            var hostedByInstance = providers
                .Select(p => p.GetServices<IHostedService>().ToArray())
                .ToArray();

            await Task.WhenAll(hostedByInstance.Select(async services =>
            {
                foreach (var hs in services)
                    await hs.StartAsync(CancellationToken.None);
            }));

            // Give every instance several poll cycles to fetch and dispatch the message.
            await Task.Delay(TimeSpan.FromSeconds(2));

            await Task.WhenAll(hostedByInstance.Select(async services =>
            {
                foreach (var hs in services)
                    await hs.StopAsync(CancellationToken.None);
            }));

            Assert.That(DeliveryTracker.TotalHandled, Is.EqualTo(1),
                $"Message was handled {DeliveryTracker.TotalHandled} times across {InstanceCount} instances. " +
                "Fetch is not atomic with the claim, so every instance picked up the same Pending row — " +
                "duplicate delivery under horizontal scaling.");
        }
        finally
        {
            foreach (var p in providers)
                await p.DisposeAsync();
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
            options.WithBufferedPublishing(flushInterval: TimeSpan.FromMilliseconds(50), flushThreshold: 1);
            options.AddConsumer<OrderConsumer>(Semantics.AtLeastOnce);
        });
        return services.BuildServiceProvider();
    }

    private static async Task<long> CountRows()
    {
        await using var cmd = PostgresContainerFixture.DataSource.CreateCommand("SELECT COUNT(*) FROM msgflux_messages");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}

public record OrderPlaced(Guid OrderId);

public sealed class OrderConsumer : IConsume<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, CancellationToken ct)
    {
        DeliveryTracker.Record(message.OrderId);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Process-wide counter shared by the consumer across all instances (each instance resolves its own
/// OrderConsumer, but they all record into the same static sink).
/// </summary>
public static class DeliveryTracker
{
    private static readonly ConcurrentBag<Guid> Handled = new();

    public static void Record(Guid orderId) => Handled.Add(orderId);
    public static void Reset() => Handled.Clear();
    public static int TotalHandled => Handled.Count;
}
