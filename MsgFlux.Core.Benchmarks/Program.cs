using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;
using MsgFlux.Core;
using MsgFlux.Postgres;
using Npgsql;
using Testcontainers.PostgreSql;

// ── Postgres container ──────────────────────────────────────────
Console.WriteLine("Starting PostgreSQL container...");
var container = new PostgreSqlBuilder().WithImage("postgres:17-alpine").Build();
await container.StartAsync();
var connectionString = container.GetConnectionString();

var ds = NpgsqlDataSource.Create(connectionString);
var init = new SchemaInitializer(ds, new PostgresOptions { AutoCreateSchema = true },
    NullLogger<SchemaInitializer>.Instance);
await init.StartAsync(CancellationToken.None);
await init.ExecuteTask!;
await ds.DisposeAsync();
Console.WriteLine("PostgreSQL ready.\n");

// ── Benchmarks ──────────────────────────────────────────────────
int[] sizes = [100, 1_000, 5_000];
string[] modes = ["AtMostOnce", "AtLeastOnce", "Mixed"];
const int warmup = 3;
const int runs = 10;

Console.WriteLine($"{"Mode",-15} {"Messages",10} {"Mean",10} {"Min",10} {"Max",10} {"Throughput",15}");
Console.WriteLine(new string('-', 70));

foreach (var mode in modes)
{
    // One ServiceProvider per mode — started once, reused across all sizes and runs.
    await using var provider = BuildProvider(mode, connectionString);
    var publisher = provider.GetRequiredService<IPublish>();
    var hostedServices = provider.GetServices<IHostedService>().ToArray();

    foreach (var hs in hostedServices)
        await hs.StartAsync(CancellationToken.None);

    foreach (var count in sizes)
    {
        var times = new List<double>();

        for (var run = 0; run < warmup + runs; run++)
        {
            var target = mode == "Mixed" ? count * 2 : count;
            BenchState.Reset(target);

            var sw = Stopwatch.StartNew();

            for (var i = 0; i < count; i++)
                await publisher.PublishAsync(new BenchMessage(), CancellationToken.None);

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try
            {
                await BenchState.Tcs.Task.WaitAsync(timeout.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"  [{mode}] TIMEOUT at run {run}! processed={BenchState.ProcessedCount}/{target}");
                break;
            }
            sw.Stop();

            if (run >= warmup)
                times.Add(sw.Elapsed.TotalMilliseconds);
        }

        if (times.Count > 0)
        {
            var mean = times.Average();
            var throughput = count / (mean / 1000.0);
            Console.WriteLine($"{mode,-15} {count,10} {mean,9:F1}ms {times.Min(),9:F1}ms {times.Max(),9:F1}ms {throughput,12:F0} msg/s");
        }
    }

    foreach (var hs in hostedServices)
        await hs.StopAsync(CancellationToken.None);
}

Console.WriteLine("\nDone.");
await container.DisposeAsync();

// ── Build ServiceProvider ───────────────────────────────────────
static ServiceProvider BuildProvider(string mode, string connectionString)
{
    var services = new ServiceCollection();
    services.AddLogging();

    if (mode is "AtLeastOnce" or "Mixed")
        services.AddMsgFluxPostgres(connectionString);

    services.AddMsgFlux(options =>
    {
        options.WithChannelCapacity(15_000);
        options.WithRetry(1, TimeSpan.FromMilliseconds(10));
        options.WithReplayInterval(TimeSpan.FromMilliseconds(50));
        options.WithBufferedPublishing(
            flushInterval: TimeSpan.FromMilliseconds(100),
            flushThreshold: 50);

        switch (mode)
        {
            case "AtMostOnce":
                options.AddConsumer<AtMostOnceConsumer>();
                break;
            case "AtLeastOnce":
                options.AddConsumer<AtLeastOnceConsumer>(Semantics.AtLeastOnce);
                break;
            case "Mixed":
                options.AddConsumer<AtMostOnceConsumer>();
                options.AddConsumer<AtLeastOnceConsumer>(Semantics.AtLeastOnce);
                break;
        }
    });

    return services.BuildServiceProvider();
}

// ── Types ───────────────────────────────────────────────────────
public class BenchMessage { }

public static class BenchState
{
    public static int ProcessedCount;
    public static int TargetCount;
    public static TaskCompletionSource Tcs = null!;

    public static void Reset(int target)
    {
        ProcessedCount = 0;
        TargetCount = target;
        Tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public static void Signal()
    {
        if (Interlocked.Increment(ref ProcessedCount) == TargetCount)
            Tcs.TrySetResult();
    }
}

public class AtMostOnceConsumer : IConsume<BenchMessage>
{
    public Task HandleAsync(BenchMessage message, CancellationToken ct)
    {
        BenchState.Signal();
        return Task.CompletedTask;
    }
}

public class AtLeastOnceConsumer : IConsume<BenchMessage>
{
    public Task HandleAsync(BenchMessage message, CancellationToken ct)
    {
        BenchState.Signal();
        return Task.CompletedTask;
    }
}
