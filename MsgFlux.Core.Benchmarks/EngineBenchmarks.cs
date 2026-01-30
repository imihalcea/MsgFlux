using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Benchmarks;

[MemoryDiagnoser]
public class EngineBenchmarks
{
    private IServiceProvider _serviceProvider = null!;
    private Engine _engine = null!;
    private IPublish _publisher = null!;
    private CancellationTokenSource _cts = null!;
    private Task _engineTask = null!;

    [Params(100, 1000, 2000)]
    public int MessageCount { get; set; }

    public class BenchmarkMessage { }

    public class BenchmarkConsumer : IConsume<BenchmarkMessage>
    {
        public static int ProcessedCount = 0;
        public static int TargetCount = 0;
        public static TaskCompletionSource Tcs = null!;

        public async Task HandleAsync(BenchmarkMessage message, CancellationToken ct)
        {
            await Task.Delay(10, ct); // Simulate small work
            if (Interlocked.Increment(ref ProcessedCount) == TargetCount)
            {
                Tcs.TrySetResult();
            }
        }
    }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        // Remove Console logging for benchmarks to avoid I/O overhead
        services.AddLogging();
        
        var options = new MsgFluxOptions { ChannelCapacity = 15_000 };
        services.AddSingleton(options);
        services.AddSingleton<IChannelRxTx>(new InMemoryRxTx(options));
        services.AddSingleton<ISerializer, JsonSerializer>();
        services.AddSingleton<Registry>();
        services.AddSingleton<Engine>();
        services.AddSingleton<IPublish, Publisher>();
        services.AddTransient<IConsume<BenchmarkMessage>, BenchmarkConsumer>();
        
        _serviceProvider = services.BuildServiceProvider();
        
        var registry = _serviceProvider.GetRequiredService<Registry>();
        registry.Register(typeof(BenchmarkMessage));

        _engine = _serviceProvider.GetRequiredService<Engine>();
        _publisher = _serviceProvider.GetRequiredService<IPublish>();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _cts = new CancellationTokenSource();
        BenchmarkConsumer.ProcessedCount = 0;
        BenchmarkConsumer.TargetCount = MessageCount;
        BenchmarkConsumer.Tcs = new TaskCompletionSource();
        _engineTask = _engine.StartAsync(_cts.Token);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _cts.Cancel();
        try { _engineTask.Wait(); } catch { }
        _cts.Dispose();
    }

    [Benchmark]
    public async Task ProcessMessages()
    {
        for (int i = 0; i < MessageCount; i++)
        {
            await _publisher.PublishAsync(new BenchmarkMessage(), CancellationToken.None);
        }

        await BenchmarkConsumer.Tcs.Task;
    }
}
