using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

/// <summary>
/// Demonstrates M6: with per-source Parallel.ForEachAsync + shared semaphore,
/// a busy source can starve a quiet source via pull-ahead buffering.
/// With await foreach + semaphore, each source pulls one item at a time,
/// giving fair access to semaphore slots.
/// </summary>
public class EngineSourceFairnessTests
{
    [Test]
    public async Task Quiet_Source_Should_Not_Be_Starved_By_Busy_Source()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options
                .WithMaxDegreeOfParallelism(2)
                .WithRetry(1, TimeSpan.FromMilliseconds(1))
                .AddConsumer<SlowHandler>()
                .AddConsumer<FastHandler>();
        });

        // Register a second source that floods slow messages
        services.AddSingleton<SlowSource>();
        services.AddSingleton<IMessageSource>(sp => sp.GetRequiredService<SlowSource>());

        SlowHandler.Reset();
        FastHandler.Reset();

        var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<ISerializer>();

        // Seed the slow source with properly serialized messages
        var slowSource = provider.GetRequiredService<SlowSource>();
        var slowConsumerId = Registry.GetConsumerId(typeof(SlowHandler));
        for (var i = 0; i < 6; i++)
        {
            slowSource.Enqueue(new Message
            {
                MessageId = Guid.NewGuid().ToString(),
                ConsumerId = slowConsumerId,
                Payload = serializer.Serialize(new SlowMessage()),
                Headers = new Dictionary<string, string>(),
                MessageType = typeof(SlowMessage).FullName!,
                CreatedAt = DateTimeOffset.UtcNow
            });
        }

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Let slow messages saturate the semaphore
        await Task.Delay(50);

        // Inject one fast message into InMemoryMessageSource (separate source)
        var inMemory = provider.GetRequiredService<InMemoryMessageSource>();
        var fastConsumerId = Registry.GetConsumerId(typeof(FastHandler));
        await inMemory.PersistAsync([new Message
        {
            MessageId = Guid.NewGuid().ToString(),
            ConsumerId = fastConsumerId,
            Payload = serializer.Serialize(new FastMessage()),
            Headers = new Dictionary<string, string>(),
            MessageType = typeof(FastMessage).FullName!,
            CreatedAt = DateTimeOffset.UtcNow
        }]);

        // Act — wait for the fast message to be dispatched
        var sw = Stopwatch.StartNew();
        var deadline = TimeSpan.FromMilliseconds(500);
        while (sw.Elapsed < deadline && FastHandler.HandledCount == 0)
            await Task.Delay(10);
        sw.Stop();

        await engine.StopAsync(CancellationToken.None);

        // 6 slow × 200ms / DOP 2 = 600ms sequential. With fair scheduling,
        // the fast message should slip in within one slow-handler cycle.
        Assert.That(FastHandler.HandledCount, Is.EqualTo(1),
            "Fast message was never dispatched — source starvation");
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(400),
            $"Fast message took {sw.ElapsedMilliseconds}ms — likely starved by busy source");
    }

    public class SlowMessage { }
    public class FastMessage { }

    public class SlowHandler : IConsume<SlowMessage>
    {
        public static int HandledCount;
        public static void Reset() => HandledCount = 0;

        public async Task HandleAsync(SlowMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            await Task.Delay(200, ct);
        }
    }

    public class FastHandler : IConsume<FastMessage>
    {
        public static int HandledCount;
        public static void Reset() => HandledCount = 0;

        public Task HandleAsync(FastMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Separate IMessageSource backed by a Channel, used to flood slow messages
    /// independently from the InMemoryMessageSource.
    /// </summary>
    public class SlowSource : IMessageSource
    {
        private readonly System.Threading.Channels.Channel<Message> _channel =
            System.Threading.Channels.Channel.CreateUnbounded<Message>();

        public void Enqueue(Message msg) => _channel.Writer.TryWrite(msg);

        public async IAsyncEnumerable<DispatchItem> StreamAsync(
            [EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var msg in _channel.Reader.ReadAllAsync(ct))
            {
                yield return new DispatchItem(msg);
            }
        }
    }
}
