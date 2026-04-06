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
                MessageId = Guid.NewGuid(),
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
            MessageId = Guid.NewGuid(),
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

    [Test]
    public async Task Messages_From_Multiple_Sources_Should_Interleave()
    {
        // Arrange — two sources (A and B), DOP=2 with non-instant handlers.
        // With DOP=2, Source A grabs up to 2 slots initially, but when one frees
        // up, Source B competes fairly for the next slot. We should see interleaving
        // rather than one source fully drained before the other.
        var sourceA = new TrackedSource("A");
        var sourceB = new TrackedSource("B");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options
                .WithMaxDegreeOfParallelism(2)
                .WithRetry(1, TimeSpan.FromMilliseconds(1))
                .AddConsumer<OrderTracker>();
        });

        // Replace default sources with our tracked ones
        services.AddSingleton<IMessageSource>(sourceA);
        services.AddSingleton<IMessageSource>(sourceB);

        OrderTracker.Reset();
        var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<ISerializer>();
        var consumerId = Registry.GetConsumerId(typeof(OrderTracker));

        // Seed both sources with 6 messages each, tagged with source origin
        for (var i = 1; i <= 6; i++)
        {
            sourceA.Enqueue(MakeMessage($"A{i}", consumerId, serializer));
            sourceB.Enqueue(MakeMessage($"B{i}", consumerId, serializer));
        }

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Wait for all 12 messages to be handled
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline && OrderTracker.Order.Count < 12)
            await Task.Delay(10);

        await engine.StopAsync(CancellationToken.None);

        // Assert — all 12 messages handled
        var order = OrderTracker.Order.ToList();
        Console.WriteLine($"Dispatch order: [{string.Join(", ", order)}]");
        Assert.That(order, Has.Count.EqualTo(12));

        // Assert — sources should interleave after the initial DOP fill.
        // With DOP=2, Source A may grab the first 2 slots, but after that
        // both sources compete fairly. A run of 6+ means one source was fully
        // drained before the other started — no interleaving at all.
        var maxConsecutive = MaxConsecutiveRun(order);
        Assert.That(maxConsecutive, Is.LessThan(6),
            $"Dispatch order [{string.Join(", ", order)}] shows one source monopolizing dispatch");
    }

    private static Message MakeMessage(string tag, string consumerId, ISerializer serializer) => new()
    {
        MessageId = Guid.NewGuid(),
        ConsumerId = consumerId,
        Payload = serializer.Serialize(new TaggedMessage { Tag = tag }),
        Headers = new Dictionary<string, string>(),
        MessageType = typeof(TaggedMessage).FullName!,
        CreatedAt = DateTimeOffset.UtcNow
    };

    private static int MaxConsecutiveRun(List<string> order)
    {
        var max = 1;
        var current = 1;
        for (var i = 1; i < order.Count; i++)
        {
            if (order[i][0] == order[i - 1][0])
                current++;
            else
                current = 1;
            max = Math.Max(max, current);
        }
        return max;
    }

    public class TaggedMessage
    {
        public string Tag { get; set; } = string.Empty;
    }

    public class OrderTracker : IConsume<TaggedMessage>
    {
        public static readonly List<string> Order = [];
        private static readonly Lock Lock = new();
        public static void Reset() { lock (Lock) Order.Clear(); }

        public async Task HandleAsync(TaggedMessage message, CancellationToken ct)
        {
            lock (Lock) Order.Add(message.Tag);
            await Task.Delay(50, ct); // hold the semaphore slot to create contention
        }
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

    public class TrackedSource(string name) : IMessageSource
    {
        private readonly System.Threading.Channels.Channel<Message> _channel =
            System.Threading.Channels.Channel.CreateUnbounded<Message>();

        public void Enqueue(Message msg) => _channel.Writer.TryWrite(msg);
        public void Complete() => _channel.Writer.TryComplete();

        public async IAsyncEnumerable<DispatchItem> StreamAsync(
            [EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var msg in _channel.Reader.ReadAllAsync(ct))
            {
                yield return new DispatchItem(msg);
            }
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
        public void Complete() => _channel.Writer.TryComplete();

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
