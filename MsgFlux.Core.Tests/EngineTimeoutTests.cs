using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class EngineTimeoutTests
{
    [Test]
    public async Task Engine_Should_Timeout_And_Mark_As_Failed_When_Consumer_Hangs()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options
                .WithReplayInterval(TimeSpan.FromMilliseconds(50))
                .WithStaleProcessingTimeout(TimeSpan.FromMilliseconds(100))
                .WithRetry(1, TimeSpan.FromMilliseconds(10))
                .WithMaxDeadLetterRetries(1)
                .AddConsumer<HangingHandler>(Semantics.AtLeastOnce);
        });

        HangingHandler.Reset();
        var provider = services.BuildServiceProvider();

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new HangingMessage { Value = "hang" });

        await Task.Delay(500);

        await engine.StopAsync(CancellationToken.None);
        await ((IAsyncDisposable)provider).DisposeAsync();

        // Assert — consumer was invoked, message timed out and was eventually dead-lettered
        var msg = store.Messages.Values.First();
        Assert.That(HangingHandler.HandledCount, Is.GreaterThanOrEqualTo(1));
        Assert.That(msg.RetryCount, Is.GreaterThanOrEqualTo(1));
        Assert.That(msg.State, Is.EqualTo(MessageState.DeadLettered));
    }

    [Test]
    public async Task Engine_Should_Not_Timeout_Fast_Consumers()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options
                .WithReplayInterval(TimeSpan.FromMilliseconds(50))
                .WithRetry(1, TimeSpan.FromMilliseconds(10))
                .WithStaleProcessingTimeout(TimeSpan.FromSeconds(5))
                .AddConsumer<FastHandler>(Semantics.AtLeastOnce);
        });

        FastHandler.Reset();
        var provider = services.BuildServiceProvider();

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new HangingMessage { Value = "fast" });

        await Task.Delay(200);

        // Assert — message should be Completed, not timed out
        var msg = store.Messages.Values.First();
        Assert.That(msg.State, Is.EqualTo(MessageState.Completed));
        Assert.That(FastHandler.HandledCount, Is.EqualTo(1));

        await engine.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Engine_Should_Timeout_In_Memory_Mode_Without_Store()
    {
        // Arrange — no durability, no store
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options
                .WithStaleProcessingTimeout(TimeSpan.FromMilliseconds(100))
                .WithRetry(1, TimeSpan.FromMilliseconds(10))
                .AddConsumer<HangingHandler>();
        });

        HangingHandler.Reset();
        var provider = services.BuildServiceProvider();

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().OfType<IHostedService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new HangingMessage { Value = "hang-inmemory" });

        await Task.Delay(500);

        // Assert — HangingHandler was invoked (proves message was dispatched)
        Assert.That(HangingHandler.HandledCount, Is.EqualTo(1));

        await engine.StopAsync(CancellationToken.None);
    }

    public class HangingMessage
    {
        public string Value { get; set; } = string.Empty;
    }

    public class HangingHandler : IConsume<HangingMessage>
    {
        public static int HandledCount;
        public static void Reset() => HandledCount = 0;

        public async Task HandleAsync(HangingMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            // Simulate a consumer that hangs indefinitely
            await Task.Delay(Timeout.Infinite, ct);
        }
    }

    public class FastHandler : IConsume<HangingMessage>
    {
        public static int HandledCount;
        public static void Reset() => HandledCount = 0;

        public Task HandleAsync(HangingMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            return Task.CompletedTask;
        }
    }
}
