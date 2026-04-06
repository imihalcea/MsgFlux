using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class DurablePublisherTests
{
    [Test]
    public async Task DurablePublisher_Should_Persist_Before_Enqueue()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options.WithReplayInterval(TimeSpan.FromMilliseconds(50));
            options.WithRetry(1, TimeSpan.FromMilliseconds(10));
            options.AddConsumer<DurableTestHandler>(Semantics.AtLeastOnce);
        });

        DurableTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new DurableTestMessage { Content = "Hello Durable" });

        await Task.Delay(200);

        // Assert
        Assert.That(store.Messages.Count, Is.EqualTo(1));
        Assert.That(store.Messages.Values.First().MessageType, Is.EqualTo(typeof(DurableTestMessage).FullName));
        Assert.That(DurableTestHandler.HandledCount, Is.EqualTo(1));

        await engine.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task DurablePublisher_Should_Buffer_When_Store_Fails()
    {
        // Arrange — store that always fails
        var store = new FailingMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options.WithReplayInterval(TimeSpan.FromMilliseconds(50));
            options.WithRetry(1, TimeSpan.FromMilliseconds(10));
            options.AddConsumer<DurableTestHandler>(Semantics.AtLeastOnce);
        });

        DurableTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        var hostedService = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await hostedService.StartAsync(CancellationToken.None);

        // Act — publish does not throw; the buffer absorbs the store failure
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new DurableTestMessage { Content = "Buffered" });

        await Task.Delay(100);

        // Assert — handler never called (messages stuck in buffer, never persisted)
        Assert.That(DurableTestHandler.HandledCount, Is.EqualTo(0));

        await hostedService.StopAsync(CancellationToken.None);
    }

    public class DurableTestMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    public class DurableTestHandler : IConsume<DurableTestMessage>
    {
        public static int HandledCount;
        public static void Reset() => HandledCount = 0;

        public Task HandleAsync(DurableTestMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            return Task.CompletedTask;
        }
    }
}
