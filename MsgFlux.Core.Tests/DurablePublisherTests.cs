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
        services.AddMsgFlux(options =>
        {
            options.WithDurability();
            options.AddConsumer<DurableTestHandler>();
        });
        services.AddSingleton<IMessageStore>(store);

        DurableTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        // Start only Engine (no ReplayService — nothing to replay, and it would race with publish)
        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new DurableTestMessage { Content = "Hello Durable" });

        await Task.Delay(500);

        // Assert
        Assert.That(store.Messages.Count, Is.EqualTo(1));
        Assert.That(store.Messages.Values.First().MessageType, Is.EqualTo(nameof(DurableTestMessage)));
        Assert.That(DurableTestHandler.HandledCount, Is.EqualTo(1));

        await engine.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task DurablePublisher_Should_Not_Enqueue_When_Store_Fails()
    {
        // Arrange
        var store = new FailingMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options.WithDurability();
            options.AddConsumer<DurableTestHandler>();
        });
        services.AddSingleton<IMessageStore>(store);

        DurableTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        var hostedService = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await hostedService.StartAsync(CancellationToken.None);

        // Act & Assert
        var publisher = provider.GetRequiredService<IPublish>();
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await publisher.PublishAsync(new DurableTestMessage { Content = "Should fail" }));

        await Task.Delay(200);
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
