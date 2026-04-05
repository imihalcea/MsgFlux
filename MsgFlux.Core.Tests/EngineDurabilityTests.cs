using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class EngineDurabilityTests
{
    [Test]
    public async Task Engine_Should_Acknowledge_On_Success()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options.WithReplayInterval(TimeSpan.FromMilliseconds(50));
            options.AddConsumer<SuccessHandler>(Semantics.AtLeastOnce);
        });

        SuccessHandler.Reset();
        var provider = services.BuildServiceProvider();

        var hostedService = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await hostedService.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new AckTestMessage { Value = "test" });

        await Task.Delay(500);

        // Assert
        var msg = store.Messages.Values.First();
        Assert.That(msg.State, Is.EqualTo(MessageState.Completed));

        await hostedService.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Engine_Should_Mark_As_Failed_On_Consumer_Failure()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            // Single-shot polling window for determinism: one dispatch → one Failed state → test stops.
            options.WithReplayInterval(TimeSpan.FromSeconds(5));
            options.AddConsumer<FailingHandler>(Semantics.AtLeastOnce);
        });

        FailingHandler.Reset();
        var provider = services.BuildServiceProvider();

        var hostedService = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await hostedService.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new AckTestMessage { Value = "will-fail" });

        await Task.Delay(2000); // Wait for Polly retries (3×) + OnFail
        await hostedService.StopAsync(CancellationToken.None);

        // Assert
        var msg = store.Messages.Values.First();
        Assert.That(msg.State, Is.EqualTo(MessageState.Failed));
    }

    public class AckTestMessage
    {
        public string Value { get; set; } = string.Empty;
    }

    public class SuccessHandler : IConsume<AckTestMessage>
    {
        public static int HandledCount;
        public static void Reset() => HandledCount = 0;

        public Task HandleAsync(AckTestMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            return Task.CompletedTask;
        }
    }

    public class FailingHandler : IConsume<AckTestMessage>
    {
        public static int CallCount;
        public static void Reset() => CallCount = 0;

        public Task HandleAsync(AckTestMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            throw new InvalidOperationException("Consumer failure");
        }
    }
}
