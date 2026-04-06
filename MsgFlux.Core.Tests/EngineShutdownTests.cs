using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class EngineShutdownTests
{
    [Test]
    public async Task StopAsync_Should_Wait_For_InFlight_Dispatches()
    {
        // Arrange — a slow durable consumer that takes 500ms to handle
        var store = new InMemoryMessageStore();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options
                .WithReplayInterval(TimeSpan.FromMilliseconds(50))
                .WithRetry(1, TimeSpan.FromMilliseconds(1))
                .AddConsumer<SlowShutdownHandler>(Semantics.AtLeastOnce);
        });

        SlowShutdownHandler.Reset();
        var provider = services.BuildServiceProvider();

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Act — publish a message and wait for the handler to START processing
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new SlowShutdownMessage());

        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline && SlowShutdownHandler.Started == 0)
            await Task.Delay(10);
        Assert.That(SlowShutdownHandler.Started, Is.EqualTo(1), "Handler never started");

        // Stop the engine while the handler is still running (handler takes 500ms)
        await engine.StopAsync(CancellationToken.None);

        // Assert — after StopAsync returns, the in-flight dispatch should have completed
        // and the message should be acknowledged in the store.
        var msg = store.Messages.Values.First();
        Assert.That(SlowShutdownHandler.Completed, Is.EqualTo(1),
            "Handler did not complete — in-flight dispatch was abandoned on shutdown");
        Assert.That(msg.State, Is.EqualTo(MessageState.Completed),
            "Message was not acknowledged — OnAck callback was orphaned on shutdown");
    }

    public class SlowShutdownMessage { }

    public class SlowShutdownHandler : IConsume<SlowShutdownMessage>
    {
        public static int Started;
        public static int Completed;
        public static void Reset() { Started = 0; Completed = 0; }

        public async Task HandleAsync(SlowShutdownMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref Started);
            await Task.Delay(500, CancellationToken.None); // ignore ct to simulate work that must finish
            Interlocked.Increment(ref Completed);
        }
    }
}
