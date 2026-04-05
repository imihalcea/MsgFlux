using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

public class MessageReplayServiceTests
{
    [Test]
    public async Task ReplayService_Should_Replay_Unprocessed_Messages()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var serializer = new JsonSerializer();

        // Pre-persist a message as if it was saved before a crash
        var payload = serializer.Serialize(new ReplayTestMessage { Data = "Replayed" });
        var consumerId = Registry.GetConsumerId(typeof(ReplayTestHandler));
        var persisted = new PersistedMessage
        {
            MessageId = Guid.NewGuid().ToString(),
            ConsumerId = consumerId,
            Payload = payload,
            Headers = new Dictionary<string, string>(),
            MessageType = nameof(ReplayTestMessage),
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        };
        await store.PersistAsync(new[] { persisted });

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options.AddConsumer<ReplayTestHandler>(Semantics.AtLeastOnce);
        });

        ReplayTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        // Start replay service first (it runs before Engine per spec)
        var replayService = provider.GetServices<IHostedService>().OfType<MessageReplayService>().First();
        await replayService.StartAsync(CancellationToken.None);

        // Then start Engine
        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        await Task.Delay(500);

        // Assert
        Assert.That(ReplayTestHandler.HandledCount, Is.EqualTo(1));
        Assert.That(ReplayTestHandler.LastData, Is.EqualTo("Replayed"));

        await engine.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ReplayService_Should_Retry_Failed_Messages_Periodically()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        var serializer = new JsonSerializer();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(options =>
        {
            options
                .WithReplayInterval(TimeSpan.FromMilliseconds(100))
                .AddConsumer<ReplayTestHandler>(Semantics.AtLeastOnce);
        });

        ReplayTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        // Start Engine first
        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Start replay service
        var replayService = provider.GetServices<IHostedService>().OfType<MessageReplayService>().First();
        await replayService.StartAsync(CancellationToken.None);

        // Simulate a message that was persisted and marked as Failed (e.g. after a timeout)
        var payload = serializer.Serialize(new ReplayTestMessage { Data = "RetryMe" });
        var consumerId = Registry.GetConsumerId(typeof(ReplayTestHandler));
        var failed = new PersistedMessage
        {
            MessageId = Guid.NewGuid().ToString(),
            ConsumerId = consumerId,
            Payload = payload,
            Headers = new Dictionary<string, string>(),
            MessageType = nameof(ReplayTestMessage),
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        };
        await store.PersistAsync(new[] { failed });
        await store.MarkAsFailedAsync(failed.MessageId, consumerId, "simulated failure");

        // Wait for replay cycle to pick it up (interval = 100ms)
        await Task.Delay(500);

        // Assert — message was replayed and processed by the consumer
        Assert.That(ReplayTestHandler.HandledCount, Is.GreaterThanOrEqualTo(1));
        Assert.That(ReplayTestHandler.LastData, Is.EqualTo("RetryMe"));

        await engine.StopAsync(CancellationToken.None);
        await replayService.StopAsync(CancellationToken.None);
    }

    public class ReplayTestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    public class ReplayTestHandler : IConsume<ReplayTestMessage>
    {
        public static int HandledCount;
        public static string? LastData;
        public static void Reset() { HandledCount = 0; LastData = null; }

        public Task HandleAsync(ReplayTestMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            LastData = message.Data;
            return Task.CompletedTask;
        }
    }
}
