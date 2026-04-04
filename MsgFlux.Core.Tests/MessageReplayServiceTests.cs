using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core.RxTx;
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
        var persisted = new PersistedMessage
        {
            MessageId = Guid.NewGuid().ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(),
            MessageType = nameof(ReplayTestMessage),
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        };
        await store.PersistAsync(persisted);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options.WithDurability();
            options.AddConsumer<ReplayTestHandler>();
        });
        services.AddSingleton<IMessageStore>(store);

        ReplayTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        // Start replay service first (it runs before Engine per spec)
        var replayService = provider.GetServices<IHostedService>().OfType<MessageReplayService>().First();
        await replayService.StartAsync(CancellationToken.None);

        // Then start Engine
        var engine = provider.GetServices<IHostedService>().OfType<Engine>().First();
        await engine.StartAsync(CancellationToken.None);

        await Task.Delay(500);

        // Assert
        Assert.That(ReplayTestHandler.HandledCount, Is.EqualTo(1));
        Assert.That(ReplayTestHandler.LastData, Is.EqualTo("Replayed"));

        await engine.StopAsync(CancellationToken.None);
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
