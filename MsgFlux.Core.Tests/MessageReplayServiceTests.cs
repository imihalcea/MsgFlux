using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

/// <summary>
/// Verifies the durable replay path: the PollingStoreSource picks up pre-persisted
/// messages and the Engine dispatches them to the owning consumer.
/// </summary>
public class MessageReplayServiceTests
{
    [Test]
    public async Task Durable_Source_Should_Stream_Pre_Persisted_Messages_To_Consumer()
    {
        var store = new InMemoryMessageStore();
        var serializer = new JsonSerializer();
        var consumerId = Registry.GetConsumerId(typeof(ReplayTestHandler));

        var payload = serializer.Serialize(new ReplayTestMessage { Data = "Replayed" });
        await store.PersistAsync(new[] { new Message
        {
            MessageId = Guid.NewGuid().ToString(),
            ConsumerId = consumerId,
            Payload = payload,
            Headers = new Dictionary<string, string>(),
            MessageType = nameof(ReplayTestMessage),
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }});

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(opt => opt
            .WithReplayInterval(TimeSpan.FromMilliseconds(50))
            .AddConsumer<ReplayTestHandler>(Semantics.AtLeastOnce));

        ReplayTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StartAsync(CancellationToken.None);

        await Task.Delay(500);

        Assert.That(ReplayTestHandler.HandledCount, Is.GreaterThanOrEqualTo(1));
        Assert.That(ReplayTestHandler.LastData, Is.EqualTo("Replayed"));

        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Durable_Source_Should_Retry_Failed_Messages_Until_Success()
    {
        var store = new InMemoryMessageStore();
        var serializer = new JsonSerializer();
        var consumerId = Registry.GetConsumerId(typeof(ReplayTestHandler));

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(store);
        services.AddMsgFlux(opt => opt
            .WithReplayInterval(TimeSpan.FromMilliseconds(50))
            .AddConsumer<ReplayTestHandler>(Semantics.AtLeastOnce));

        ReplayTestHandler.Reset();
        var provider = services.BuildServiceProvider();

        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StartAsync(CancellationToken.None);

        var payload = serializer.Serialize(new ReplayTestMessage { Data = "RetryMe" });
        var messageId = Guid.NewGuid().ToString();
        await store.PersistAsync(new[] { new Message
        {
            MessageId = messageId,
            ConsumerId = consumerId,
            Payload = payload,
            Headers = new Dictionary<string, string>(),
            MessageType = nameof(ReplayTestMessage),
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        }});
        await store.MarkAsFailedAsync(messageId, consumerId, "simulated failure");

        await Task.Delay(500);

        Assert.That(ReplayTestHandler.HandledCount, Is.GreaterThanOrEqualTo(1));
        Assert.That(ReplayTestHandler.LastData, Is.EqualTo("RetryMe"));

        foreach (var hs in provider.GetServices<IHostedService>())
            await hs.StopAsync(CancellationToken.None);
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
