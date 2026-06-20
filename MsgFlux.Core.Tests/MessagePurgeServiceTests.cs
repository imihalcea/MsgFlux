using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;

namespace MsgFlux.Core.Tests;

public class MessagePurgeServiceTests
{
    [Test]
    public async Task PurgeService_Should_Purge_Completed_Messages_Periodically()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        const string consumerId = "consumer-a";

        // Persist and complete an old message
        var oldMsg = new Message
        {
            MessageId = Guid.NewGuid(),
            ConsumerId = consumerId,
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow.AddDays(-10)
        };
        await store.PersistAsync(new[] { oldMsg });
        await store.AcknowledgeAsync(oldMsg.MessageId, consumerId);

        // Persist and complete a recent message (should NOT be purged)
        var recentMsg = new Message
        {
            MessageId = Guid.NewGuid(),
            ConsumerId = consumerId,
            Payload = [0x02],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        };
        await store.PersistAsync(new[] { recentMsg });
        await store.AcknowledgeAsync(recentMsg.MessageId, consumerId);

        var options = new MsgFluxOptions();
        options.WithPurge(olderThan: TimeSpan.FromDays(7), interval: TimeSpan.FromMilliseconds(50));

        using var loggerFactory = LoggerFactory.Create(_ => { });
        var service = new MessagePurgeService(options, loggerFactory.CreateLogger<MessagePurgeService>(), store);

        // Act
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        try { await service.StartAsync(cts.Token); await Task.Delay(200); }
        catch (OperationCanceledException) { }
        await service.StopAsync(CancellationToken.None);

        // Assert — old message purged, recent message kept
        Assert.That(store.Messages, Has.Count.EqualTo(1));
        Assert.That(store.Messages.ContainsKey((recentMsg.MessageId, consumerId)), Is.True);
    }
}
