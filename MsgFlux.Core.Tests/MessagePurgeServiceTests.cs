using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class MessagePurgeServiceTests
{
    [Test]
    public async Task PurgeService_Should_Purge_Completed_Messages_Periodically()
    {
        // Arrange
        var store = new InMemoryMessageStore();

        // Persist and complete an old message
        var oldMsg = new PersistedMessage
        {
            MessageId = Guid.NewGuid().ToString(),
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow.AddDays(-10)
        };
        await store.PersistAsync(oldMsg);
        await store.AcknowledgeAsync(oldMsg.MessageId);

        // Persist and complete a recent message (should NOT be purged)
        var recentMsg = new PersistedMessage
        {
            MessageId = Guid.NewGuid().ToString(),
            Payload = [0x02],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            State = MessageState.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        };
        await store.PersistAsync(recentMsg);
        await store.AcknowledgeAsync(recentMsg.MessageId);

        var options = new MsgFluxOptions();
        options.WithDurability();
        options.WithPurge(olderThan: TimeSpan.FromDays(7), interval: TimeSpan.FromMilliseconds(50));

        using var loggerFactory = LoggerFactory.Create(_ => { });
        var service = new MessagePurgeService(store, options, loggerFactory.CreateLogger<MessagePurgeService>());

        // Act
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        try { await service.StartAsync(cts.Token); await Task.Delay(200); }
        catch (OperationCanceledException) { }
        await service.StopAsync(CancellationToken.None);

        // Assert — old message purged, recent message kept
        Assert.That(store.Messages, Has.Count.EqualTo(1));
        Assert.That(store.Messages.ContainsKey(recentMsg.MessageId), Is.True);
    }
}
