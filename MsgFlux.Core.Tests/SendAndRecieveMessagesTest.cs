using MsgFlux.Abstractions;
using NUnit.Framework;

namespace MsgFlux.Core.Tests;

public class SendAndRecieveMessagesTest
{
    private static Message CreateMessage(string id = "m1") => new()
    {
        MessageId = id,
        ConsumerId = "consumer-a",
        Payload = [0x01, 0x02, 0x03],
        Headers = new Dictionary<string, string> { ["key"] = "value" },
        MessageType = "Test",
        CreatedAt = DateTimeOffset.UtcNow
    };

    [Test]
    public async Task Should_Preserve_Message_Integrity_Through_InMemorySource()
    {
        var source = new InMemoryMessageSource(new MsgFluxOptions());
        var original = CreateMessage();

        await source.PersistAsync(new[] { original });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await using var e = source.StreamAsync(cts.Token).GetAsyncEnumerator(cts.Token);
        Assert.That(await e.MoveNextAsync(), Is.True);

        var received = e.Current.Message;
        Assert.That(received.MessageId, Is.EqualTo(original.MessageId));
        Assert.That(received.ConsumerId, Is.EqualTo(original.ConsumerId));
        Assert.That(received.Payload, Is.EqualTo(original.Payload));
        Assert.That(received.Headers["key"], Is.EqualTo("value"));
    }

    [Test]
    public async Task Should_Apply_Backpressure_When_Source_Is_Full()
    {
        var options = new MsgFluxOptions().WithChannelCapacity(1);
        var source = new InMemoryMessageSource(options);

        await source.PersistAsync(new[] { CreateMessage("1") }); // Fills the buffer

        var blockedTask = source.PersistAsync(new[] { CreateMessage("2") });
        await Task.WhenAny(blockedTask, Task.Delay(50));
        Assert.That(blockedTask.IsCompleted, Is.False, "Producer should be blocked when source is full");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await using var e = source.StreamAsync(cts.Token).GetAsyncEnumerator(cts.Token);
        await e.MoveNextAsync(); // drain one

        var completed = await Task.WhenAny(blockedTask, Task.Delay(1000));
        Assert.That(completed, Is.EqualTo(blockedTask), "Producer should resume when space becomes available");
    }
}
