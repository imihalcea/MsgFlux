using System.Threading.Channels;
using MsgFlux.Core.RxTx;
using NUnit.Framework;

namespace MsgFlux.Core.Tests;

public class SendAndRecieveMessagesTest
{
    [Test]
    public async Task Should_Preserve_Message_Integrity_Through_Channel()
    {
        // Arrange
        var rxTx = new InMemoryRxTx();
        var messageType = typeof(string);
        var originalPayload = new byte[] { 0x01, 0x02, 0x03 };
        var envelope = new Envelope(
            MessageId: Guid.NewGuid().ToString(),
            Payload: originalPayload,
            Headers: new Dictionary<string, string> { { "key", "value" } },
            MessageType: messageType.Name
        );

        var writer = rxTx.GetWriter(messageType);
        var reader = rxTx.GetReader(messageType);

        // Act
        await writer.WriteAsync(envelope);
        var receivedEnvelope = await reader.ReadAsync();

        // Assert
        Assert.That(receivedEnvelope, Is.EqualTo(envelope));
        Assert.That(receivedEnvelope.Payload, Is.EqualTo(originalPayload));
        Assert.That(receivedEnvelope.Headers["key"], Is.EqualTo("value"));
    }

    [Test]
    public async Task Should_Apply_Backpressure_When_Channel_Is_Full()
    {
        // Arrange: Channel with capacity 1
        var options = new MsgFluxOptions().WithChannelCapacity(1);
        var rxTx = new InMemoryRxTx(options);
        var messageType = typeof(int);
        var writer = rxTx.GetWriter(messageType);
        var reader = rxTx.GetReader(messageType);
        
        var envelope1 = new Envelope("1", [], new(), "int");
        var envelope2 = new Envelope("2", [], new(), "int");

        // Act & Assert
        
        await writer.WriteAsync(envelope1); // Buffer full(Capacity = 1)
        var blockedWriteTask = writer.WriteAsync(envelope2).AsTask();
        await Task.WhenAny(blockedWriteTask, Task.Delay(50));
        Assert.That(blockedWriteTask.IsCompleted, Is.False, "Producer should be blocked when channel is full");
        
        await reader.ReadAsync();
        
        // The blocked write task should complete.
        var completedTask = await Task.WhenAny(blockedWriteTask, Task.Delay(1000));
        Assert.That(completedTask, Is.EqualTo(blockedWriteTask), "Producer should resume when space becomes available");
        Assert.That(blockedWriteTask.IsCompleted, Is.True);
    }
}
