using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class DurableBufferTests
{
    [Test]
    public async Task Should_Flush_When_Threshold_Reached()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 3);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        await buffer.AddAsync(MakeMessages(3));

        Assert.That(store.Messages, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Should_Flush_On_Timer_Before_Threshold()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromMilliseconds(50), flushThreshold: 100);
        await using var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        await buffer.AddAsync(MakeMessages(2));

        // Not yet flushed (threshold=100)
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        // Wait for timer flush
        await Task.Delay(200);

        Assert.That(store.Messages, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Should_Flush_Remaining_On_Dispose()
    {
        var store = new InMemoryMessageStore();
        var options = new MsgFluxOptions().WithBufferedPublishing(
            flushInterval: TimeSpan.FromSeconds(30), flushThreshold: 100);
        var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance, store);

        await buffer.AddAsync(MakeMessages(5));
        Assert.That(store.Messages, Has.Count.EqualTo(0));

        await buffer.DisposeAsync();
        Assert.That(store.Messages, Has.Count.EqualTo(5));
    }

    private static List<Message> MakeMessages(int count) =>
        Enumerable.Range(0, count).Select(i => new Message
        {
            MessageId = Guid.NewGuid().ToString(),
            ConsumerId = "test-consumer",
            Payload = [0x01],
            Headers = new Dictionary<string, string>(),
            MessageType = "Test",
            CreatedAt = DateTimeOffset.UtcNow
        }).ToList();
}
