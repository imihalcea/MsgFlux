using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

public class PublisherTests
{
    [Test]
    public async Task PublishAsync_Should_Inject_TraceContext_And_Enqueue_For_Consumer()
    {
        // Arrange
        var options = new MsgFluxOptions();
        var registry = new Registry();
        registry.Register<TestEvent, TestConsumer>(Semantics.AtMostOnce);
        var inMemory = new InMemoryMessageSource(options);
        var serializer = new JsonSerializer();
        var logger = NullLogger<Publisher>.Instance;
        var buffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance);
        var publisher = new Publisher(inMemory, registry, serializer, options, logger, buffer);

        using var activityListener = new ActivityListener();
        activityListener.ShouldListenTo = s => s.Name == "MsgFlux";
        activityListener.Sample = (ref _) => ActivitySamplingResult.AllData;
        ActivitySource.AddActivityListener(activityListener);

        // Act
        await publisher.PublishAsync(new TestEvent { Content = "Hello World" });

        // Assert — read the first DispatchItem off the source stream
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await using var enumerator = inMemory.StreamAsync(cts.Token).GetAsyncEnumerator(cts.Token);
        Assert.That(await enumerator.MoveNextAsync(), Is.True);

        var msg = enumerator.Current.Message;
        Assert.That(msg.MessageType, Is.EqualTo(nameof(TestEvent)));
        Assert.That(msg.Headers.ContainsKey("traceparent"), Is.True);
        Assert.That(msg.Headers["traceparent"], Is.Not.Empty);

        var deserialized = serializer.Deserialize<TestEvent>(msg.Payload);
        Assert.That(deserialized, Is.Not.Null);
        Assert.That(deserialized!.Content, Is.EqualTo("Hello World"));
    }

    [Test]
    public void PublishAsync_Should_Throw_When_Payload_Exceeds_Configured_Limit()
    {
        var options = new MsgFluxOptions().WithMaxPayloadSizeKb(1);
        var registry = new Registry();
        registry.Register<TestEvent, TestConsumer>(Semantics.AtMostOnce);
        var inMemory = new InMemoryMessageSource(options);
        var serializer = new JsonSerializer();
        var logger = NullLogger<Publisher>.Instance;
        var durableBuffer = new DurableBuffer(options, NullLogger<DurableBuffer>.Instance);
        var publisher = new Publisher(inMemory, registry, serializer, options, logger, durableBuffer);

        var bytes = new byte[2048];
        new Random().NextBytes(bytes);
        var largeContent = Convert.ToBase64String(bytes);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => publisher.PublishAsync(new TestEvent { Content = largeContent }));
        Assert.That(ex!.Message, Does.Contain("exceeds the configured limit of 1KB"));
    }

    public class TestEvent
    {
        public string Content { get; set; } = string.Empty;
    }

    public class TestConsumer : IConsume<TestEvent>
    {
        public Task HandleAsync(TestEvent @event, CancellationToken ct) => Task.CompletedTask;
    }
}
