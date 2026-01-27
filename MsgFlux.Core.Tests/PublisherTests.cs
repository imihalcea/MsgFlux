using System.Diagnostics;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

public class PublisherTests
{
    [Test]
    public async Task PublishAsync_Should_Inject_TraceContext_And_Write_To_Channel()
    {
        // Arrange
        var rxTx = new InMemoryRxTx();
        var serializer = new JsonSerializer();
        var publisher = new FluxPublisher(rxTx, serializer);
        var message = new TestMessage("Hello World");

        // Setup ActivityListener to verify OpenTelemetry behavior
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Flux",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(activityListener);

        // Act
        await publisher.PublishAsync(message);

        // Assert
        var reader = rxTx.GetReader(typeof(TestMessage));
        var envelope = await reader.ReadAsync();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(envelope.MessageType, Is.EqualTo(nameof(TestMessage)));
            Assert.That(envelope.Headers.ContainsKey("traceparent"), Is.True, "Traceparent header should be present");
            Assert.That(envelope.Headers["traceparent"], Is.Not.Empty, "Traceparent should not be empty");
        }

        var deserialized = serializer.Deserialize<TestMessage>(envelope.Payload);
        Assert.That(deserialized, Is.EqualTo(message));
    }

    private record TestMessage(string Content);
}
