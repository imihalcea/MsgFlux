using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Abstractions;
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
        var logger = NullLogger<Publisher>.Instance;
        var publisher = new Publisher(rxTx, serializer, logger);
        var message = new TestMessage { Content = "Hello World" };

        // Setup ActivityListener to verify OpenTelemetry behavior
        using var activityListener = new ActivityListener();
        activityListener.ShouldListenTo = s => s.Name == "MsgFlux";
        activityListener.Sample = (ref _) => ActivitySamplingResult.AllData;
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
        Assert.That(deserialized, Is.Not.Null);
        Assert.That(deserialized.Content, Is.EqualTo(message.Content));
    }

    [Test]
    public async Task PublishAsync_Should_Log_Warning_When_Payload_Exceeds_Configured_Limit()
    {
        // Arrange
        var options = new MsgFluxOptions().WithMaxPayloadSizeKb(1); // 1KB limit
        var rxTx = new InMemoryRxTx(options);
        var serializer = new JsonSerializer();
        var logger = new TestLogger<Publisher>();
        var publisher = new Publisher(rxTx, serializer, logger, options);
        
        // Create a message that will serialize to > 1KB even with compression.
        // Random data compresses poorly. 2KB of random data should suffice.
        var random = new Random();
        var buffer = new byte[2048];
        random.NextBytes(buffer);
        var largeContent = Convert.ToBase64String(buffer);

        var message = new TestMessage { Content = largeContent };

        // Act
        await publisher.PublishAsync(message);

        // Assert
        Assert.That(logger.LogEntries.Count, Is.GreaterThan(0));
        var warning = logger.LogEntries.FirstOrDefault(e => e.LogLevel == LogLevel.Warning);
        Assert.That(warning, Is.Not.Null, "Should have logged a warning");
        Assert.That(warning!.Message, Does.Contain("exceeds 1KB"));
    }

    public class TestMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    private class TestLogger<T> : ILogger<T>
    {
        public List<LogEntry> LogEntries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            LogEntries.Add(new LogEntry(logLevel, formatter(state, exception)));
        }

        public record LogEntry(LogLevel LogLevel, string Message);
    }
}
