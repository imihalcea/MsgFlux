using System.Diagnostics;
using Microsoft.Extensions.Logging;
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
        var publisher = new Publisher(inMemory, registry, serializer, options, logger);

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
    public async Task PublishAsync_Should_Log_Warning_When_Payload_Exceeds_Configured_Limit()
    {
        var options = new MsgFluxOptions().WithMaxPayloadSizeKb(1);
        var registry = new Registry();
        registry.Register<TestEvent, TestConsumer>(Semantics.AtMostOnce);
        var inMemory = new InMemoryMessageSource(options);
        var serializer = new JsonSerializer();
        var logger = new TestLogger<Publisher>();
        var publisher = new Publisher(inMemory, registry, serializer, options, logger);

        var buffer = new byte[2048];
        new Random().NextBytes(buffer);
        var largeContent = Convert.ToBase64String(buffer);

        await publisher.PublishAsync(new TestEvent { Content = largeContent });

        var warning = logger.LogEntries.FirstOrDefault(e => e.LogLevel == LogLevel.Warning);
        Assert.That(warning, Is.Not.Null);
        Assert.That(warning!.Message, Does.Contain("exceeds 1KB"));
    }

    public class TestEvent
    {
        public string Content { get; set; } = string.Empty;
    }

    public class TestConsumer : IConsume<TestEvent>
    {
        public Task HandleAsync(TestEvent @event, CancellationToken ct) => Task.CompletedTask;
    }

    private class TestLogger<T> : ILogger<T>
    {
        public List<LogEntry> LogEntries { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => LogEntries.Add(new LogEntry(logLevel, formatter(state, exception)));
        public record LogEntry(LogLevel LogLevel, string Message);
    }
}
