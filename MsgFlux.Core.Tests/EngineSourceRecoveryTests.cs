using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core.Tests;

public class EngineSourceRecoveryTests
{
    [Test]
    public async Task Engine_Should_Recover_After_Source_Crash()
    {
        // Arrange — a source that throws on the first StreamAsync call, then works
        var crashingSource = new CrashOnceSource();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options
                .WithRetry(1, TimeSpan.FromMilliseconds(10))
                .WithReplayInterval(TimeSpan.FromMilliseconds(50))
                .AddConsumer<RecoveryHandler>();
        });

        // Replace the InMemoryMessageSource with our crashing source
        services.AddSingleton<IMessageSource>(crashingSource);

        RecoveryHandler.Reset();
        var provider = services.BuildServiceProvider();

        var engine = provider.GetServices<IHostedService>().OfType<EngineService>().First();
        await engine.StartAsync(CancellationToken.None);

        // Act — publish after giving the source time to crash and restart
        await Task.Delay(200);

        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new RecoveryMessage { Value = "after-crash" });

        // Push the message into the crashing source so it can be consumed
        var serializer = provider.GetRequiredService<ISerializer>();
        var consumerId = Registry.GetConsumerId(typeof(RecoveryHandler));
        crashingSource.Inject(new Message
        {
            MessageId = Guid.NewGuid(),
            ConsumerId = consumerId,
            Payload = serializer.Serialize(new RecoveryMessage { Value = "after-crash" }),
            Headers = new Dictionary<string, string>(),
            MessageType = typeof(RecoveryMessage).FullName!,
            CreatedAt = DateTimeOffset.UtcNow
        });

        await Task.Delay(300);
        await engine.StopAsync(CancellationToken.None);

        // Assert — the source recovered and dispatched the message
        Assert.That(crashingSource.StreamCallCount, Is.GreaterThanOrEqualTo(2));
        Assert.That(RecoveryHandler.HandledCount, Is.GreaterThanOrEqualTo(1));
    }

    public class RecoveryMessage
    {
        public string Value { get; set; } = string.Empty;
    }

    public class RecoveryHandler : IConsume<RecoveryMessage>
    {
        public static int HandledCount;
        public static void Reset() => HandledCount = 0;

        public Task HandleAsync(RecoveryMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// IMessageSource that throws on the first StreamAsync call, then yields injected messages.
    /// </summary>
    private class CrashOnceSource : IMessageSource
    {
        public int StreamCallCount;
        private readonly List<Message> _pending = [];
        private readonly Lock _lock = new();

        public void Complete() { }

        public void Inject(Message msg)
        {
            lock (_lock) _pending.Add(msg);
        }

        public async IAsyncEnumerable<DispatchItem> StreamAsync(
            [EnumeratorCancellation] CancellationToken ct)
        {
            var call = Interlocked.Increment(ref StreamCallCount);
            if (call == 1)
                throw new InvalidOperationException("Source crash on first call");

            while (!ct.IsCancellationRequested)
            {
                List<Message> snapshot;
                lock (_lock)
                {
                    snapshot = [.. _pending];
                    _pending.Clear();
                }

                foreach (var msg in snapshot)
                    yield return new DispatchItem(msg);

                try { await Task.Delay(50, ct); }
                catch (OperationCanceledException) { yield break; }
            }
        }
    }
}
