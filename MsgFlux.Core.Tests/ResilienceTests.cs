using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;

namespace MsgFlux.Core.Tests;

public class ResilienceTests
{
    [Test]
    public async Task Engine_Should_Retry_Consumer_On_Failure()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options.AddConsumer<RetryConsumer>();
        });
        
        RetryConsumer.Reset();

        var provider = services.BuildServiceProvider();
        var hostedService = provider.GetRequiredService<IHostedService>();
        var publisher = provider.GetRequiredService<IPublish>();

        await hostedService.StartAsync(CancellationToken.None);

        // Act
        await publisher.PublishAsync(new RetryMessage { Content = "Retry Me" });
        
        // Wait enough time for retries (3 retries * ~200ms backoff + processing time)
        await Task.Delay(2000);

        // Assert
        // Should be called 1 (initial) + 3 (retries) = 4 times
        Assert.That(RetryConsumer.CallCount, Is.EqualTo(4));

        await hostedService.StopAsync(CancellationToken.None);
    }

    public class RetryMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    public class RetryConsumer : IConsume<RetryMessage>
    {
        public static int CallCount = 0;
        public static void Reset() => CallCount = 0;

        public Task HandleAsync(RetryMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            throw new InvalidOperationException("Fail to trigger retry");
        }
    }
}
