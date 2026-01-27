using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;

namespace MsgFlux.Core.Tests;

public class ResilienceTests
{
    [Test]
    public async Task Engine_Should_Continue_Processing_When_Consumer_Fails()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging(); // Basic logging registration required for DI
        services.AddMsgFlux(Assembly.GetExecutingAssembly());
        
        // Reset state
        FailingConsumer.Reset();
        SuccessConsumer.Reset();

        var provider = services.BuildServiceProvider();
        var hostedService = provider.GetRequiredService<IHostedService>();
        var publisher = provider.GetRequiredService<IPublish>();

        await hostedService.StartAsync(CancellationToken.None);

        // Act
        await publisher.PublishAsync(new ResilienceMessage("Message 1"));
        
        // Wait processing
        await Task.Delay(200);

        // Assert 1: Failing consumer failed, Success consumer succeeded
        using (Assert.EnterMultipleScope())
        {
            
            Assert.That(FailingConsumer.CallCount, Is.EqualTo(1));
            Assert.That(SuccessConsumer.CallCount, Is.EqualTo(1));
        }

        // Act 2: Publish another message to prove the Engine is still alive
        await publisher.PublishAsync(new ResilienceMessage("Message 2"));
        
        // Wait processing
        await Task.Delay(200);

        using (Assert.EnterMultipleScope())
        {
            // Assert 2: Both consumers were called again (Engine is alive)
            Assert.That(FailingConsumer.CallCount, Is.EqualTo(2));
            Assert.That(SuccessConsumer.CallCount, Is.EqualTo(2));
        }

        await hostedService.StopAsync(CancellationToken.None);
    }

    public record ResilienceMessage(string Content);

    public class FailingConsumer : IConsume<ResilienceMessage>
    {
        public static int CallCount = 0;
        public static void Reset() => CallCount = 0;

        public Task HandleAsync(ResilienceMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            throw new InvalidOperationException("Boom, on purpose!");
        }
    }

    public class SuccessConsumer : IConsume<ResilienceMessage>
    {
        public static int CallCount = 0;
        public static void Reset() => CallCount = 0;

        public Task HandleAsync(ResilienceMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            return Task.CompletedTask;
        }
    }
}
