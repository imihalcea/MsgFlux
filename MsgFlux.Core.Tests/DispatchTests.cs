using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MsgFlux.Core.Tests;

public class DispatchTests
{
    [Test]
    public async Task Should_Dispatch_Message_To_Multiple_Consumers()
    {
        // Arrange
        var services = new ServiceCollection();
        
        services.AddLogging();
        services.AddMsgFlux(Assembly.GetExecutingAssembly());
        
        // Test State
        UserCreatedHandler1.Reset();
        UserCreatedHandler2.Reset();

        var provider = services.BuildServiceProvider();
        
        // Start Engine
        var hostedService = provider.GetRequiredService<IHostedService>();
        await hostedService.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new UserCreated("Ionut"));

        // Assert - Wait for processing
        await Task.Delay(500); // Simple wait for test
        
        Assert.That(UserCreatedHandler1.HandledCount, Is.EqualTo(1));
        Assert.That(UserCreatedHandler2.HandledCount, Is.EqualTo(1));
        Assert.That(UserCreatedHandler1.LastUser, Is.EqualTo("Ionut"));
        Assert.That(UserCreatedHandler2.LastUser, Is.EqualTo("Ionut"));

        await hostedService.StopAsync(CancellationToken.None);
    }

    public record UserCreated(string Name);

    public class UserCreatedHandler1 : IConsume<UserCreated>
    {
        public static int HandledCount = 0;
        public static string? LastUser;

        public static void Reset() { HandledCount = 0; LastUser = null; }

        public Task HandleAsync(UserCreated message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            LastUser = message.Name;
            return Task.CompletedTask;
        }
    }

    public class UserCreatedHandler2 : IConsume<UserCreated>
    {
        public static int HandledCount = 0;
        public static string? LastUser;

        public static void Reset() { HandledCount = 0; LastUser = null; }

        public Task HandleAsync(UserCreated message, CancellationToken ct)
        {
            Interlocked.Increment(ref HandledCount);
            LastUser = message.Name;
            return Task.CompletedTask;
        }
    }
}
