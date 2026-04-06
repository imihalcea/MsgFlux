using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class DispatchTests
{
    [Test]
    public async Task Should_Dispatch_Message_To_Multiple_Consumers()
    {
        // Arrange
        var services = new ServiceCollection();
        
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options.WithRetry(1, TimeSpan.FromMilliseconds(10));
            options.AddConsumer<UserCreatedHandler1>();
            options.AddConsumer<UserCreatedHandler2>();
        });

        UserCreatedHandler1.Reset();
        UserCreatedHandler2.Reset();

        var provider = services.BuildServiceProvider();

        var hostedService = provider.GetRequiredService<IHostedService>();
        await hostedService.StartAsync(CancellationToken.None);

        // Act
        var publisher = provider.GetRequiredService<IPublish>();
        await publisher.PublishAsync(new UserCreated { Name = "Ionut" });

        await Task.Delay(200);
        
        Assert.That(UserCreatedHandler1.HandledCount, Is.EqualTo(1));
        Assert.That(UserCreatedHandler2.HandledCount, Is.EqualTo(1));
        Assert.That(UserCreatedHandler1.LastUser, Is.EqualTo("Ionut"));
        Assert.That(UserCreatedHandler2.LastUser, Is.EqualTo("Ionut"));

        await hostedService.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Engine_Should_Survive_Poison_Message()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFlux(options =>
        {
            options.WithRetry(1, TimeSpan.FromMilliseconds(10));
            options.AddConsumer<UserCreatedHandler1>();
        });

        UserCreatedHandler1.Reset();
        var provider = services.BuildServiceProvider();

        var hostedService = provider.GetRequiredService<IHostedService>();
        await hostedService.StartAsync(CancellationToken.None);

        var inMemory = provider.GetRequiredService<InMemoryMessageSource>();
        var publisher = provider.GetRequiredService<IPublish>();
        var consumerId = Registry.GetConsumerId(typeof(UserCreatedHandler1));

        // Act 1: Inject Poison Message (Corrupted Bytes) directly into the in-memory source
        await inMemory.PersistAsync(new[]
        {
            new Message
            {
                MessageId = Guid.NewGuid(),
                ConsumerId = consumerId,
                Payload = [0xDE, 0xAD, 0xBE, 0xEF], // Invalid Brotli/JSON
                Headers = new Dictionary<string, string>(),
                MessageType = typeof(UserCreated).FullName!,
                CreatedAt = DateTimeOffset.UtcNow
            }
        });

        // Act 2: Send Valid Message
        await publisher.PublishAsync(new UserCreated { Name = "Survivor" });

        await Task.Delay(200);
        Assert.That(UserCreatedHandler1.HandledCount, Is.EqualTo(1));
        Assert.That(UserCreatedHandler1.LastUser, Is.EqualTo("Survivor"));

        await hostedService.StopAsync(CancellationToken.None);
    }

    public class UserCreated
    {
        public string Name { get; set; } = string.Empty;
    }

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
