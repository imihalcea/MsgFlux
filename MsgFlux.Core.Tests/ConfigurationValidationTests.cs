using Microsoft.Extensions.DependencyInjection;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

public class ConfigurationValidationTests
{
    [Test]
    public void AddMsgFlux_Should_Throw_When_AtLeastOnce_Consumer_Without_Store()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddMsgFlux(opt => opt.AddConsumer<DummyHandler>(Semantics.AtLeastOnce)));

        Assert.That(ex!.Message, Does.Contain("Semantics.AtLeastOnce"));
        Assert.That(ex.Message, Does.Contain(nameof(DummyHandler)));
    }

    [Test]
    public void AddMsgFlux_Should_Succeed_When_Store_Registered_Before()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageStore>(new InMemoryMessageStore());

        Assert.DoesNotThrow(() =>
            services.AddMsgFlux(opt => opt.AddConsumer<DummyHandler>(Semantics.AtLeastOnce)));
    }

    [Test]
    public void AddMsgFlux_Should_Succeed_When_Only_AtMostOnce_Consumers()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        Assert.DoesNotThrow(() =>
            services.AddMsgFlux(opt => opt.AddConsumer<DummyHandler>(Semantics.AtMostOnce)));
    }

    public class DummyMessage { }
    public class DummyHandler : IConsume<DummyMessage>
    {
        public Task HandleAsync(DummyMessage message, CancellationToken ct) => Task.CompletedTask;
    }
}
