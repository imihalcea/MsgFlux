using Microsoft.Extensions.DependencyInjection;
using MsgFlux.Abstractions;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public static class Extensions
{
    public static IServiceCollection AddMsgFlux(this IServiceCollection services, Action<MsgFluxOptions>? configureOptions = null)
    {
        var options = new MsgFluxOptions();
        configureOptions?.Invoke(options);
        services.AddSingleton(options);

        services.AddSingleton<IChannelRxTx, InMemoryRxTx>();
        services.AddSingleton<ISerializer, JsonSerializer>();

        var registry = new Registry();
        services.AddSingleton(registry);

        foreach (var registration in options.ConsumerRegistrations)
        {
            registration(services, registry);
        }

        var durabilityRequired = registry.HasAtLeastOnceConsumers();

        if (durabilityRequired)
        {
            services.AddSingleton<IPublish, DurablePublisher>();
            services.AddHostedService<MessageReplayService>();
            services.AddHostedService<MessagePurgeService>();
        }
        else
        {
            services.AddSingleton<IPublish, Publisher>();
        }

        services.AddHostedService(sp => new DurabilityValidator(registry, sp, durabilityRequired));
        services.AddHostedService<EngineService>();

        return services;
    }
}
