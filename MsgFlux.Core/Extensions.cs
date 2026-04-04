using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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

        if (options.DurabilityEnabled)
        {
            services.AddSingleton<IPublish, DurablePublisher>();
            services.AddHostedService<MessageReplayService>();
        }
        else
        {
            services.AddSingleton<IPublish, Publisher>();
        }

        services.AddHostedService<Engine>();

        var registry = new Registry();
        services.AddSingleton(registry);

        // Execute explicit consumer registrations
        foreach (var registration in options.ConsumerRegistrations)
        {
            registration(services, registry);
        }

        return services;
    }
}
