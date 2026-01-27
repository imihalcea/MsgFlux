using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public static class Extensions
{
    public static IServiceCollection AddMsgFlux(this IServiceCollection services, params Assembly[] assemblies)
    {
        services.AddSingleton<IChannelRxTx, InMemoryRxTx>();
        services.AddSingleton<ISerializer, JsonSerializer>();
        services.AddSingleton<IPublish, Publisher>();
        services.AddHostedService<Engine>();
        
        var registry = new Registry();
        services.AddSingleton(registry);

        if (assemblies.Length == 0)
        {
            assemblies = [Assembly.GetCallingAssembly()];
        }

        var consumerInterface = typeof(IConsume<>);

        foreach (var assembly in assemblies)
        {
            var types = assembly.GetTypes()
                .Where(t => t is { IsClass: true, IsAbstract: false });

            foreach (var type in types)
            {
                foreach (var i in type.GetInterfaces())
                {
                    if (i.IsGenericType && i.GetGenericTypeDefinition() == consumerInterface)
                    {
                        // Register the consumer
                        services.AddScoped(i, type);
                        
                        var messageType = i.GetGenericArguments()[0];
                        registry.Register(messageType);
                    }
                }
            }
        }

        return services;
    }
}
