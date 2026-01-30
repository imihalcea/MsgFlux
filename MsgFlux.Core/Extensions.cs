using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public static class Extensions
{
    public static IServiceCollection AddMsgFlux(this IServiceCollection services, Action<MsgFluxOptions>? configureOptions = null, params Assembly[] assemblies)
    {
        var options = new MsgFluxOptions();
        configureOptions?.Invoke(options);
        services.AddSingleton(options);

        services.AddSingleton<IChannelRxTx, InMemoryRxTx>();
        
        // Register the serializer based on options
        services.AddSingleton(typeof(ISerializer), options.SerializerType);

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
                var consumersInterfaces = type
                    .GetInterfaces()
                    .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == consumerInterface);
                
                foreach (var i in consumersInterfaces)
                {
                        // Register the consumer
                        services.AddScoped(i, type);
                        var messageType = i.GetGenericArguments()[0];
                        registry.Register(messageType);
                }
            }
        }

        return services;
    }

    // Overload to keep backward compatibility with existing calls that just pass assemblies
    public static IServiceCollection AddMsgFlux(this IServiceCollection services, params Assembly[] assemblies)
    {
        return AddMsgFlux(services, null, assemblies);
    }
}
