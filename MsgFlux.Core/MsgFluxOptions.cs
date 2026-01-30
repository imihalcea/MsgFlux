using Microsoft.Extensions.DependencyInjection;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public class MsgFluxOptions
{
    public int MaxPayloadSizeKb { get; set; } = 64;
    public int ChannelCapacity { get; set; } = 1000;
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    
    internal List<Action<IServiceCollection, Registry>> ConsumerRegistrations { get; } = new();

    public MsgFluxOptions WithMaxPayloadSizeKb(int sizeKb)
    {
        MaxPayloadSizeKb = sizeKb;
        return this;
    }

    public MsgFluxOptions WithChannelCapacity(int capacity)
    {
        ChannelCapacity = capacity;
        return this;
    }

    public MsgFluxOptions WithMaxDegreeOfParallelism(int maxDegreeOfParallelism)
    {
        MaxDegreeOfParallelism = maxDegreeOfParallelism;
        return this;
    }

    public MsgFluxOptions AddConsumer<TConsumer>() where TConsumer : class
    {
        ConsumerRegistrations.Add((services, registry) =>
        {
            var consumerType = typeof(TConsumer);
            var consumerInterface = typeof(IConsume<>);
            
            var interfaces = consumerType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == consumerInterface)
                .ToList();

            if (!interfaces.Any())
            {
                throw new InvalidOperationException($"Type {consumerType.Name} does not implement IConsume<T>.");
            }

            foreach (var i in interfaces)
            {
                services.AddScoped(i, consumerType);
                var messageType = i.GetGenericArguments()[0];
                registry.Register(messageType);
            }
        });
        return this;
    }
}
