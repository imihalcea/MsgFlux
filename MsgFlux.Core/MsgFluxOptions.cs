using Microsoft.Extensions.DependencyInjection;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public class MsgFluxOptions
{
    public int MaxPayloadSizeKb { get; set; } = 64;
    public int ChannelCapacity { get; set; } = 1000;
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    public bool DurabilityEnabled { get; internal set; }
    public TimeSpan StaleProcessingTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxDeadLetterRetries { get; set; } = 3;
    public TimeSpan PurgeOlderThan { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan PurgeInterval { get; set; } = TimeSpan.FromHours(1);
    public TimeSpan ReplayInterval { get; set; } = TimeSpan.FromSeconds(30);

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

    public MsgFluxOptions WithDurability()
    {
        DurabilityEnabled = true;
        return this;
    }

    public MsgFluxOptions WithStaleProcessingTimeout(TimeSpan timeout)
    {
        StaleProcessingTimeout = timeout;
        return this;
    }

    public MsgFluxOptions WithPurge(TimeSpan olderThan, TimeSpan interval)
    {
        PurgeOlderThan = olderThan;
        PurgeInterval = interval;
        return this;
    }

    public MsgFluxOptions WithReplayInterval(TimeSpan interval)
    {
        ReplayInterval = interval;
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
                RegisterMethod.MakeGenericMethod(messageType).Invoke(null, [registry]);
            }
        });
        return this;
    }

    private static readonly System.Reflection.MethodInfo RegisterMethod =
        typeof(MsgFluxOptions).GetMethod(nameof(RegisterTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

    private static void RegisterTyped<TMessage>(Registry registry) => registry.Register<TMessage>();
}
