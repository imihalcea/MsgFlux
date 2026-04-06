using Microsoft.Extensions.DependencyInjection;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public class MsgFluxOptions
{
    public int MaxPayloadSizeKb { get; set; } = 64;
    public int ChannelCapacity { get; set; } = 1000;
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    public TimeSpan StaleProcessingTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxDeadLetterRetries { get; set; } = 3;
    public TimeSpan PurgeOlderThan { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan PurgeInterval { get; set; } = TimeSpan.FromHours(1);
    public TimeSpan ReplayInterval { get; set; } = TimeSpan.FromSeconds(5);
    public int PollingBatchSize { get; set; } = 500;
    public TimeSpan BufferFlushInterval { get; set; } = TimeSpan.Zero;
    public int BufferFlushThreshold { get; set; } = 1;
    public int MaxRetryAttempts { get; set; } = 3;
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    internal List<Action<IServiceCollection, Registry>> ConsumerRegistrations { get; } = new();

    public MsgFluxOptions WithMaxPayloadSizeKb(int sizeKb) { MaxPayloadSizeKb = sizeKb; return this; }
    public MsgFluxOptions WithChannelCapacity(int capacity) { ChannelCapacity = capacity; return this; }
    public MsgFluxOptions WithMaxDegreeOfParallelism(int dop) { MaxDegreeOfParallelism = dop; return this; }
    public MsgFluxOptions WithStaleProcessingTimeout(TimeSpan t) { StaleProcessingTimeout = t; return this; }
    public MsgFluxOptions WithPurge(TimeSpan olderThan, TimeSpan interval) { PurgeOlderThan = olderThan; PurgeInterval = interval; return this; }
    public MsgFluxOptions WithReplayInterval(TimeSpan interval) { ReplayInterval = interval; return this; }
    public MsgFluxOptions WithMaxDeadLetterRetries(int retries) { MaxDeadLetterRetries = retries; return this; }
    public MsgFluxOptions WithPollingBatchSize(int batchSize) { PollingBatchSize = batchSize; return this; }
    public MsgFluxOptions WithRetry(int maxAttempts, TimeSpan delay) { MaxRetryAttempts = maxAttempts; RetryDelay = delay; return this; }
    public MsgFluxOptions WithBufferedPublishing(TimeSpan flushInterval, int flushThreshold = 50)
    {
        BufferFlushInterval = flushInterval;
        BufferFlushThreshold = flushThreshold;
        return this;
    }

    /// <summary>
    /// Registers a consumer with an explicit delivery semantic. AtMostOnce (default) uses the
    /// in-memory source; AtLeastOnce requires an IMessageStore provider (e.g., AddMsgFluxPostgres).
    /// </summary>
    public MsgFluxOptions AddConsumer<TConsumer>(Semantics semantics = Semantics.AtMostOnce) where TConsumer : class
    {
        ConsumerRegistrations.Add((services, registry) =>
        {
            var consumerType = typeof(TConsumer);
            var consumerInterface = typeof(IConsume<>);

            var interfaces = consumerType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == consumerInterface)
                .ToList();

            if (!interfaces.Any())
                throw new InvalidOperationException($"Type {consumerType.Name} does not implement IConsume<T>.");

            foreach (var i in interfaces)
            {
                services.AddScoped(i, consumerType);
                var messageType = i.GetGenericArguments()[0];
                RegisterMethod.MakeGenericMethod(messageType, consumerType)
                    .Invoke(null, [registry, semantics]);
            }
        });
        return this;
    }

    private static readonly System.Reflection.MethodInfo RegisterMethod =
        typeof(MsgFluxOptions).GetMethod(nameof(RegisterTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

    private static void RegisterTyped<TMessage, TConsumer>(Registry registry, Semantics semantics)
        where TConsumer : class, IConsume<TMessage>
        => registry.Register<TMessage, TConsumer>(semantics);
}
