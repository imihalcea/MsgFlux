using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public static class Extensions
{
    /// <summary>
    /// Registers MsgFlux into the service collection. Validates synchronously that every
    /// AtLeastOnce consumer has a matching IMessageStore provider registered — so any
    /// misconfiguration throws at this call site rather than at host startup.
    ///
    /// Convention: register the durability provider (e.g. AddMsgFluxPostgres) BEFORE AddMsgFlux.
    /// </summary>
    public static IServiceCollection AddMsgFlux(this IServiceCollection services, Action<MsgFluxOptions>? configureOptions = null)
    {
        var options = new MsgFluxOptions();
        configureOptions?.Invoke(options);
        services.AddSingleton(options);
        services.AddSingleton<ISerializer, JsonSerializer>();

        var registry = new Registry();
        foreach (var registration in options.ConsumerRegistrations)
            registration(services, registry);
        services.AddSingleton(registry);

        ValidateDurabilityProvider(services, registry);

        services.AddSingleton<InMemoryMessageSource>();
        services.AddSingleton<IMessageSource>(sp => sp.GetRequiredService<InMemoryMessageSource>());

        services.AddSingleton<DurableBuffer>();
        // Same instance as a hosted service so its final flush runs during the host's ordered
        // shutdown, while the store is still alive (rather than relying on container disposal order).
        services.AddHostedService(sp => sp.GetRequiredService<DurableBuffer>());
        services.AddSingleton<IPublish, Publisher>();
        services.AddSingleton<IMessageSource>(sp =>
        {
            var store = sp.GetService<IMessageStore>();
            if (store is null) return new NullMessageSource();
            return new PollingStoreSource(store, sp.GetRequiredService<MsgFluxOptions>(),
                sp.GetRequiredService<ILogger<PollingStoreSource>>());
        });

        services.AddHostedService<MessagePurgeService>();
        services.AddHostedService<EngineService>();

        return services;
    }

    private static void ValidateDurabilityProvider(IServiceCollection services, Registry registry)
    {
        if (!registry.HasAtLeastOnceConsumers()) return;

        var hasStore = services.Any(d => d.ServiceType == typeof(IMessageStore));
        if (hasStore) return;

        var offenders = registry.GetMessageTypes()
            .SelectMany(t => registry.GetConsumers(t)
                .Where(c => c.Semantics == Semantics.AtLeastOnce)
                .Select(c => c.ConsumerType.Name));

        throw new InvalidOperationException(
            $"Consumer(s) declared with Semantics.AtLeastOnce ({string.Join(", ", offenders)}) " +
            "but no IMessageStore provider is registered. Call services.AddMsgFluxPostgres(\"...\") " +
            "(or another durability provider) BEFORE services.AddMsgFlux(...).");
    }
}

/// <summary>
/// Placeholder IMessageSource used when no durable store is registered; yields nothing.
/// Waits until cancelled to avoid tight-looping in the engine's consume loop.
/// </summary>
internal sealed class NullMessageSource : IMessageSource
{
    public async IAsyncEnumerable<DispatchItem> StreamAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        try { await Task.Delay(Timeout.Infinite, ct); }
        catch (OperationCanceledException) { }
        yield break;
    }

    public void Complete() { }
}
