using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

/// <summary>
/// Validates at startup that if any consumer is AtLeastOnce, a non-NoOp IMessageStore is registered.
/// </summary>
internal sealed class DurabilityValidator(Registry registry, IServiceProvider services, bool durabilityRequired) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!durabilityRequired) return Task.CompletedTask;

        var store = services.GetService<IMessageStore>();
        if (store is null || store is NoOpMessageStore)
        {
            var offenders = registry.GetMessageTypes()
                .SelectMany(t => registry.GetConsumers(t)
                    .Where(c => c.Semantics == Semantics.AtLeastOnce)
                    .Select(c => c.ConsumerType.Name));

            throw new InvalidOperationException(
                $"One or more consumers are declared with Semantics.AtLeastOnce ({string.Join(", ", offenders)}) " +
                "but no IMessageStore provider is registered. " +
                "Add a durability provider (e.g., services.AddMsgFluxPostgres(\"...\")) before calling AddMsgFlux or in the same composition root.");
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
