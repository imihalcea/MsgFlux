using System.Text;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public class Registry
{
    private readonly Dictionary<Type, List<ConsumerRegistration>> _consumersByMessageType = new();
    private readonly Dictionary<Type, Func<object, object, CancellationToken, Task>> _invokers = new();
    private readonly Dictionary<Type, Type> _consumerServiceTypes = new();

    public void Register<TMessage, TConsumer>(Semantics semantics) where TConsumer : class, IConsume<TMessage>
    {
        var messageType = typeof(TMessage);
        var consumerType = typeof(TConsumer);

        if (!_consumersByMessageType.TryGetValue(messageType, out var list))
        {
            list = new List<ConsumerRegistration>();
            _consumersByMessageType[messageType] = list;
            _consumerServiceTypes[messageType] = typeof(IConsume<TMessage>);
            _invokers[messageType] = (consumer, message, ct) =>
                ((IConsume<TMessage>)consumer).HandleAsync((TMessage)message, ct);
        }

        if (list.Any(r => r.ConsumerType == consumerType)) return;

        var consumerId = ComputeStableHash(consumerType.FullName ?? consumerType.Name);
        list.Add(new ConsumerRegistration(consumerType, consumerId, semantics));
    }

    public IEnumerable<Type> GetMessageTypes() => _consumersByMessageType.Keys;

    public Type GetConsumerServiceType(Type messageType) => _consumerServiceTypes[messageType];

    public Func<object, object, CancellationToken, Task> GetInvoker(Type messageType) => _invokers[messageType];

    public IReadOnlyList<ConsumerRegistration> GetConsumers(Type messageType) =>
        _consumersByMessageType.TryGetValue(messageType, out var list)
            ? list
            : Array.Empty<ConsumerRegistration>();

    public bool HasAtLeastOnceConsumers() =>
        _consumersByMessageType.Values.Any(list => list.Any(r => r.Semantics == Semantics.AtLeastOnce));

    /// <summary>
    /// Computes the ConsumerId for a given consumer type (stable FNV-1a hash of its FullName).
    /// </summary>
    public static string GetConsumerId(Type consumerType) =>
        ComputeStableHash(consumerType.FullName ?? consumerType.Name);

    /// <summary>
    /// Stable 64-bit FNV-1a hash, encoded as lowercase hex. Stable across runs and processes.
    /// </summary>
    internal static string ComputeStableHash(string input)
    {
        const ulong fnvOffsetBasis = 14695981039346656037UL;
        const ulong fnvPrime = 1099511628211UL;

        var bytes = Encoding.UTF8.GetBytes(input);
        ulong hash = fnvOffsetBasis;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= fnvPrime;
        }
        return hash.ToString("x16");
    }
}

public sealed record ConsumerRegistration(Type ConsumerType, string ConsumerId, Semantics Semantics);
