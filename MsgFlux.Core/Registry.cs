using System.Reflection;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public class Registry
{
    private readonly HashSet<Type> _messageTypes = new();
    private readonly Dictionary<Type, Func<object, object, CancellationToken, Task>> _invokers = new();
    private readonly Dictionary<Type, Type> _consumerServiceTypes = new();

    public void Register(Type messageType)
    {
        if (!_messageTypes.Add(messageType)) return;

        _consumerServiceTypes[messageType] = typeof(IConsume<>).MakeGenericType(messageType);
        _invokers[messageType] = CreateInvoker(messageType);
    }

    public IEnumerable<Type> GetMessageTypes() => _messageTypes;

    public Type GetConsumerServiceType(Type messageType) => _consumerServiceTypes[messageType];

    public Func<object, object, CancellationToken, Task> GetInvoker(Type messageType) => _invokers[messageType];

    private static Func<object, object, CancellationToken, Task> CreateInvoker(Type messageType)
    {
        return typeof(Registry)
            .GetMethod(nameof(TypedInvoke), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(messageType)
            .CreateDelegate<Func<object, object, CancellationToken, Task>>();
    }

    private static Task TypedInvoke<T>(object consumer, object message, CancellationToken ct)
    {
        return ((IConsume<T>)consumer).HandleAsync((T)message, ct);
    }
}
