using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public class Registry
{
    private readonly HashSet<Type> _messageTypes = new();
    private readonly Dictionary<Type, Func<object, object, CancellationToken, Task>> _invokers = new();
    private readonly Dictionary<Type, Type> _consumerServiceTypes = new();

    public void Register<TMessage>()
    {
        var messageType = typeof(TMessage);
        if (!_messageTypes.Add(messageType)) return;

        _consumerServiceTypes[messageType] = typeof(IConsume<TMessage>);
        _invokers[messageType] = (consumer, message, ct) =>
            ((IConsume<TMessage>)consumer).HandleAsync((TMessage)message, ct);
    }

    public IEnumerable<Type> GetMessageTypes() => _messageTypes;

    public Type GetConsumerServiceType(Type messageType) => _consumerServiceTypes[messageType];

    public Func<object, object, CancellationToken, Task> GetInvoker(Type messageType) => _invokers[messageType];
}
