namespace MsgFlux.Core;

public class Registry
{
    private readonly HashSet<Type> _messageTypes = new();

    public void Register(Type messageType)
    {
        _messageTypes.Add(messageType);
    }

    public IEnumerable<Type> GetMessageTypes() => _messageTypes;
}
