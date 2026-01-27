namespace MsgFlux.Core.Serialization;

public interface ISerializer
{
    byte[] Serialize<T>(T message);
    T? Deserialize<T>(byte[] bytes);
    object? Deserialize(byte[] bytes, Type type);
}