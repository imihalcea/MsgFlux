using ProtoBuf;

namespace MsgFlux.Core.Serialization;

public class ProtoBufSerializer : ISerializer
{
    public byte[] Serialize<T>(T message)
    {
        using var stream = new MemoryStream();
        Serializer.Serialize(stream, message);
        return stream.ToArray();
    }

    public T? Deserialize<T>(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        return Serializer.Deserialize<T>(stream);
    }

    public object? Deserialize(byte[] bytes, Type type)
    {
        using var stream = new MemoryStream(bytes);
        return Serializer.Deserialize(type, stream);
    }
}