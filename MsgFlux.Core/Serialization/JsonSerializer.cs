using System.Text.Json;
using SysJsonSerializer = System.Text.Json.JsonSerializer;

namespace MsgFlux.Core.Serialization;

public class JsonSerializer : ISerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true
    };


    public byte[] Serialize<T>(T message) =>
        SysJsonSerializer.SerializeToUtf8Bytes(message, Options);


    public T? Deserialize<T>(byte[] bytes) =>
        SysJsonSerializer.Deserialize<T>(bytes, Options);

    public object? Deserialize(byte[] bytes, Type type) =>
        SysJsonSerializer.Deserialize(bytes, type, Options);
}