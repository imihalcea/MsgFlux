using System.IO.Compression;
using System.Text.Json;
using SysJsonSerializer = System.Text.Json.JsonSerializer;

namespace MsgFlux.Core.Serialization;

public class JsonSerializer : ISerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public byte[] Serialize<T>(T message)
    {
        using var outputStream = new MemoryStream();
        using (var brotliStream = new BrotliStream(outputStream, CompressionLevel.Fastest))
        {
            SysJsonSerializer.Serialize(brotliStream, message, Options);
        }
        return outputStream.ToArray();
    }

    public T? Deserialize<T>(byte[] bytes)
    {
        using var inputStream = new MemoryStream(bytes);
        using var brotliStream = new BrotliStream(inputStream, CompressionMode.Decompress);
        return SysJsonSerializer.Deserialize<T>(brotliStream, Options);
    }

    public object? Deserialize(byte[] bytes, Type type)
    {
        using var inputStream = new MemoryStream(bytes);
        using var brotliStream = new BrotliStream(inputStream, CompressionMode.Decompress);
        return SysJsonSerializer.Deserialize(brotliStream, type, Options);
    }
}