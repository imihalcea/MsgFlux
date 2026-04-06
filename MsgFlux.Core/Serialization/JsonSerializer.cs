using System.IO.Compression;
using System.Text.Json;
using Microsoft.IO;
using SysJsonSerializer = System.Text.Json.JsonSerializer;

namespace MsgFlux.Core.Serialization;

public class JsonSerializer : ISerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static readonly RecyclableMemoryStreamManager StreamManager = new();

    public byte[] Serialize<T>(T message)
    {
        using var outputStream = StreamManager.GetStream();
        using (var brotliStream = new BrotliStream(outputStream, CompressionLevel.Fastest, leaveOpen: true))
        {
            SysJsonSerializer.Serialize(brotliStream, message, Options);
        }
        return outputStream.ToArray();
    }

    public T? Deserialize<T>(byte[] bytes)
    {
        using var inputStream = StreamManager.GetStream(bytes);
        using var brotliStream = new BrotliStream(inputStream, CompressionMode.Decompress);
        return SysJsonSerializer.Deserialize<T>(brotliStream, Options);
    }

    public object? Deserialize(byte[] bytes, Type type)
    {
        using var inputStream = StreamManager.GetStream(bytes);
        using var brotliStream = new BrotliStream(inputStream, CompressionMode.Decompress);
        return SysJsonSerializer.Deserialize(brotliStream, type, Options);
    }
}
