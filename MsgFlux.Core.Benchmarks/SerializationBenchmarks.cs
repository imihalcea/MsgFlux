using BenchmarkDotNet.Attributes;
using MsgFlux.Core.Serialization;
using ProtoBuf;

namespace MsgFlux.Core.Benchmarks;

[ProtoContract]
public class BenchmarkMessage
{
    [ProtoMember(1)]
    public int Id { get; set; }
    [ProtoMember(2)]
    public string Name { get; set; } = string.Empty;
    [ProtoMember(3)]
    public DateTime CreatedAt { get; set; }
    [ProtoMember(4)]
    public List<int> Data { get; set; } = new();
}

[MemoryDiagnoser]
public class SerializationBenchmarks
{
    private readonly ISerializer _jsonSerializer;
    private readonly ISerializer _protoBufSerializer;
    private readonly BenchmarkMessage _message;
    private readonly byte[] _jsonBytes;
    private readonly byte[] _protoBytes;

    public SerializationBenchmarks()
    {
        _jsonSerializer = new JsonSerializer();
        _protoBufSerializer = new ProtoBufSerializer();

        _message = new BenchmarkMessage
        {
            Id = 12345,
            Name = "Benchmark Message",
            CreatedAt = DateTime.UtcNow,
            Data = Enumerable.Range(0, 100).ToList()
        };

        _jsonBytes = _jsonSerializer.Serialize(_message);
        _protoBytes = _protoBufSerializer.Serialize(_message);
    }

    [Benchmark]
    public byte[] Json_Serialize() => _jsonSerializer.Serialize(_message);

    [Benchmark]
    public BenchmarkMessage? Json_Deserialize() => _jsonSerializer.Deserialize<BenchmarkMessage>(_jsonBytes);

    [Benchmark]
    public byte[] ProtoBuf_Serialize() => _protoBufSerializer.Serialize(_message);

    [Benchmark]
    public BenchmarkMessage? ProtoBuf_Deserialize() => _protoBufSerializer.Deserialize<BenchmarkMessage>(_protoBytes);

    [GlobalSetup]
    public void Setup()
    {
        Console.WriteLine($"JSON Size: {_jsonBytes.Length} bytes");
        Console.WriteLine($"ProtoBuf Size: {_protoBytes.Length} bytes");
    }
}