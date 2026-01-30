using MsgFlux.Core.Serialization;
using ProtoBuf;

namespace MsgFlux.Core.Tests;

[ProtoContract]
public class TestMessage
{
    [ProtoMember(1)]
    public int Id { get; set; }
    [ProtoMember(2)]
    public string Name { get; set; } = string.Empty;
}

public class SerializationTests
{
    [Test]
    public void ProtoBufSerializer_RoundTrip()
    {
        var serializer = new ProtoBufSerializer();
        var message = new TestMessage { Id = 1, Name = "Test" };

        var bytes = serializer.Serialize(message);
        var deserialized = serializer.Deserialize<TestMessage>(bytes);

        Assert.That(deserialized, Is.Not.Null);
        Assert.That(deserialized.Id, Is.EqualTo(message.Id));
        Assert.That(deserialized.Name, Is.EqualTo(message.Name));
    }
}