using System.Text.Json;
using JsonSerializer = MsgFlux.Core.Serialization.JsonSerializer;

namespace MsgFlux.Core.Tests;

public class EmailMessage
{
    public string To { get; set; } = string.Empty;
    public string From { get; set; } = string.Empty;
    public string Subject { get; set; } = string.Empty;
    public string Body { get; set; } = string.Empty;
    public DateTime SentAt { get; set; }
    public bool IsHtml { get; set; }
}

public class SerializationTests
{
    [Test]
    public void JsonSerializer_RoundTrip_With_Compression()
    {
        var serializer = new JsonSerializer();
        var message = new EmailMessage 
        { 
            To = "user@example.com", 
            From = "noreply@system.com",
            Subject = "Welcome!",
            Body = "Hello User",
            SentAt = DateTime.UtcNow,
            IsHtml = true
        };

        var bytes = serializer.Serialize(message);
        var deserialized = serializer.Deserialize<EmailMessage>(bytes);

        Assert.That(deserialized, Is.Not.Null);
        Assert.That(deserialized.To, Is.EqualTo(message.To));
        Assert.That(deserialized.Subject, Is.EqualTo(message.Subject));
    }

    [Test]
    public void Deserialize_Should_Throw_On_Corrupted_Data()
    {
        var serializer = new JsonSerializer();
        var corruptedBytes = new byte[] { 0x1, 0x2, 0x3, 0x4 }; // Not valid Brotli data

        Assert.Throws<JsonException>(() => serializer.Deserialize<EmailMessage>(corruptedBytes));
    }

    [Test]
    public void Deserialize_Should_Throw_On_Invalid_Json_Inside_Compressed_Stream()
    {
        var serializer = new JsonSerializer();
        
        // Manually compress invalid JSON
        using var ms = new MemoryStream();
        using (var brotli = new System.IO.Compression.BrotliStream(ms, System.IO.Compression.CompressionLevel.Fastest))
        using (var writer = new StreamWriter(brotli))
        {
            writer.Write("{ invalid json }");
        }
        var bytes = ms.ToArray();

        Assert.Throws<JsonException>(() => serializer.Deserialize<EmailMessage>(bytes));
    }

    [Test]
    public void JsonSerializer_Should_Be_ThreadSafe()
    {
        var serializer = new JsonSerializer();
        var message = new EmailMessage { To = "concurrent@test.com" };
        
        // Run 1000 serializations in parallel
        Parallel.For(0, 1000, i =>
        {
            var bytes = serializer.Serialize(message);
            var deserialized = serializer.Deserialize<EmailMessage>(bytes);
            
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized.To, Is.EqualTo("concurrent@test.com"));
        });
    }
}
