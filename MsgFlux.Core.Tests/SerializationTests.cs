using MsgFlux.Core.Serialization;

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
}
