namespace MsgFlux.Abstractions;

public record Envelope(string MessageId, byte[] Payload, Dictionary<string, string> Headers, string MessageType);
