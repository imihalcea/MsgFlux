namespace MsgFlux.Core;

public record Envelope(string MessageId, byte[] Payload, Dictionary<string, string> Headers, string MessageType);
