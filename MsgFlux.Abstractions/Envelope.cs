namespace MsgFlux.Abstractions;

/// <summary>
/// Transport envelope carried through the in-memory channel.
/// </summary>
/// <param name="MessageId">Unique message identifier.</param>
/// <param name="Payload">Serialized message body.</param>
/// <param name="Headers">Transport headers (e.g., trace context).</param>
/// <param name="MessageType">Type name of the original message.</param>
/// <param name="TargetConsumerId">
/// Optional stable hash of a single target consumer. When set, the Engine will dispatch
/// this envelope only to the matching consumer (used by the replay path to deliver
/// AtLeastOnce retries without re-invoking already-acknowledged consumers).
/// When null, the envelope is dispatched to all consumers registered for MessageType.
/// </param>
public record Envelope(
    string MessageId,
    byte[] Payload,
    Dictionary<string, string> Headers,
    string MessageType,
    string? TargetConsumerId = null);
