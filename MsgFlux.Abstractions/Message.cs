namespace MsgFlux.Abstractions;

public record Message
{
    public required string MessageId { get; init; }
    /// <summary>
    /// Stable hash of the target consumer's concrete type FullName. One row per (MessageId, ConsumerId).
    /// </summary>
    public required string ConsumerId { get; init; }
    public required byte[] Payload { get; init; }
    public required Dictionary<string, string> Headers { get; init; }
    public required string MessageType { get; init; }
    public MessageState State { get; init; } = MessageState.Pending;
    public int RetryCount { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? ProcessedAt { get; init; }
    public string? ErrorDetails { get; init; }
}
