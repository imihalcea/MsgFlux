namespace MsgFlux.Abstractions;

/// <summary>
/// A message awaiting deferred delivery. Holds the original message content as an opaque blob
/// (<see cref="Msg"/>) so the schedule store stays unaware of payload/headers/consumers. One row
/// per scheduled publish; the fan-out across the message type's consumers happens at promotion time.
/// </summary>
public record ScheduledMessage
{
    /// <summary>Generated message id; stable across promotion and used for cancellation.</summary>
    public required Guid Id { get; init; }
    /// <summary>Logical message type that drives routing once promoted.</summary>
    public required string Type { get; init; }
    /// <summary>Serialized content of the original published message (payload + headers), round-tripped opaquely by the store.</summary>
    public required byte[] Msg { get; init; }
    /// <summary>UTC instant at or after which the message becomes eligible for delivery.</summary>
    public required DateTimeOffset DeliverAt { get; init; }
    public ScheduledState Status { get; init; } = ScheduledState.Scheduled;
    public DateTimeOffset CreatedAt { get; init; }
}
