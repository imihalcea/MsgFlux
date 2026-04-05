namespace MsgFlux.Abstractions;

/// <summary>
/// Delivery guarantee for a consumer.
/// </summary>
public enum Semantics
{
    /// <summary>
    /// Fire-and-forget in-memory delivery. No persistence; messages are lost on crash.
    /// </summary>
    AtMostOnce = 0,

    /// <summary>
    /// Guaranteed delivery via a backing message store. Requires an IMessageStore provider.
    /// Each AtLeastOnce consumer gets its own inbox row (duplicated message per consumer).
    /// </summary>
    AtLeastOnce = 1
}
