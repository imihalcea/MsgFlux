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
    /// <para>
    /// At-least-once means a message may be delivered more than once (e.g. a crash after the
    /// consumer ran but before the ack, or a slow handle that exceeds StaleProcessingTimeout and
    /// is reclaimed by another instance). Consumers MUST therefore be idempotent — handling the
    /// same message twice has to be safe. To avoid legitimate re-delivery of in-progress work,
    /// keep StaleProcessingTimeout comfortably above the longest expected handle duration.
    /// </para>
    /// </summary>
    AtLeastOnce = 1
}
