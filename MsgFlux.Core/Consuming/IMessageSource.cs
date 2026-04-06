using MsgFlux.Abstractions;

namespace MsgFlux.Core;

/// <summary>
/// A stream of messages waiting to be dispatched, paired with completion callbacks
/// that are specific to the backing store (no-ops for in-memory, durable store
/// operations for persisted stores). The Engine consumes <see cref="IMessageSource"/>s
/// uniformly without knowing which backend produced the message.
/// </summary>
public interface IMessageSource
{
    IAsyncEnumerable<DispatchItem> StreamAsync(CancellationToken ct);

    /// <summary>
    /// Signals that no more messages will be produced. Implementations should reject
    /// any subsequent writes after this call.
    /// </summary>
    void Complete();
}

/// <summary>
/// One message ready for dispatch + the completion hooks that bind it back to its source.
/// Any null hook is a no-op.
/// </summary>
public sealed record DispatchItem(
    Message Message,
    Func<CancellationToken, Task>? OnProcessing = null,
    Func<CancellationToken, Task>? OnAck = null,
    Func<string, CancellationToken, Task>? OnFail = null,
    Func<string, CancellationToken, Task>? OnDeadLetter = null);
