using System.Runtime.CompilerServices;
using System.Threading.Channels;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;

namespace MsgFlux.Core;

/// <summary>
/// In-memory fire-and-forget queue backing AtMostOnce consumers. Uses a bounded
/// <see cref="Channel{T}"/> for native async signalling and backpressure.
/// No state tracking, no persistence — on ack or fail the message simply leaves the queue.
/// </summary>
public sealed class InMemoryMessageSource : IMessageSource
{
    private readonly Channel<Message> _channel;

    public InMemoryMessageSource(MsgFluxOptions options)
    {
        _channel = Channel.CreateBounded<Message>(new BoundedChannelOptions(options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    public void Complete() => _channel.Writer.Complete();

    /// <summary>
    /// Enqueues messages. Blocks (awaits) if the channel is full — producer-side backpressure.
    /// </summary>
    public async Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
    {
        foreach (var message in messages)
        {
            await _channel.Writer.WriteAsync(message, ct);
        }
    }

    public async IAsyncEnumerable<DispatchItem> StreamAsync([EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var message in _channel.Reader.ReadAllAsync(ct))
        {
            // All callbacks are no-ops: AtMostOnce messages have no state to update.
            yield return new DispatchItem(message);
        }
    }
}
