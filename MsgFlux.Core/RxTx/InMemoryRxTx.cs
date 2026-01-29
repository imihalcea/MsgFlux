using System.Collections.Concurrent;
using System.Threading.Channels;

namespace MsgFlux.Core.RxTx;

public class InMemoryRxTx(MsgFluxOptions options) : IChannelRxTx
{
    private readonly ConcurrentDictionary<Type, Channel<Envelope>> _channels = new();

    public InMemoryRxTx() : this(new MsgFluxOptions())
    {
    }

    // Keep old constructor for backward compatibility if needed, or remove it.
    // Adapting it to use options internally.
    public InMemoryRxTx(int capacity) : this(new MsgFluxOptions { ChannelCapacity = capacity })
    {
    }

    public ChannelWriter<Envelope> GetWriter(Type messageType)
    {
        return GetChannel(messageType).Writer;
    }

    public ChannelReader<Envelope> GetReader(Type messageType)
    {
        return GetChannel(messageType).Reader;
    }

    private Channel<Envelope> GetChannel(Type messageType)
    {
        return _channels.GetOrAdd(messageType, _ =>
        {
            var channelOptions = new BoundedChannelOptions(options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = true
            };
            return Channel.CreateBounded<Envelope>(channelOptions);
        });
    }
}
