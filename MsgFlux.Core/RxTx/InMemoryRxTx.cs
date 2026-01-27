using System.Collections.Concurrent;
using System.Threading.Channels;

namespace MsgFlux.Core.RxTx;

public class InMemoryRxTx(int capacity = 1000) : IChannelRxTx
{
    private readonly ConcurrentDictionary<Type, Channel<Envelope>> _channels = new();

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
            var options = new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = true
            };
            return Channel.CreateBounded<Envelope>(options);
        });
    }
}
