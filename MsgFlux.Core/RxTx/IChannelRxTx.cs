using System.Threading.Channels;

namespace MsgFlux.Core.RxTx;

public interface IChannelRxTx
{
    ChannelWriter<Envelope> GetWriter(Type messageType);
    ChannelReader<Envelope> GetReader(Type messageType);
}
