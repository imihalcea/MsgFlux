namespace MsgFlux.Core;

public class MsgFluxOptions
{
    public int MaxPayloadSizeKb { get; set; } = 64;
    public int ChannelCapacity { get; set; } = 1000;

    public MsgFluxOptions WithMaxPayloadSizeKb(int sizeKb)
    {
        MaxPayloadSizeKb = sizeKb;
        return this;
    }

    public MsgFluxOptions WithChannelCapacity(int capacity)
    {
        ChannelCapacity = capacity;
        return this;
    }
}
