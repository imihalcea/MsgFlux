namespace MsgFlux.Core;

public interface IPublish
{
    Task PublishAsync<T>(T message, CancellationToken ct = default);
}
