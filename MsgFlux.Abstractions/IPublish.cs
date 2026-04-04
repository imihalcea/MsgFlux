namespace MsgFlux.Abstractions;

public interface IPublish
{
    Task PublishAsync<T>(T message, CancellationToken ct = default);
}
