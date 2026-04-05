namespace MsgFlux.Abstractions;

public interface IPublish
{
    Task PublishAsync<T>(T payload, CancellationToken ct = default);
}
