namespace MsgFlux.Core;

public interface IConsume<in T>
{
    Task HandleAsync(T message, CancellationToken ct);
}
