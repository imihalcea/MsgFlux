namespace MsgFlux.Abstractions;

public interface IConsume<in T>
{
    Task HandleAsync(T message, CancellationToken ct);
}
