namespace MsgFlux.Abstractions;

public enum MessageState
{
    Pending,
    Processing,
    Completed,
    Failed,
    DeadLettered
}
