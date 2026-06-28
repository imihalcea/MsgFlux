namespace MsgFlux.Abstractions;

public enum ScheduledState
{
    Scheduled,      // awaiting its due date
    Promoted,       // transferred into the delivery path (terminal, retained until purge)
    Cancelled       // cancelled before promotion (terminal, retained until purge)
}
