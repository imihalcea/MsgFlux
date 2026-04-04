using MsgFlux.Abstractions;

namespace MsgFlux.Postgres.Tests;

public class FakeClock : IClock
{
    public DateTimeOffset UtcNow { get; set; } = DateTimeOffset.UtcNow;

    public void Advance(TimeSpan duration) => UtcNow += duration;
}
