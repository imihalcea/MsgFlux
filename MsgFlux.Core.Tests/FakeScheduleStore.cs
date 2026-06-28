using System.Collections.Concurrent;
using MsgFlux.Abstractions;

namespace MsgFlux.Core.Tests;

/// <summary>
/// In-memory IScheduleStore for unit testing. Keyed by message Id. FetchDueAsync returns Scheduled
/// rows whose DeliverAt has passed. MarkPromotedAsync can be made to fail a number of times to
/// simulate a crash between hot-path persistence and the promoted-mark (idempotence tests).
/// </summary>
public class FakeScheduleStore : IScheduleStore
{
    public ConcurrentDictionary<Guid, ScheduledMessage> Items { get; } = new();
    public List<Guid> PromotedCalls { get; } = new();
    public int MarkPromotedThrowTimes { get; set; }

    public Task ScheduleAsync(IReadOnlyList<ScheduledMessage> messages, CancellationToken ct = default)
    {
        foreach (var m in messages) Items.TryAdd(m.Id, m);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<ScheduledMessage>> FetchDueAsync(int maxCount = 100, CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow;
        IReadOnlyList<ScheduledMessage> due = Items.Values
            .Where(m => m.Status == ScheduledState.Scheduled && m.DeliverAt <= now)
            .OrderBy(m => m.DeliverAt).ThenBy(m => m.CreatedAt)
            .Take(maxCount)
            .ToList();
        return Task.FromResult(due);
    }

    public Task MarkPromotedAsync(IReadOnlyList<Guid> ids, CancellationToken ct = default)
    {
        if (MarkPromotedThrowTimes > 0)
        {
            MarkPromotedThrowTimes--;
            throw new InvalidOperationException("simulated mark-promoted failure");
        }

        PromotedCalls.AddRange(ids);
        foreach (var id in ids)
            if (Items.TryGetValue(id, out var m))
                Items[id] = m with { Status = ScheduledState.Promoted };
        return Task.CompletedTask;
    }

    public Task<bool> CancelScheduledAsync(Guid id, CancellationToken ct = default)
    {
        if (Items.TryGetValue(id, out var m) && m.Status == ScheduledState.Scheduled)
        {
            Items[id] = m with { Status = ScheduledState.Cancelled };
            return Task.FromResult(true);
        }
        return Task.FromResult(false);
    }

    public Task<int> PurgeAsync(TimeSpan olderThan, CancellationToken ct = default) => Task.FromResult(0);
}
