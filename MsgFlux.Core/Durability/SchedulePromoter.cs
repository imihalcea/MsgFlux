using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

/// <summary>
/// Bridges scheduling and delivery. Periodically claims due scheduled messages from the
/// <see cref="IScheduleStore"/>, fans each out into per-consumer rows in the <see cref="IMessageStore"/>
/// hot path, then marks them promoted. The existing engine then picks them up as ordinary Pending
/// messages — it stays unaware of scheduling.
///
/// Promotion is idempotent: a crash between persisting the hot-path rows and marking the scheduled
/// row promoted simply re-fetches it next cycle; re-persistence is a no-op (ON CONFLICT on the
/// stable MessageId) and the mark is rerun. No distributed transaction across the two stores.
/// </summary>
public sealed partial class SchedulePromoter(
    IScheduleStore scheduleStore,
    IMessageStore messageStore,
    Registry registry,
    ISerializer serializer,
    MsgFluxOptions options,
    ILogger<SchedulePromoter> logger) : BackgroundService
{
    private readonly Dictionary<string, Type> _messageTypesByName =
        registry.GetMessageTypes().ToDictionary(t => t.FullName!, t => t);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var interval = options.PromotionInterval > TimeSpan.Zero
            ? options.PromotionInterval
            : TimeSpan.FromSeconds(1);

        while (!ct.IsCancellationRequested)
        {
            int promoted;
            try
            {
                promoted = await PromoteDueAsync(ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                LogPromoteError(logger, ex);
                promoted = 0;
            }

            // Drain backlog without waiting when work was found; otherwise back off one interval.
            if (promoted == 0)
            {
                try { await Task.Delay(interval, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task<int> PromoteDueAsync(CancellationToken ct)
    {
        var due = await scheduleStore.FetchDueAsync(ct: ct);
        if (due.Count == 0) return 0;

        var rows = new List<Message>();
        var promotedIds = new List<Guid>(due.Count);
        var now = DateTimeOffset.UtcNow;

        foreach (var s in due)
        {
            promotedIds.Add(s.Id);

            if (!_messageTypesByName.TryGetValue(s.Type, out var messageType))
            {
                // No consumer registered for this type — drop (consistent with publish EF-P2),
                // still marked promoted so it is not re-fetched.
                LogUnknownType(logger, s.Id, s.Type);
                continue;
            }

            ScheduledEnvelope envelope;
            try
            {
                envelope = serializer.Deserialize<ScheduledEnvelope>(s.Msg)
                    ?? throw new InvalidOperationException("Scheduled blob deserialized to null");
            }
            catch (Exception ex)
            {
                // Undecodable blob is not retryable; drop but mark promoted.
                LogDeserializeError(logger, s.Id, ex);
                continue;
            }

            foreach (var c in registry.GetConsumers(messageType))
            {
                // Only durable consumers are served via the store; AtMostOnce consumers are not scheduled.
                if (c.Semantics != Semantics.AtLeastOnce) continue;
                rows.Add(new Message
                {
                    MessageId = s.Id,
                    ConsumerId = c.ConsumerId,
                    Payload = envelope.Payload,
                    Headers = envelope.Headers,
                    MessageType = s.Type,
                    CreatedAt = now
                });
            }
        }

        if (rows.Count > 0)
            await messageStore.PersistAsync(rows, ct);

        await scheduleStore.MarkPromotedAsync(promotedIds, ct);
        return due.Count;
    }

    [LoggerMessage(LogLevel.Error, "Error during schedule promotion cycle")]
    static partial void LogPromoteError(ILogger logger, Exception ex);

    [LoggerMessage(LogLevel.Warning, "Scheduled message {MessageId} has unknown type {MessageType}; dropped on promotion")]
    static partial void LogUnknownType(ILogger logger, Guid messageId, string messageType);

    [LoggerMessage(LogLevel.Error, "Failed to deserialize scheduled message {MessageId}; dropped on promotion")]
    static partial void LogDeserializeError(ILogger logger, Guid messageId, Exception ex);
}
