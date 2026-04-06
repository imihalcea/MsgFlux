using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

/// <summary>
/// Generic polling adapter: turns any <see cref="IMessageStore"/> into an <see cref="IMessageSource"/>
/// by repeatedly calling <see cref="IMessageStore.FetchUnprocessedAsync"/>. Backs off when empty,
/// streams immediately when work is available. Written once so new store backends need only
/// implement the primitive CRUD contract.
/// </summary>
public sealed partial class PollingStoreSource(
    IMessageStore store,
    MsgFluxOptions options,
    ILogger<PollingStoreSource> logger) : IMessageSource
{
    private readonly ConcurrentDictionary<(Guid MessageId, string ConsumerId), byte> _inFlight = new();
    private readonly ConcurrentQueue<(Guid MessageId, string ConsumerId)> _pendingAcks = new();

    public async IAsyncEnumerable<DispatchItem> StreamAsync([EnumeratorCancellation] CancellationToken ct)
    {
        var pollInterval = options.ReplayInterval;
        var batchSize = options.PollingBatchSize;

        while (!ct.IsCancellationRequested)
        {
            await FlushPendingAcksAsync(ct);

            IReadOnlyList<Message> batch;
            try
            {
                batch = await store.FetchUnprocessedAsync(
                    messageType: null,
                    maxCount: batchSize,
                    staleProcessingTimeout: options.StaleProcessingTimeout,
                    ct: ct);
            }
            catch (OperationCanceledException) { yield break; }
            catch (Exception ex)
            {
                LogFetchError(logger, ex);
                await SafeDelay(pollInterval, ct);
                continue;
            }

            if (batch.Count == 0)
            {
                await SafeDelay(pollInterval, ct);
                continue;
            }

            var yielded = 0;

            foreach (var msg in batch)
            {
                if (ct.IsCancellationRequested) yield break;

                var key = (msg.MessageId, msg.ConsumerId);

                // Skip if already in-flight (prevents duplicate dispatch on re-fetch).
                if (!_inFlight.TryAdd(key, 0)) continue;

                // Dead-letter if retries exhausted; do not dispatch.
                if (msg.RetryCount >= options.MaxDeadLetterRetries)
                {
                    await SafeInvoke(() => store.DeadLetterAsync(msg.MessageId, msg.ConsumerId,
                        $"Max retries ({options.MaxDeadLetterRetries}) exceeded", ct), msg, "DeadLetter");
                    _inFlight.TryRemove(key, out _);
                    continue;
                }

                yielded++;

                // Claim is deferred to OnProcessing so the stale-processing timeout
                // starts when the Engine is ready to dispatch, not when the item is yielded.
                yield return new DispatchItem(
                    Message: msg,
                    OnProcessing: async c =>
                    {
                        try { await store.MarkAsProcessingAsync(msg.MessageId, msg.ConsumerId, c); }
                        catch { _inFlight.TryRemove(key, out _); throw; }
                    },
                    OnAck: ct2 => { _inFlight.TryRemove(key, out _); _pendingAcks.Enqueue((msg.MessageId, msg.ConsumerId)); return Task.CompletedTask; },
                    OnFail: (reason, c) => { _inFlight.TryRemove(key, out _); return store.MarkAsFailedAsync(msg.MessageId, msg.ConsumerId, reason, c); },
                    OnDeadLetter: (reason, c) => { _inFlight.TryRemove(key, out _); return store.DeadLetterAsync(msg.MessageId, msg.ConsumerId, reason, c); });
            }

            // Nothing new dispatched — all filtered by dedup or dead-lettered.
            // Back off to avoid tight-looping SELECT queries on the store.
            if (yielded == 0)
                await SafeDelay(pollInterval, ct);
        }
    }

    private async Task FlushPendingAcksAsync(CancellationToken ct)
    {
        if (_pendingAcks.IsEmpty) return;

        var items = new List<(Guid, string)>();
        while (_pendingAcks.TryDequeue(out var item))
            items.Add(item);

        if (items.Count == 0) return;

        try
        {
            await store.AcknowledgeBatchAsync(items, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogAckFlushError(logger, items.Count, ex);
        }
    }

    private static async Task SafeDelay(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); }
        catch (OperationCanceledException) { }
    }

    private async Task SafeInvoke(Func<Task> action, Message msg, string op)
    {
        try { await action(); }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogOperationError(logger, op, msg.MessageId, msg.ConsumerId, ex);
        }
    }

    public void Complete()
    {
        // Flush remaining acks synchronously on shutdown.
        FlushPendingAcksAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    [LoggerMessage(LogLevel.Error, "Failed to flush {Count} pending acks")]
    static partial void LogAckFlushError(ILogger logger, int count, Exception ex);

    [LoggerMessage(LogLevel.Error, "Error fetching unprocessed messages from durable store")]
    static partial void LogFetchError(ILogger logger, Exception ex);

    [LoggerMessage(LogLevel.Error, "Store operation {Operation} failed for message {MessageId} / consumer {ConsumerId}")]
    static partial void LogOperationError(ILogger logger, string operation, Guid messageId, string consumerId, Exception ex);
}
