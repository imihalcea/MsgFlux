using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;

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
        var maxDop = options.MaxDegreeOfParallelism > 0
            ? options.MaxDegreeOfParallelism
            : Environment.ProcessorCount;

        while (!ct.IsCancellationRequested)
        {
            await FlushPendingAcksAsync(ct);

            // Claim only what we can dispatch right now. FetchUnprocessedAsync claims rows on fetch
            // (state -> Processing, processed_at = now), so reserving a whole PollingBatchSize up front
            // stamps tail rows long before they reach a free slot — they pass StaleProcessingTimeout
            // while still queued and another instance re-claims them. Bounding the claim to the free
            // capacity (MaxDOP - local in-flight) keeps processed_at close to actual dispatch time, so
            // the stale clause only ever fires for genuine crashes.
            //
            // Note: _inFlight counts only this durable source's in-flight work, while the engine's
            // throttle is shared with the in-memory (AtMostOnce) source. Under heavy mixed load this
            // can slightly over-claim (slots taken by in-memory work); the precise fix is to have the
            // source observe the shared semaphore, a larger redesign left for later.
            var capacity = maxDop - _inFlight.Count;
            if (capacity <= 0)
            {
                await SafeDelay(TimeSpan.FromMilliseconds(50), ct);
                continue;
            }

            IReadOnlyList<Message> batch;
            try
            {
                batch = await store.FetchUnprocessedAsync(
                    messageType: null,
                    maxCount: Math.Min(batchSize, capacity),
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

                // The row was already claimed (state -> Processing) atomically by FetchUnprocessedAsync,
                // so no OnProcessing hook is needed. _inFlight still guards against this instance
                // re-dispatching a message that becomes stale while it is still being processed locally.
                yield return new DispatchItem(
                    Message: msg,
                    OnAck: ct2 => { _inFlight.TryRemove(key, out _); _pendingAcks.Enqueue((msg.MessageId, msg.ConsumerId)); return Task.CompletedTask; },
                    OnFail: (reason, c) => { _inFlight.TryRemove(key, out _); return store.MarkAsFailedAsync(msg.MessageId, msg.ConsumerId, reason, c); },
                    OnDeadLetter: (reason, c) => { _inFlight.TryRemove(key, out _); return store.DeadLetterAsync(msg.MessageId, msg.ConsumerId, reason, c); });
            }

            // Nothing new dispatched — all filtered by dedup or dead-lettered.
            // Short backoff to avoid tight-looping, but not the full poll interval
            // so new messages arriving shortly after are picked up fast.
            if (yielded == 0)
                await SafeDelay(TimeSpan.FromMilliseconds(50), ct);
        }
    }

    private Task FlushPendingAcksAsync(CancellationToken ct)
        => FlushQueueAsync(_pendingAcks, store.AcknowledgeBatchAsync, LogAckFlushError, ct);

    private async Task FlushQueueAsync(
        ConcurrentQueue<(Guid, string)> queue,
        Func<IReadOnlyList<(Guid, string)>, CancellationToken, Task> persist,
        Action<ILogger, int, Exception> logError,
        CancellationToken ct = default)
    {
        if (queue.IsEmpty) return;

        var items = new List<(Guid, string)>();
        while (queue.TryDequeue(out var item))
            items.Add(item);

        if (items.Count == 0) return;

        try
        {
            await persist(items, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logError(logger, items.Count, ex);
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
