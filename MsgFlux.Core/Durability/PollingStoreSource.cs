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
    public async IAsyncEnumerable<DispatchItem> StreamAsync([EnumeratorCancellation] CancellationToken ct)
    {
        var pollInterval = options.ReplayInterval;
        var batchSize = options.PollingBatchSize;

        while (!ct.IsCancellationRequested)
        {
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

            foreach (var msg in batch)
            {
                if (ct.IsCancellationRequested) yield break;

                // Dead-letter if retries exhausted; do not dispatch.
                if (msg.RetryCount >= options.MaxDeadLetterRetries)
                {
                    await SafeInvoke(() => store.DeadLetterAsync(msg.MessageId, msg.ConsumerId,
                        $"Max retries ({options.MaxDeadLetterRetries}) exceeded", ct), msg, "DeadLetter");
                    continue;
                }

                // Claim the row synchronously before yielding, so the next poll cycle
                // won't see it (state moves from Pending/Failed to Processing).
                await SafeInvoke(() => store.MarkAsProcessingAsync(msg.MessageId, msg.ConsumerId, ct), msg, "MarkAsProcessing");

                yield return new DispatchItem(
                    Message: msg,
                    OnAck: c => store.AcknowledgeAsync(msg.MessageId, msg.ConsumerId, c),
                    OnFail: (reason, c) => store.MarkAsFailedAsync(msg.MessageId, msg.ConsumerId, reason, c),
                    OnDeadLetter: (reason, c) => store.DeadLetterAsync(msg.MessageId, msg.ConsumerId, reason, c));
            }
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

    [LoggerMessage(LogLevel.Error, "Error fetching unprocessed messages from durable store")]
    static partial void LogFetchError(ILogger logger, Exception ex);

    [LoggerMessage(LogLevel.Error, "Store operation {Operation} failed for message {MessageId} / consumer {ConsumerId}")]
    static partial void LogOperationError(ILogger logger, string operation, string messageId, string consumerId, Exception ex);
}
