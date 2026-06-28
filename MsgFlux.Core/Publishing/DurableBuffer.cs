using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;

namespace MsgFlux.Core;

/// <summary>
/// Accumulates durable messages and flushes them in batch to an <see cref="IMessageStore"/>
/// when the count threshold is reached or the periodic interval fires — whichever comes first.
///
/// Uses a group-commit model: <see cref="AddAsync"/> only completes once the batch containing its
/// messages has been durably persisted, so a publisher is never acknowledged before the write
/// reaches the store. Concurrent publishes are coalesced into a single batch (one flush in flight
/// at a time), preserving batching throughput without an at-least-once loss window. A bounded
/// number of in-flight messages applies backpressure to the producer instead of buffering unbounded.
/// </summary>
public sealed partial class DurableBuffer(
    MsgFluxOptions options,
    ILogger<DurableBuffer> logger,
    IMessageStore? store = null) : IHostedService, IAsyncDisposable
{
    private readonly Lock _lock = new();
    private List<Message> _buffer = [];
    private TaskCompletionSource _batch = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task? _flushLoop;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _flushGate = new(1, 1);
    private readonly SemaphoreSlim _capacity = new(options.MaxBufferedMessages, options.MaxBufferedMessages);
    private int _shutdown;

    /// <summary>
    /// Buffers <paramref name="messages"/> and returns a task that completes once the batch they
    /// belong to is durably persisted. If persistence fails, the task faults with the store error
    /// and the caller is expected to republish (no internal retry; at-least-once is never silently lost).
    /// </summary>
    public async Task AddAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (messages.Count == 0) return;

        // Backpressure: reserve a slot per message before buffering. Once MaxBufferedMessages are
        // awaiting persistence, this waits rather than growing memory unbounded.
        await AcquireCapacityAsync(messages.Count, ct);

        Task batchTask;
        bool shouldFlush;
        lock (_lock)
        {
            _buffer.AddRange(messages);
            batchTask = _batch.Task;
            shouldFlush = _buffer.Count >= options.BufferFlushThreshold;
            _flushLoop ??= FlushLoopAsync(_cts.Token);
        }

        if (shouldFlush)
            _ = TryFlushAsync();

        // Group-commit: complete only once this batch has actually been persisted (or faulted).
        await batchTask;
    }

    internal async Task FlushAsync(CancellationToken ct = default)
    {
        List<Message> batch;
        TaskCompletionSource batchToComplete;
        lock (_lock)
        {
            if (_buffer.Count == 0) return;
            batch = _buffer;
            batchToComplete = _batch;
            _buffer = [];
            _batch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        try
        {
            await store!.PersistAsync(batch, ct);
            batchToComplete.TrySetResult();
        }
        catch (Exception ex)
        {
            // Propagate the failure to every publisher waiting on this batch; they republish.
            batchToComplete.TrySetException(ex);
            throw;
        }
        finally
        {
            _capacity.Release(batch.Count);
        }
    }

    private async Task AcquireCapacityAsync(int count, CancellationToken ct)
    {
        var acquired = 0;
        try
        {
            for (; acquired < count; acquired++)
                await _capacity.WaitAsync(ct);
        }
        catch
        {
            if (acquired > 0) _capacity.Release(acquired);
            throw;
        }
    }

    private async Task TryFlushAsync()
    {
        await _flushGate.WaitAsync();
        try { await FlushAsync(); }
        catch (Exception ex) { LogFlushError(logger, ex); }
        finally { _flushGate.Release(); }
    }

    private async Task FlushLoopAsync(CancellationToken ct)
    {
        var interval = options.BufferFlushInterval > TimeSpan.Zero
            ? options.BufferFlushInterval
            : TimeSpan.FromMilliseconds(100);

        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(interval, ct); }
            catch (OperationCanceledException) { break; }

            await TryFlushAsync();
        }
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    // Flushed during the host's ordered shutdown — before singletons (incl. the store) are disposed.
    public Task StopAsync(CancellationToken cancellationToken) => ShutdownAsync();

    public async ValueTask DisposeAsync()
    {
        await ShutdownAsync();
        _cts.Dispose();
        _flushGate.Dispose();
        _capacity.Dispose();
    }

    private async Task ShutdownAsync()
    {
        if (Interlocked.Exchange(ref _shutdown, 1) == 1) return;

        await _cts.CancelAsync();
        if (_flushLoop is not null)
        {
            try { await _flushLoop; }
            catch (OperationCanceledException) { }
        }

        await _flushGate.WaitAsync();
        try { await FlushAsync(); }
        catch (Exception ex) { LogFlushError(logger, ex); }
        finally { _flushGate.Release(); }
    }

    [LoggerMessage(LogLevel.Error, "Buffered flush to durable store failed")]
    static partial void LogFlushError(ILogger logger, Exception ex);
}
