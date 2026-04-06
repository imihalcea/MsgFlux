using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

/// <summary>
/// Accumulates durable messages and flushes them in batch to an <see cref="IMessageStore"/>
/// when the count threshold is reached or the periodic interval fires — whichever comes first.
/// </summary>
public sealed partial class DurableBuffer(
    MsgFluxOptions options,
    ILogger<DurableBuffer> logger,
    IMessageStore? store = null) : IAsyncDisposable
{
    private readonly Lock _lock = new();
    private List<Message> _buffer = [];
    private Task? _flushLoop;
    private readonly CancellationTokenSource _cts = new();

    public Task AddAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        bool shouldFlush;
        lock (_lock)
        {
            _buffer.AddRange(messages);
            shouldFlush = _buffer.Count >= options.BufferFlushThreshold;
            _flushLoop ??= FlushLoopAsync(_cts.Token);
        }

        return shouldFlush ? TryFlushAsync(ct) : Task.CompletedTask;
    }

    internal async Task FlushAsync(CancellationToken ct = default)
    {
        List<Message> batch;
        lock (_lock)
        {
            if (_buffer.Count == 0) return;
            batch = _buffer;
            _buffer = [];
        }

        try
        {
            await store!.PersistAsync(batch, ct);
        }
        catch
        {
            // Restore the batch so messages are not lost; next flush will retry.
            lock (_lock)
            {
                batch.AddRange(_buffer);
                _buffer = batch;
            }
            throw;
        }
    }

    private async Task TryFlushAsync(CancellationToken ct = default)
    {
        try { await FlushAsync(ct); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex) { LogFlushError(logger, ex); }
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

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        if (_flushLoop is not null)
        {
            try { await _flushLoop; }
            catch (OperationCanceledException) { }
        }

        try { await FlushAsync(); }
        catch (Exception ex) { LogFlushError(logger, ex); }

        _cts.Dispose();
    }

    [LoggerMessage(LogLevel.Error, "Buffered flush to durable store failed")]
    static partial void LogFlushError(ILogger logger, Exception ex);
}
