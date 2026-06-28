using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;
using MsgFlux.Core.Configuration;

namespace MsgFlux.Core;

/// <summary>
/// Periodically purges completed messages from the durable store and promoted/cancelled rows from the
/// schedule store. No-op when neither store is registered.
/// </summary>
public partial class MessagePurgeService(
    MsgFluxOptions options,
    ILogger<MessagePurgeService> logger,
    IMessageStore? messageStore = null,
    IScheduleStore? scheduleStore = null) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (messageStore is null && scheduleStore is null) return;

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(options.PurgeInterval, stoppingToken);

            try
            {
                if (messageStore is not null)
                {
                    var purged = await messageStore.PurgeCompletedAsync(options.PurgeOlderThan, stoppingToken);
                    if (purged > 0)
                        LogPurged(logger, purged, options.PurgeOlderThan);
                }

                if (scheduleStore is not null)
                {
                    var purgedScheduled = await scheduleStore.PurgeAsync(options.PurgeOlderThan, stoppingToken);
                    if (purgedScheduled > 0)
                        LogScheduledPurged(logger, purgedScheduled, options.PurgeOlderThan);
                }
            }
            catch (Exception ex)
            {
                LogPurgeError(logger, ex);
            }
        }
    }

    [LoggerMessage(LogLevel.Information, "Purged {Count} completed messages older than {OlderThan}")]
    static partial void LogPurged(ILogger logger, int count, TimeSpan olderThan);

    [LoggerMessage(LogLevel.Information, "Purged {Count} promoted/cancelled scheduled messages older than {OlderThan}")]
    static partial void LogScheduledPurged(ILogger logger, int count, TimeSpan olderThan);

    [LoggerMessage(LogLevel.Error, "Error during message purge")]
    static partial void LogPurgeError(ILogger logger, Exception ex);
}
