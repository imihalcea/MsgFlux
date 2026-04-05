using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgFlux.Abstractions;

namespace MsgFlux.Core;

public partial class MessagePurgeService(
    IMessageStore messageStore,
    MsgFluxOptions options,
    ILogger<MessagePurgeService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(options.PurgeInterval, stoppingToken);

            try
            {
                var purged = await messageStore.PurgeCompletedAsync(options.PurgeOlderThan, stoppingToken);
                if (purged > 0)
                    LogPurged(logger, purged, options.PurgeOlderThan);
            }
            catch (Exception ex)
            {
                LogPurgeError(logger, ex);
            }
        }
    }

    [LoggerMessage(LogLevel.Information, "Purged {Count} completed messages older than {OlderThan}")]
    static partial void LogPurged(ILogger logger, int count, TimeSpan olderThan);

    [LoggerMessage(LogLevel.Error, "Error during message purge")]
    static partial void LogPurgeError(ILogger logger, Exception ex);
}
