using System.Diagnostics;
using Microsoft.Extensions.Logging;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public partial class Publisher(IChannelRxTx channelRxTx, ISerializer serializer, ILogger<Publisher> logger, MsgFluxOptions options) : IPublish
{
    private static readonly ActivitySource ActivitySource = new("MsgFlux");

    public Publisher(IChannelRxTx channelRxTx, ISerializer serializer, ILogger<Publisher> logger) 
        : this(channelRxTx, serializer, logger, new MsgFluxOptions())
    {
    }

    public async Task PublishAsync<T>(T message, CancellationToken ct = default)
    {
        // ReSharper disable once ExplicitCallerInfoArgument
        using var activity = ActivitySource.StartActivity(nameof(PublishAsync), ActivityKind.Producer);
        
        var headers = new Dictionary<string, string>();
        if (activity != null)
        {
            // Inject TraceContext into headers
            headers["traceparent"] = activity.Id ?? string.Empty;
            if (activity.TraceStateString != null)
            {
                headers["tracestate"] = activity.TraceStateString;
            }
        }

        var payload = serializer.Serialize(message);

        if (payload.Length > options.MaxPayloadSizeKb * 1024)
        {
            LogPayloadTooLarge(logger, payload.Length, typeof(T).Name, options.MaxPayloadSizeKb);
        }

        var envelope = new Envelope(
            MessageId: Guid.NewGuid().ToString(),
            Payload: payload,
            Headers: headers,
            MessageType: typeof(T).Name
        );

        var writer = channelRxTx.GetWriter(typeof(T));
        await writer.WriteAsync(envelope, ct);
    }

    [LoggerMessage(LogLevel.Warning, "Payload size {PayloadSize} for message {MessageType} exceeds {MaxPayloadSize}KB")]
    static partial void LogPayloadTooLarge(ILogger logger, int payloadSize, string messageType, int maxPayloadSize);
}
