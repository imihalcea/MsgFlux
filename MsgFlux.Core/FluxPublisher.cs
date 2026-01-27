using System.Diagnostics;
using MsgFlux.Core.RxTx;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

public class FluxPublisher(IChannelRxTx channelRxTx, ISerializer serializer) : IPublish
{
    private static readonly ActivitySource ActivitySource = new("Flux");

    public async Task PublishAsync<T>(T message, CancellationToken ct = default)
    {
        // ReSharper disable once ExplicitCallerInfoArgument
        using var activity = ActivitySource.StartActivity("FluxPublisher.PublishAsync", ActivityKind.Producer);
        
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
        var envelope = new Envelope(
            MessageId: Guid.NewGuid().ToString(),
            Payload: payload,
            Headers: headers,
            MessageType: typeof(T).Name
        );

        var writer = channelRxTx.GetWriter(typeof(T));
        await writer.WriteAsync(envelope, ct);
    }
}
