using System.Diagnostics;
using MsgFlux.Core.Configuration;
using MsgFlux.Core.Serialization;

namespace MsgFlux.Core;

/// <summary>
/// Message-building primitives shared by the immediate <see cref="Publisher"/> and the deferred
/// <see cref="Scheduler"/>: the "MsgFlux" ActivitySource, W3C trace-context header capture, and
/// payload serialization with the configured size validation.
/// </summary>
internal static class MessageContent
{
    public static readonly ActivitySource ActivitySource = new("MsgFlux");

    /// <summary>Captures the current trace context (traceparent/tracestate) into message headers.</summary>
    public static Dictionary<string, string> CaptureTraceHeaders(Activity? activity)
    {
        var headers = new Dictionary<string, string>();
        if (activity != null)
        {
            headers["traceparent"] = activity.Id ?? string.Empty;
            if (activity.TraceStateString != null)
                headers["tracestate"] = activity.TraceStateString;
        }
        return headers;
    }

    /// <summary>Serializes <paramref name="payload"/> and enforces the configured max payload size.</summary>
    public static byte[] SerializeValidated<T>(ISerializer serializer, MsgFluxOptions options, T payload)
    {
        var payloadBytes = serializer.Serialize(payload);
        if (payloadBytes.Length > options.MaxPayloadSizeKb * 1024)
        {
            throw new InvalidOperationException(
                $"Payload size {payloadBytes.Length} bytes for message {typeof(T).Name} exceeds the configured limit of {options.MaxPayloadSizeKb}KB.");
        }
        return payloadBytes;
    }
}
