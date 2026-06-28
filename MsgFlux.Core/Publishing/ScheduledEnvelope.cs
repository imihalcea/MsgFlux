namespace MsgFlux.Core;

/// <summary>
/// Opaque content of a scheduled message: the serialized payload plus the captured trace headers.
/// Stored as the <c>Msg</c> blob and round-tripped between <see cref="Scheduler"/> (write) and
/// <see cref="SchedulePromoter"/> (read, then fan-out across the type's consumers).
/// </summary>
internal sealed record ScheduledEnvelope(byte[] Payload, Dictionary<string, string> Headers);
