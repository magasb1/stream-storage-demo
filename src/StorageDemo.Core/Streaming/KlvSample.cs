namespace StorageDemo.Core.Streaming;

/// <summary>How a KLV packet is tied to the pictures, per MISB ST 1402.2's two carriage modes.</summary>
public enum KlvAlignment
{
    /// <summary>
    /// Synchronous carriage: the packet's PES carried a presentation timestamp, so <see
    /// cref="KlvSample.ReferencePts"/> is on the same clock as the video and a frame is matched by
    /// comparing the two.
    /// </summary>
    PresentationTimestamp,

    /// <summary>Asynchronous carriage: no PES timestamp.</summary>
    Timestamp,
}

/// <summary>One KLV packet as received, with what was decoded from it.</summary>
/// <param name="ReferencePts">
/// The packet's presentation time on the stream's reference clock, or null under asynchronous
/// carriage.
/// </param>
/// <param name="Fields">The minimum set, or null when the packet was not an ST 0601 local set at all.</param>
/// <param name="Raw">The packet exactly as it arrived, for whatever the fields leave out.</param>
public sealed record KlvSample(
    long? ReferencePts,
    KlvAlignment Alignment,
    DateTimeOffset ReceivedAt,
    Misb0601Set? Fields,
    byte[] Raw);
