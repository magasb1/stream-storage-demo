namespace StorageDemo.Core.Streaming;

/// <summary>
/// How a KLV packet is tied to the pictures, per MISB ST 1402.2's two carriage modes.
/// </summary>
public enum KlvAlignment
{
    /// <summary>
    /// Synchronous carriage: the packet's PES carried a presentation timestamp, so
    /// <see cref="KlvSample.ReferencePts"/> is on the same clock as the video and a frame is
    /// matched by comparing the two.
    /// </summary>
    PresentationTimestamp,

    /// <summary>
    /// Asynchronous carriage: no PES timestamp. The only clock is ST 0601 tag 2, and a frame is
    /// matched against its own ST 0604 or ST 0603 time. Less trustworthy, and a worker geo-locating
    /// a detection should know which of the two it got.
    /// </summary>
    Timestamp,
}

/// <summary>One KLV packet as received, with what was decoded from it.</summary>
/// <param name="ReferencePts">
/// The packet's presentation time on the stream's reference clock, or null under asynchronous
/// carriage.
/// </param>
/// <param name="Fields">
/// The minimum set, or null when the packet was not an ST 0601 local set at all. A packet that
/// was one but failed its checksum is not kept.
/// </param>
/// <param name="Raw">The packet exactly as it arrived, for whatever the fields leave out.</param>
/// <param name="Synthesised">
/// True when this service built the packet from a fixed camera's configured position rather than
/// receiving it. The out-of-band half of provenance: a client says "configured position" rather
/// than "reported position" from this, without parsing a string.
///
/// The in-band half is ST 0601 tag 10, which the same packet carries and which survives to any
/// conforming consumer downstream of this service; see <c>StaticSensorPublisher.Designation</c>.
/// Both, because a flag on this service's own API is lost at the first hop and a string in the
/// packet is not something a client should have to match on.
/// </param>
public sealed record KlvSample(
    long? ReferencePts,
    KlvAlignment Alignment,
    DateTimeOffset ReceivedAt,
    Misb0601Set? Fields,
    byte[] Raw,
    bool Synthesised = false);
