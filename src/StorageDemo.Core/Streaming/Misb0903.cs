using System.Buffers.Binary;
using System.Text;

namespace StorageDemo.Core.Streaming;

/// <summary>
/// One detection in one frame, in the terms a detector actually produces: an identifier, a bounding
/// box in pixels, and optionally how sure it is and what it thinks the thing is.
/// </summary>
/// <param name="Id">VTarget Pack Target ID Number, 1 to 2,097,151.</param>
/// <param name="ConfidencePercent">VTarget Pack tag 5, 0 to 100.</param>
/// <param name="OntologyClass">
/// VObject LS tag 2, the class name as it appears in the ontology named by <see
/// cref="VmtiFrame.Ontology"/>.
/// </param>
/// <param name="Track">
/// VTarget Pack tag 104, the VTracker LS: present when a tracker, not just a detector, produced
/// this box.
/// </param>
public sealed record VmtiDetection(
    int Id,
    int Left,
    int Top,
    int Right,
    int Bottom,
    int? ConfidencePercent = null,
    string? OntologyClass = null,
    VmtiTrack? Track = null);

/// <summary>
/// ST 0903.4 Table 16, the VTracker LS Detection Status values, numbered as the standard numbers
/// them so the enum casts straight onto the wire.
/// </summary>
public enum VmtiTrackStatus
{
    /// <summary>Detections have ended: merged, split, or nothing correlates any more.</summary>
    Inactive = 0,

    /// <summary>Established or updated from a VMTI report or a prediction.</summary>
    Active = 1,

    /// <summary>Uncorrelated for longer than a threshold, "lost", but may still resume.</summary>
    Dropped = 2,

    /// <summary>Stationary, or always was.</summary>
    Stopped = 3,
}

/// <summary>
/// What a track adds to a detection, in the terms of ST 0903.4 Table 6 (VTracker LS): who the track
/// is, what state it is in, and when it was first and last actually observed.
/// </summary>
/// <param name="Id">Tag 1, Track ID.</param>
/// <param name="Started">Tag 3, first observation, microseconds (ST 0603 clock).</param>
/// <param name="LastSeen">Tag 4, the most recent observation.</param>
/// <param name="Algorithm">Tag 6, a name that identifies the tracker uniquely.</param>
/// <param name="ConfidencePercent">
/// Tag 7, 0 to 100: certainty that the sequence of detections is one object.
/// </param>
public sealed record VmtiTrack(
    Guid Id,
    VmtiTrackStatus Status,
    DateTimeOffset Started,
    DateTimeOffset LastSeen,
    string? Algorithm = null,
    int? ConfidencePercent = null);

/// <summary>
/// One frame's detections, with the frame-level facts a consumer needs to make sense of them.
/// </summary>
/// <param name="Timestamp">VMTI LS tag 2, the frame this describes.</param>
/// <param name="FrameWidth">VMTI LS tag 8.</param>
/// <param name="SourceSensor">VMTI LS tag 10, which imagery the detector ran on.</param>
/// <param name="Ontology">
/// VObject LS tag 1, the URI of the OWL ontology the class names come from (ST 0903.4-45).
/// </param>
public sealed record VmtiFrame(
    DateTimeOffset Timestamp,
    int FrameWidth,
    int FrameHeight,
    string SourceSensor,
    IReadOnlyList<VmtiDetection> Detections,
    string? Ontology = null);

/// <summary>
/// One VMTI frame as a worker posted it to the stream's owner and as the owner serves it: the typed
/// frame beside the packet it encodes to.
/// </summary>
public sealed record VmtiSample(VmtiFrame Frame, byte[] Raw);

/// <summary>
/// Which detection caused a capture: the three things that identify one uniquely on the wire, and
/// there is no fourth.
/// </summary>
public sealed record DetectionReference(string Stream, DateTimeOffset Timestamp, int TargetId)
{
    /// <summary>Where it lands, beside "Live stream", "Captured" and "Classification".</summary>
    public const string MetadataKey = "Detection";

    /// <summary>
    /// The whole reference as one line a person can read in a document's properties without knowing
    /// this format exists: stream, at a moment, target number.
    /// </summary>
    public override string ToString()
        => $"{Stream}@{Timestamp.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.ffffff}Z#{TargetId}";
}

/// <summary>
/// Encodes a standalone MISB ST 0903 VMTI Local Set: the detections from one frame, as a KLV packet
/// a STANAG 4609 consumer already knows how to read.
/// </summary>
public static class Misb0903
{
    /// <summary>ST 0903.4 Table 1: the VMTI LS 16-byte universal key.</summary>
    public static ReadOnlySpan<byte> Key =>
    [
        0x06, 0x0E, 0x2B, 0x34, 0x02, 0x0B, 0x01, 0x01, 0x0E, 0x01, 0x03, 0x03, 0x06, 0x00, 0x00, 0x00,
    ];

    /// <summary>The revision this encoder writes, sent as VMTI LS tag 4 (ST 0903.4 section 11.4).</summary>
    public const int Version = 4;

    /// <summary>The packet, ready to hand to a KLV carriage.</summary>
    public static byte[] Encode(VmtiFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        ArgumentOutOfRangeException.ThrowIfLessThan(frame.FrameWidth, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(frame.FrameHeight, 1);

        var body = new List<byte>();

        // ST 0903.4-14: when a Precision Time Stamp is present it is the first TLV in the set.
        Item(body, 2, Fixed(Microseconds(frame.Timestamp), 8));
        Item(body, 4, Variable(Version));

        // ST 0903.4-19: Number of Reported Targets is always specified, zero included.
        Item(body, 6, Variable((ulong)frame.Detections.Count));
        Item(body, 8, Variable((ulong)frame.FrameWidth));
        Item(body, 9, Variable((ulong)frame.FrameHeight));
        Item(body, 10, Encoding.UTF8.GetBytes(frame.SourceSensor));

        // ST 0903.4-10 requires at least one TLV after a VTarget Pack's id, so an empty frame says
        // "nothing detected" with tag 6 alone; Table 1 tag 5 agrees that no targets is expressed by
        // no value at all.
        if (frame.Detections.Count > 0)
        {
            Item(body, 101, Series(frame));
        }

        // ST 0903.4-16/17: the checksum is the last TLV and covers the key, the set's length, every
        // TLV before it and its own tag and length byte, but not its own value.
        var payload = new List<byte>(body) { 1, 2 };
        byte[] packet = [.. Key, .. Length(payload.Count + 2), .. payload, 0, 0];

        BinaryPrimitives.WriteUInt16BigEndian(
            packet.AsSpan(packet.Length - 2),
            // ST 0903.4 section 11.1 defers to ST 0601 for the algorithm, so this is the same sum.
            Misb0601.Checksum(packet.AsSpan(0, packet.Length - 2)));

        return packet;
    }

    /// <summary>
    /// ST 0903.4-06/07: a Series is a variable-length pack of same-typed elements, here VTarget
    /// Packs, each a BER length and a value, with no key and no count in front of them.
    /// </summary>
    private static byte[] Series(VmtiFrame frame)
    {
        var series = new List<byte>();

        foreach (var detection in frame.Detections)
        {
            var pack = Pack(frame, detection);
            series.AddRange(Length(pack.Length));
            series.AddRange(pack);
        }

        return [.. series];
    }

    private static byte[] Pack(VmtiFrame frame, VmtiDetection detection)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(detection.Id, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(detection.Id, 2_097_151);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(detection.Left, detection.Right);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(detection.Top, detection.Bottom);

        // ST 0903.4-09: the target id comes first, BER-OID encoded, with no tag and no length.
        var pack = new List<byte>(Oid(detection.Id));

        // ST 0903.4-29: a centroid must be present.
        Item(pack, 1, Variable(Pixel(frame, (detection.Left + detection.Right) / 2, (detection.Top + detection.Bottom) / 2)));
        Item(pack, 2, Variable(Pixel(frame, detection.Left, detection.Top)));
        Item(pack, 3, Variable(Pixel(frame, detection.Right, detection.Bottom)));

        if (detection.ConfidencePercent is { } confidence)
        {
            // Table 2 tag 5: one byte, 0 to 100 as a percentage.
            ArgumentOutOfRangeException.ThrowIfNegative(confidence);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(confidence, 100);
            Item(pack, 5, [(byte)confidence]);
        }

        if (detection.OntologyClass is { } target)
        {
            Item(pack, 102, VObject(frame.Ontology, target));
        }

        if (detection.Track is { } track)
        {
            Item(pack, 104, VTracker(track));
        }

        return [.. pack];
    }

    /// <summary>Table 6: the VTracker LS, nested under VTarget Pack tag 104 (Table 2).</summary>
    private static byte[] VTracker(VmtiTrack track)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(track.LastSeen, track.Started);

        var set = new List<byte>();

        // Tag 1, F16, ST 0903.4-53: a UUID.
        Item(set, 1, track.Id.ToByteArray(bigEndian: true));

        // Tag 2, F1: Table 16's enumeration, whose numbering the enum reproduces.
        Item(set, 2, [(byte)track.Status]);

        // Tags 3 and 4 are V8 ("Variable up to 8 Bytes"), unlike the VMTI LS's own tag 2, which is
        // a fixed eight.
        Item(set, 3, Variable(Microseconds(track.Started)));
        Item(set, 4, Variable(Microseconds(track.LastSeen)));

        if (track.Algorithm is { } algorithm)
        {
            Item(set, 6, Encoding.UTF8.GetBytes(algorithm));
        }

        if (track.ConfidencePercent is { } confidence)
        {
            // Tag 7, one byte, 0 to 100 as a percentage, like the VTarget Pack's tag 5.
            ArgumentOutOfRangeException.ThrowIfNegative(confidence);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(confidence, 100);
            Item(set, 7, [(byte)confidence]);
        }

        // Tags 8 and 9 (track point count and locus) are left out together: ST 0903.4-55 wants a
        // count of at least one whenever it is present, and the section 11 note on tag 8 says the
        // count need not be specified at all, so absent is the only conforming option without a
        // geodetic locus.
        return [.. set];
    }

    /// <summary>Table 4: the VObject LS, which is where ST 0903 puts what a target is.</summary>
    private static byte[] VObject(string? ontology, string target)
    {
        var set = new List<byte>();

        if (ontology is not null)
        {
            Item(set, 1, Encoding.UTF8.GetBytes(ontology));
        }

        Item(set, 2, Encoding.UTF8.GetBytes(target));

        return [.. set];
    }

    /// <summary>
    /// ST 0903.4 section 11.15 Tag 1: pixel number is Column + (Row - 1) x Frame Width, counting
    /// from 1 at the top left, row-major.
    /// </summary>
    private static ulong Pixel(VmtiFrame frame, int column, int row)
    {
        if (column < 1 || column > frame.FrameWidth || row < 1 || row > frame.FrameHeight)
        {
            throw new ArgumentOutOfRangeException(
                nameof(frame),
                $"pixel ({column}, {row}) is outside a {frame.FrameWidth}x{frame.FrameHeight} frame");
        }

        return (ulong)column + ((ulong)(row - 1) * (ulong)frame.FrameWidth);
    }

    private static ulong Microseconds(DateTimeOffset timestamp)
        => (ulong)(timestamp.UtcDateTime - DateTime.UnixEpoch).Ticks / 10;

    private static void Item(List<byte> into, int tag, ReadOnlySpan<byte> value)
    {
        // Every tag this encoder writes is below 128, which BER-OID encodes as the byte itself.
        into.Add((byte)tag);
        into.AddRange(Length(value.Length));
        into.AddRange(value);
    }

    /// <summary>BER length: one byte below 128, otherwise a count of the length bytes that follow.</summary>
    private static byte[] Length(int length)
    {
        if (length < 128)
        {
            return [(byte)length];
        }

        var bytes = Variable((ulong)length);

        return [(byte)(0x80 | bytes.Length), .. bytes];
    }

    /// <summary>
    /// ST 0903.4 section 8.3, the "Vmax" formats: the fewest bytes that hold the value, leading
    /// zeroes dropped, and one byte for zero (ST 0903.4-05).
    /// </summary>
    private static byte[] Variable(ulong value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);

        var first = 0;

        while (first < 7 && bytes[first] == 0)
        {
            first++;
        }

        return bytes[first..];
    }

    /// <summary>The "Fn" formats, which unlike "Vn" are always the full width.</summary>
    private static byte[] Fixed(ulong value, int length)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);

        return bytes[(8 - length)..];
    }

    /// <summary>BER-OID: seven bits per byte, high bit set on every byte but the last.</summary>
    private static byte[] Oid(int value)
    {
        var bytes = new List<byte>();

        do
        {
            bytes.Insert(0, (byte)(value & 0x7F));
            value >>= 7;
        }
        while (value > 0);

        for (var i = 0; i < bytes.Count - 1; i++)
        {
            bytes[i] |= 0x80;
        }

        return [.. bytes];
    }
}
