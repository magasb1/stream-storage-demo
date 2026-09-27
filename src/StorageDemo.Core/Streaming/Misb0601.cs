using System.Buffers.Binary;
using System.Text;

namespace StorageDemo.Core.Streaming;

/// <summary>
/// The MISB ST 0902 Motion Imagery Sensor Minimum Metadata Set, decoded from one ST 0601 UAS
/// Datalink Local Set packet.
/// </summary>
public sealed record Misb0601Set
{
    /// <summary>Tag 2, the time every item in this packet describes.</summary>
    public DateTimeOffset? Timestamp { get; init; }

    public string? MissionId { get; init; }

    public double? PlatformHeading { get; init; }

    public double? PlatformPitch { get; init; }

    public double? PlatformRoll { get; init; }

    public string? PlatformDesignation { get; init; }

    public string? ImageSourceSensor { get; init; }

    public string? ImageCoordinateSystem { get; init; }

    public double? SensorLatitude { get; init; }

    public double? SensorLongitude { get; init; }

    public double? SensorTrueAltitude { get; init; }

    public double? SensorHorizontalFov { get; init; }

    public double? SensorVerticalFov { get; init; }

    public double? SensorRelativeAzimuth { get; init; }

    public double? SensorRelativeElevation { get; init; }

    public double? SensorRelativeRoll { get; init; }

    public double? SlantRange { get; init; }

    public double? FrameCenterLatitude { get; init; }

    public double? FrameCenterLongitude { get; init; }

    public double? FrameCenterElevation { get; init; }

    /// <summary>
    /// The ST 0102 classification marking from the nested security set, or null when the packet
    /// carried none.
    /// </summary>
    public string? Classification { get; init; }

    /// <summary>Tag 65, which revision of ST 0601 the sender wrote to.</summary>
    public int? Version { get; init; }

    /// <summary>Every item outside the minimum set, raw, keyed by ST 0601 tag.</summary>
    public IReadOnlyDictionary<int, byte[]> Unparsed { get; init; } = new Dictionary<int, byte[]>();
}

/// <summary>Decodes the ST 0902 minimum set out of an ST 0601 packet, and nothing more.</summary>
public static class Misb0601
{
    /// <summary>ST 0601.8-18: the UAS Datalink LS 16-byte universal key.</summary>
    public static ReadOnlySpan<byte> Key =>
    [
        0x06, 0x0E, 0x2B, 0x34, 0x02, 0x0B, 0x01, 0x01, 0x0E, 0x01, 0x03, 0x01, 0x01, 0x00, 0x00, 0x00,
    ];

    private static readonly string[] Classifications =
        ["UNCLASSIFIED", "RESTRICTED", "CONFIDENTIAL", "SECRET", "TOP SECRET"];

    /// <summary>Whether this packet is an ST 0601 local set at all, before any checksum is done.</summary>
    public static bool IsUasDatalink(ReadOnlySpan<byte> packet)
        => packet.Length > Key.Length && packet[..Key.Length].SequenceEqual(Key);

    /// <summary>Null when the packet is not a UAS Datalink LS, is malformed, or fails its checksum.</summary>
    public static Misb0601Set? Decode(ReadOnlySpan<byte> packet)
    {
        if (!IsUasDatalink(packet))
        {
            return null;
        }

        var offset = Key.Length;

        if (!ReadLength(packet, ref offset, out var length) || offset + length != packet.Length)
        {
            return null;
        }

        var set = new Misb0601Set();
        var unparsed = new Dictionary<int, byte[]>();
        var checksumSeen = false;

        while (offset < packet.Length)
        {
            if (!ReadTag(packet, ref offset, out var tag)
                || !ReadLength(packet, ref offset, out var valueLength)
                || offset + valueLength > packet.Length)
            {
                return null;
            }

            var value = packet.Slice(offset, valueLength);
            offset += valueLength;

            if (tag == 1)
            {
                // The sum runs from the first byte of the key through the checksum item's length
                // byte, i.e. everything before the checksum value; the checksum is the last item.
                if (offset != packet.Length || valueLength != 2
                    || Checksum(packet[..(packet.Length - 2)]) != BinaryPrimitives.ReadUInt16BigEndian(value))
                {
                    return null;
                }

                checksumSeen = true;
                continue;
            }

            set = tag switch
            {
                2 when value.Length == 8 => set with
                {
                    Timestamp = DateTimeOffset.UnixEpoch.AddTicks((long)(BinaryPrimitives.ReadUInt64BigEndian(value) * 10)),
                },
                3 => set with { MissionId = Text(value) },
                5 => set with { PlatformHeading = U16(value, 0, 360) },
                6 => set with { PlatformPitch = S16(value, 20) },
                7 => set with { PlatformRoll = S16(value, 50) },
                10 => set with { PlatformDesignation = Text(value) },
                11 => set with { ImageSourceSensor = Text(value) },
                12 => set with { ImageCoordinateSystem = Text(value) },
                13 => set with { SensorLatitude = S32(value, 90) },
                14 => set with { SensorLongitude = S32(value, 180) },
                15 => set with { SensorTrueAltitude = U16(value, -900, 19000) },
                16 => set with { SensorHorizontalFov = U16(value, 0, 180) },
                17 => set with { SensorVerticalFov = U16(value, 0, 180) },
                18 => set with { SensorRelativeAzimuth = U32(value, 0, 360) },
                19 => set with { SensorRelativeElevation = S32(value, 180) },
                20 => set with { SensorRelativeRoll = U32(value, 0, 360) },
                21 => set with { SlantRange = U32(value, 0, 5_000_000) },
                23 => set with { FrameCenterLatitude = S32(value, 90) },
                24 => set with { FrameCenterLongitude = S32(value, 180) },
                25 => set with { FrameCenterElevation = U16(value, -900, 19000) },
                48 => set with { Classification = ClassificationOf(value) },
                65 when value.Length == 1 => set with { Version = value[0] },
                _ => Keep(set, unparsed, tag, value),
            };
        }

        return checksumSeen ? set with { Unparsed = unparsed } : null;
    }

    /// <summary>
    /// ST 0601.8 section 6.8: a running 16-bit big-endian word sum, odd trailing byte in the high
    /// half.
    /// </summary>
    public static ushort Checksum(ReadOnlySpan<byte> bytes)
    {
        ushort sum = 0;

        for (var i = 0; i < bytes.Length; i++)
        {
            sum += (ushort)(bytes[i] << (8 * ((i + 1) % 2)));
        }

        return sum;
    }

    private static Misb0601Set Keep(Misb0601Set set, Dictionary<int, byte[]> unparsed, int tag, ReadOnlySpan<byte> value)
    {
        unparsed[tag] = value.ToArray();
        return set;
    }

    /// <summary>
    /// Only the classification is read out of the ST 0102 local set; the rest of the security items
    /// ride along raw inside the packet.
    /// </summary>
    private static string? ClassificationOf(ReadOnlySpan<byte> security)
    {
        var offset = 0;

        while (offset < security.Length)
        {
            if (!ReadTag(security, ref offset, out var tag)
                || !ReadLength(security, ref offset, out var length)
                || offset + length > security.Length)
            {
                return null;
            }

            if (tag == 1 && length == 1)
            {
                var code = security[offset];

                return code is >= 1 and <= 5 ? Classifications[code - 1] : $"UNKNOWN ({code})";
            }

            offset += length;
        }

        return null;
    }

    private static string Text(ReadOnlySpan<byte> value) => Encoding.ASCII.GetString(value);

    private static double? U16(ReadOnlySpan<byte> value, double min, double max)
        => value.Length == 2 ? min + BinaryPrimitives.ReadUInt16BigEndian(value) * (max - min) / ushort.MaxValue : null;

    private static double? U32(ReadOnlySpan<byte> value, double min, double max)
        => value.Length == 4 ? min + BinaryPrimitives.ReadUInt32BigEndian(value) * (max - min) / uint.MaxValue : null;

    /// <summary>Map -(2^15-1)..(2^15-1) to ±range; -2^15 is the standard's error indicator.</summary>
    private static double? S16(ReadOnlySpan<byte> value, double range)
        => value.Length == 2 && BinaryPrimitives.ReadInt16BigEndian(value) is var raw && raw != short.MinValue
            ? raw * range / short.MaxValue
            : null;

    /// <summary>Map -(2^31-1)..(2^31-1) to ±range; -2^31 is the standard's error indicator.</summary>
    private static double? S32(ReadOnlySpan<byte> value, double range)
        => value.Length == 4 && BinaryPrimitives.ReadInt32BigEndian(value) is var raw && raw != int.MinValue
            ? raw * range / int.MaxValue
            : null;

    /// <summary>BER-OID: seven bits per byte, high bit set on every byte but the last.</summary>
    private static bool ReadTag(ReadOnlySpan<byte> bytes, ref int offset, out int tag)
    {
        tag = 0;

        for (var i = 0; i < 4 && offset < bytes.Length; i++)
        {
            var b = bytes[offset++];
            tag = (tag << 7) | (b & 0x7F);

            if ((b & 0x80) == 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>BER length: one byte below 128, otherwise a count of the length bytes that follow.</summary>
    private static bool ReadLength(ReadOnlySpan<byte> bytes, ref int offset, out int length)
    {
        length = 0;

        if (offset >= bytes.Length)
        {
            return false;
        }

        var first = bytes[offset++];

        if ((first & 0x80) == 0)
        {
            length = first;
            return true;
        }

        var count = first & 0x7F;

        if (count is 0 or > 4 || offset + count > bytes.Length)
        {
            return false;
        }

        for (var i = 0; i < count; i++)
        {
            length = (length << 8) | bytes[offset++];
        }

        return length >= 0;
    }
}
