using System.Buffers.Binary;
using System.Text;

namespace StorageDemo.Core.Streaming;

/// <summary>
/// The MISB ST 0902 Motion Imagery Sensor Minimum Metadata Set, decoded from one ST 0601 UAS
/// Datalink Local Set packet. Everything the packet carries beyond that stays raw in
/// <see cref="Unparsed"/>, keyed by tag.
///
/// Angles are degrees, distances metres, latitude and longitude WGS84. A field is null when the
/// packet did not carry it, carried it at the wrong length, or sent the standard's error value.
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
    /// carried none. Null is a meaningful answer: a client shows it as unmarked, which is not the
    /// same as an empty marking.
    /// </summary>
    public string? Classification { get; init; }

    /// <summary>Tag 65, which revision of ST 0601 the sender wrote to.</summary>
    public int? Version { get; init; }

    /// <summary>Every item outside the minimum set, raw, keyed by ST 0601 tag.</summary>
    public IReadOnlyDictionary<int, byte[]> Unparsed { get; init; } = new Dictionary<int, byte[]>();
}

/// <summary>
/// Decodes the ST 0902 minimum set out of an ST 0601 packet, and encodes the fixed subset of it a
/// static sensor's configuration amounts to.
///
/// The two halves share one scale table, which is the point of them being in one file: the encoder
/// inverts what the decoder reads, so the round trip in Misb0601Tests holds the encoder to a table
/// already checked against a real sender's arithmetic in Misb0601RealStreamTests. A second table
/// somewhere else would be the copy that rots.
///
/// Sources, so the scaling can be checked against a document rather than against this file:
/// - STANAG 4609 Ed. 5 adopts MISP-2019.1, whose normative references are MISB ST 0601.14 (UAS
///   Datalink LS), ST 0902.8 (Minimum Metadata Set), ST 0102.12 (Security LS), ST 1402.2 (KLV in
///   MPEG-2 TS) and ST 1201.3 (IMAPB).
/// - Every scale below was read from ST 0601.8 Table 1, which is the latest revision published
///   outside the NGA registry. ST 0601 has never changed the encoding of an existing item, so
///   these hold for 0601.14; none of the minimum set uses ST 1201 IMAPB, which 0601 adopted only
///   for items added later. The checksum is ST 0601.8 section 6.8.
/// - ST 0902.8 itself sits behind the NGA registry's CAPTCHA and could not be fetched, so the
///   membership of the set was taken from the project owner. Esri's public FMV sample stream
///   carries every one of these items and nothing is missing from it, which corroborates the
///   membership without proving it; see Misb0601RealStreamTests.
/// - Every scale here is checked against that stream: over its 711 packets the slant range agrees
///   with the distance computed from the sensor and frame centre positions and elevations to
///   within four metres, which it cannot do if any of those five scales is wrong.
/// - ST 0102 tag numbers and classification codes are as implemented by jmisb (WestRidgeSystems),
///   which tracks ST 0102.12: local set tag 1, one byte, 1 UNCLASSIFIED through 5 TOP SECRET.
///
/// ponytail: the full ST 0601 table is about 140 items. Adding one is a case in <see cref="Decode"/>
/// and a property; a table-driven parser is the upgrade if that ever becomes routine.
/// </summary>
public static class Misb0601
{
    /// <summary>ST 0601.8-18: the UAS Datalink LS 16-byte universal key.</summary>
    public static ReadOnlySpan<byte> Key =>
    [
        0x06, 0x0E, 0x2B, 0x34, 0x02, 0x0B, 0x01, 0x01, 0x0E, 0x01, 0x03, 0x01, 0x01, 0x00, 0x00, 0x00,
    ];

    /// <summary>
    /// ST 0102 local set tag 1, codes 1 to 5, in that order. Public because a marking is also
    /// something an operator configures and something this file encodes, and a second copy of this
    /// list somewhere else would be the copy that rots.
    /// </summary>
    public static readonly IReadOnlyList<string> Classifications =
        ["UNCLASSIFIED", "RESTRICTED", "CONFIDENTIAL", "SECRET", "TOP SECRET"];

    /// <summary>Whether this packet is an ST 0601 local set at all, before any checksum is done.</summary>
    public static bool IsUasDatalink(ReadOnlySpan<byte> packet)
        => packet.Length > Key.Length && packet[..Key.Length].SequenceEqual(Key);

    /// <summary>
    /// Null when the packet is not a UAS Datalink LS, is malformed, or fails its checksum. ST
    /// 0601.8-08 says a packet whose checksum does not match is discarded, and a discarded packet
    /// is better than a platform placed a hemisphere away by a flipped bit.
    /// </summary>
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
    /// A ST 0601 Local Set describing a fixed camera, built from configuration rather than
    /// telemetry: the packet a static sensor would send if it had anything to send with.
    ///
    /// What it carries and why, item by item. Every scale is <see cref="Decode"/>'s, inverted -
    /// which is what makes the round-trip test in Misb0601Tests worth something, since those scales
    /// are already checked against a real stream in Misb0601RealStreamTests.
    /// - Tag 2, precision time stamp, first in the set as ST 0601.8-06 requires.
    /// - Tags 5 and 18, platform heading and sensor relative azimuth, both present. Eighteen is
    ///   emitted although it is zero, and that is not tidiness: <see cref="SensorGeometry.SensorBearing"/>
    ///   is the sum of the two and returns null if either is absent, so a set without tag 18 would
    ///   carry a bearing no consumer could read.
    /// - Tag 19, sensor relative elevation, the depression angle.
    /// - Tags 13, 14, 15, where the sensor is.
    /// - Tags 16 and 17, the field of view, which is what lets a client draw a wedge.
    /// - Tag 10, platform designation, carrying <paramref name="designation"/>. This is the
    ///   in-band half of saying the set was synthesised: <see cref="Misb0601Set.PlatformDesignation"/>
    ///   already decodes it, so it survives the round trip to any conforming consumer rather than
    ///   only to this service's own clients.
    /// - Tag 48, the ST 0102 security set, only when a marking was configured.
    /// - Tag 65, the version, last before the checksum.
    /// - Tag 1, the checksum, last, per ST 0601.8-08.
    ///
    /// Deliberately absent:
    /// - Tag 20, sensor relative roll. Absent is read as unrolled by
    ///   <see cref="SensorGeometry.NorthInImage(double?, double?)"/>, which is correct for a fixed
    ///   mount, and sending zero would claim a measurement nobody made.
    /// - Tags 21, 23, 24 and 25, slant range and frame centre. Computing where a camera looks at
    ///   on the ground needs a range or a terrain model, and this service has neither. A guessed
    ///   frame centre is worse than none: it is indistinguishable from a measured one, and
    ///   geolocation work built on top of it would be built on an invention.
    /// </summary>
    /// <param name="timestamp">Tag 2. The time the set describes, which for a static sensor is simply now.</param>
    /// <param name="designation">
    /// Tag 10. Short, and it is what a consumer reads to tell configured position from reported
    /// position.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown rather than saturated, for the same reason <see cref="Misb0903.Encode"/> throws: a
    /// value outside an item's range is a configuration that was never refused, and a packet
    /// carrying the range's endpoint instead would look exactly like a real measurement.
    /// </exception>
    public static byte[] Encode(StaticSensor sensor, DateTimeOffset timestamp, string designation)
    {
        ArgumentNullException.ThrowIfNull(sensor);
        ArgumentException.ThrowIfNullOrEmpty(designation);

        var body = new List<byte>();

        // ST 0601.8-06: the precision time stamp is the first item in the set.
        Item(body, 2, Microseconds(timestamp));

        Item(body, 5, U16(sensor.TrueBearing, 0, 360));
        Item(body, 10, Encoding.ASCII.GetBytes(designation));
        Item(body, 13, S32(sensor.Latitude, 90));
        Item(body, 14, S32(sensor.Longitude, 180));
        Item(body, 15, U16(sensor.AltitudeMetres, -900, 19_000));
        Item(body, 16, U16(sensor.HorizontalFov, 0, 180));
        Item(body, 17, U16(sensor.VerticalFov, 0, 180));

        // Zero, and sent. See the note on tags 5 and 18 above: absent, it would cost the set its
        // bearing.
        Item(body, 18, U32(0, 0, 360));

        Item(body, 19, S32(sensor.Depression, 180));

        if (sensor.Classification is { Length: > 0 } marking)
        {
            Item(body, 48, Security(marking));
        }

        Item(body, 65, [Version]);

        // ST 0601.8-08: the checksum is the last item and covers the key, the set length, every
        // item before it and its own tag and length byte, but not its own value - the same shape
        // Misb0903.Encode writes, which defers to this standard for the algorithm.
        var payload = new List<byte>(body) { 1, 2 };
        byte[] packet = [.. Key, .. Length(payload.Count + 2), .. payload, 0, 0];

        BinaryPrimitives.WriteUInt16BigEndian(
            packet.AsSpan(packet.Length - 2),
            Checksum(packet.AsSpan(0, packet.Length - 2)));

        return packet;
    }

    /// <summary>
    /// The revision this encoder writes, sent as tag 65. Sixteen is ST 0601.16; the items above are
    /// unchanged since .8, whose table this file's scales were read from, and ST 0601 has never
    /// changed the encoding of an existing item.
    /// </summary>
    public const byte Version = 16;

    /// <summary>
    /// The ST 0102 Security Local Set for tag 48, carrying the marking and nothing else.
    ///
    /// Only tag 1. A full ST 0102 set also carries a classifying country and a releasing
    /// instruction, and this service is told neither: emitting a country because the set expects
    /// one would be inventing the very field an operator would read to decide what may be shared.
    /// Tag 1 alone is what <see cref="Decode"/> reads, and what a consumer needs to show a marking.
    /// </summary>
    private static byte[] Security(string marking)
    {
        var code = 0;

        for (var index = 0; index < Classifications.Count; index++)
        {
            if (string.Equals(Classifications[index], marking, StringComparison.OrdinalIgnoreCase))
            {
                code = index + 1;
            }
        }

        if (code == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(marking),
                marking,
                $"Not an ST 0102 classification. One of: {string.Join(", ", Classifications)}.");
        }

        var set = new List<byte>();
        Item(set, 1, [(byte)code]);

        return [.. set];
    }

    private static byte[] Microseconds(DateTimeOffset timestamp)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(
            bytes,
            (ulong)(timestamp.UtcDateTime - DateTime.UnixEpoch).Ticks / 10);

        return bytes;
    }

    /// <summary>The inverse of <see cref="U16(ReadOnlySpan{byte}, double, double)"/>.</summary>
    private static byte[] U16(double value, double min, double max)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(value, min);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value, max);

        var bytes = new byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, (ushort)Math.Round((value - min) / (max - min) * ushort.MaxValue));

        return bytes;
    }

    /// <summary>The inverse of <see cref="U32(ReadOnlySpan{byte}, double, double)"/>.</summary>
    private static byte[] U32(double value, double min, double max)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(value, min);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value, max);

        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)Math.Round((value - min) / (max - min) * uint.MaxValue));

        return bytes;
    }

    /// <summary>
    /// The inverse of <see cref="S32(ReadOnlySpan{byte}, double)"/>. The range maps onto
    /// -(2^31-1)..(2^31-1), leaving -2^31 free as the standard's error indicator, which this
    /// encoder never writes: a value it cannot represent is a refusal, not an error code.
    /// </summary>
    private static byte[] S32(double value, double range)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(value, -range);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value, range);

        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, (int)Math.Round(value / range * int.MaxValue));

        return bytes;
    }

    /// <summary>
    /// One TLV. Every tag this encoder writes is below 128, which BER-OID encodes as the byte
    /// itself, and every value is short, which BER encodes as one length byte.
    ///
    /// ponytail: this and <see cref="Length"/> are the same two functions <see cref="Misb0903"/>
    /// keeps privately, which is two copies of BER. They are eight lines each and the two encoders
    /// are otherwise independent, so they are left alone rather than pulled into a shared KLV
    /// primitives type; that is the change to make if a third encoder ever lands.
    /// </summary>
    private static void Item(List<byte> into, int tag, ReadOnlySpan<byte> value)
    {
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

        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)length);

        var first = 0;

        while (first < 3 && bytes[first] == 0)
        {
            first++;
        }

        return [(byte)(0x80 | (4 - first)), .. bytes[first..]];
    }

    /// <summary>ST 0601.8 section 6.8: a running 16-bit big-endian word sum, odd trailing byte in the high half.</summary>
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
    /// Only the classification is read out of the ST 0102 local set; the rest of the security
    /// items ride along raw inside the packet. An unknown code is reported as such rather than
    /// dropped, because a marking a client cannot show is not the same as no marking.
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
