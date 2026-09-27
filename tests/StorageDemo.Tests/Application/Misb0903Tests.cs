using System.Buffers.Binary;
using StorageDemo.Core.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Application;

/// <summary>The VMTI encoder against MISB ST 0903.4, 23 October 2014.</summary>
public sealed class Misb0903Tests
{
    private const string Sensor = "EO Nose";
    private const string Ontology = "https://raw.githubusercontent.com/example/detector/main/coco.owl";

    /// <summary>
    /// Odd microseconds on purpose: ST 0903.4 tag 2 is a microsecond clock, not a millisecond one,
    /// and a truncating conversion would round this off.
    /// </summary>
    private static readonly DateTimeOffset Timestamp =
        new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero).AddTicks(1_234_560);

    private static VmtiFrame Frame(params VmtiDetection[] detections)
        => new(Timestamp, 1920, 1080, Sensor, detections, Ontology);

    [Fact]
    public void A_frame_of_detections_survives_the_round_trip()
    {
        var packet = Misb0903.Encode(Frame(
            new VmtiDetection(7, 100, 200, 140, 260, ConfidencePercent: 93, OntologyClass: "Vehicle"),
            new VmtiDetection(2_097_151, 1, 1, 3, 5),
            new VmtiDetection(31, 1877, 1041, 1920, 1080, ConfidencePercent: 4, OntologyClass: "Person")));

        var decoded = Vmti.Decode(packet);

        Assert.Equal(
            Timestamp,
            DateTimeOffset.UnixEpoch.AddTicks((long)Vmti.Integer(decoded.Items[2]) * 10));
        Assert.Equal((ulong)Misb0903.Version, Vmti.Integer(decoded.Items[4]));
        Assert.Equal(3UL, Vmti.Integer(decoded.Items[6]));
        Assert.Equal(1920UL, Vmti.Integer(decoded.Items[8]));
        Assert.Equal(1080UL, Vmti.Integer(decoded.Items[9]));
        Assert.Equal(Sensor, Vmti.Text(decoded.Items[10]));

        // A target id of 2,097,151 is three BER-OID bytes, and the corner boxes are where an
        // off-by-one in the one-based pixel numbering shows up rather than hiding mid-frame.
        Assert.Equal(new[] { 7, 2_097_151, 31 }, decoded.Targets.Select(t => t.Id));

        var (first, small, corner) = (decoded.Targets[0], decoded.Targets[1], decoded.Targets[2]);

        Assert.Equal((120, 230), Pixel(first, 1));
        Assert.Equal((100, 200), Pixel(first, 2));
        Assert.Equal((140, 260), Pixel(first, 3));
        Assert.Equal(93UL, Vmti.Integer(first.Items[5]));

        Assert.Equal((2, 3), Pixel(small, 1));
        Assert.Equal((1, 1), Pixel(small, 2));
        Assert.Equal((3, 5), Pixel(small, 3));

        Assert.Equal((1920, 1080), Pixel(corner, 3));
        Assert.Equal(4UL, Vmti.Integer(corner.Items[5]));

        // Table 4: what the target is lives in a VObject LS, class named out of a named ontology.
        var vobject = Vmti.Nested(first.Items[102]);

        Assert.Equal(Ontology, Vmti.Text(vobject[1]));
        Assert.Equal("Vehicle", Vmti.Text(vobject[2]));
        Assert.Equal("Person", Vmti.Text(Vmti.Nested(corner.Items[102])[2]));

        // A box with no class and no confidence carries neither, rather than a zero that reads as
        // "the detector is certain this is nothing".
        Assert.DoesNotContain(5, small.Items.Keys);
        Assert.DoesNotContain(102, small.Items.Keys);
    }

    [Fact]
    public void The_checksum_verifies_by_the_algorithm_the_standard_specifies()
    {
        var packet = Misb0903.Encode(Frame(new VmtiDetection(1, 10, 20, 30, 40)));
        var decoded = Vmti.Decode(packet);

        // ST 0903.4-17: the checksum is the last TLV, so its two bytes end the packet, and ST
        // 0903.4-16 covers everything before them.
        Assert.Equal(new byte[] { 0x01, 0x02 }, packet.AsSpan(packet.Length - 4, 2).ToArray());
        Assert.Equal(
            Vmti.Checksum(packet.AsSpan(0, packet.Length - 2)),
            BinaryPrimitives.ReadUInt16BigEndian(decoded.Items[1]));
    }

    [Fact]
    public void A_flipped_bit_anywhere_in_the_packet_breaks_the_checksum()
    {
        var packet = Misb0903.Encode(Frame(new VmtiDetection(1, 10, 20, 30, 40)));
        var checksum = BinaryPrimitives.ReadUInt16BigEndian(packet.AsSpan(packet.Length - 2));

        // The top left corner of the box, which is the kind of error the checksum exists to catch.
        packet[^7] ^= 0x01;

        Assert.NotEqual(checksum, Vmti.Checksum(packet.AsSpan(0, packet.Length - 2)));
    }

    [Fact]
    public void The_key_is_the_universal_label_the_standard_defines()
    {
        // ST 0903.4 Table 1, typed out here rather than compared to the encoder's own copy.
        byte[] key =
        [
            0x06, 0x0E, 0x2B, 0x34, 0x02, 0x0B, 0x01, 0x01, 0x0E, 0x01, 0x03, 0x03, 0x06, 0x00, 0x00, 0x00,
        ];

        Assert.Equal(key, Misb0903.Encode(Frame()).AsSpan(0, 16).ToArray());
    }

    [Fact]
    public void A_length_that_will_not_fit_in_one_byte_is_written_in_the_long_form()
    {
        // Forty targets is far past 127 bytes for both the VTargetSeries and the set itself, so
        // both lengths take the long form, and every target still has to come back out.
        var detections = Enumerable.Range(1, 40)
            .Select(i => new VmtiDetection(i, i, i, i + 20, i + 20, ConfidencePercent: i % 101))
            .ToArray();

        var packet = Misb0903.Encode(Frame(detections));

        Assert.Equal(0x82, (int)packet[16]);
        Assert.Equal(packet.Length, 16 + 3 + BinaryPrimitives.ReadUInt16BigEndian(packet.AsSpan(17)));

        var decoded = Vmti.Decode(packet);

        Assert.Equal(40, decoded.Targets.Count);
        Assert.Equal(40UL, Vmti.Integer(decoded.Items[6]));
        Assert.Equal(detections.Select(d => d.Id), decoded.Targets.Select(t => t.Id));
        Assert.All(decoded.Targets, t => Assert.Equal(20, Pixel(t, 3).Column - Pixel(t, 2).Column));
    }

    [Fact]
    public void A_frame_with_no_detections_is_a_valid_packet()
    {
        var decoded = Vmti.Decode(Misb0903.Encode(Frame()));

        // ST 0903.4-19: the count is always there.
        Assert.Equal(new byte[] { 0x00 }, decoded.Items[6]);
        Assert.DoesNotContain(101, decoded.Items.Keys);
        Assert.Empty(decoded.Targets);
    }

    [Fact]
    public void A_realistic_frame_is_the_size_a_real_system_would_send()
    {
        var packet = Misb0903.Encode(Frame(
            new VmtiDetection(1, 220, 470, 356, 604, ConfidencePercent: 91, OntologyClass: "Car"),
            new VmtiDetection(2, 764, 402, 830, 560, ConfidencePercent: 84, OntologyClass: "Person"),
            new VmtiDetection(3, 1102, 388, 1290, 522, ConfidencePercent: 77, OntologyClass: "Truck"),
            new VmtiDetection(4, 1455, 612, 1512, 700, ConfidencePercent: 62, OntologyClass: "Person"),
            new VmtiDetection(5, 88, 900, 214, 1010, ConfidencePercent: 55, OntologyClass: "Car")));

        // An ST 0601 minimum-set packet is about 120 bytes, and five detections carrying a 63-byte
        // ontology URI apiece should cost a few hundred more, not tens and not thousands.
        Assert.InRange(Misb0903.Encode(Frame()).Length, 40, 100);
        Assert.InRange(packet.Length, 400, 1000);
        Assert.Equal(5, Vmti.Decode(packet).Targets.Count);
    }

    [Fact]
    public void A_box_outside_the_frame_is_refused_rather_than_encoded()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => Misb0903.Encode(Frame(new VmtiDetection(1, 1900, 1000, 1930, 1060))));

        // Zero-based pixel coordinates are the likeliest way to arrive here, and the standard
        // numbers from 1, so a zero is wrong rather than merely unusual.
        Assert.Throws<ArgumentOutOfRangeException>(
            () => Misb0903.Encode(Frame(new VmtiDetection(1, 0, 0, 30, 40))));
    }

    [Fact]
    public void A_vtracker_local_set_survives_the_round_trip()
    {
        var id = Guid.Parse("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"); // the section 11 example UUID
        var started = Timestamp.AddSeconds(-12.5);
        var full = new VmtiTrack(id, VmtiTrackStatus.Dropped, started, Timestamp, "ByteTrack", 70);
        var bare = new VmtiTrack(Guid.NewGuid(), VmtiTrackStatus.Active, Timestamp, Timestamp);

        var decoded = Vmti.Decode(Misb0903.Encode(Frame(
            new VmtiDetection(7, 100, 200, 140, 260, Track: full),
            new VmtiDetection(8, 300, 200, 340, 260, Track: bare),
            new VmtiDetection(9, 500, 200, 540, 260))));

        var vtracker = Vmti.Nested(decoded.Targets[0].Items[104]);

        // Table 6 tag 1 is F16 in RFC 4122 byte order: the standard's example spells that UUID as
        // F8 1D 4F AE 7D EC 11 D0 ..., and a little-endian Guid would come back as a different id.
        Assert.Equal(id, Vmti.Uuid(vtracker[1]));
        Assert.Equal(new byte[] { 0xF8, 0x1D, 0x4F, 0xAE }, vtracker[1][..4]);
        Assert.Equal(2UL, Vmti.Integer(vtracker[2])); // Table 16: Dropped
        Assert.Equal(started, DateTimeOffset.UnixEpoch.AddTicks((long)Vmti.Integer(vtracker[3]) * 10));
        Assert.Equal(Timestamp, DateTimeOffset.UnixEpoch.AddTicks((long)Vmti.Integer(vtracker[4]) * 10));
        Assert.Equal("ByteTrack", Vmti.Text(vtracker[6]));
        Assert.Equal(70UL, Vmti.Integer(vtracker[7]));
        Assert.DoesNotContain(8, vtracker.Keys); // no geodetic locus, so no track point count

        var minimal = Vmti.Nested(decoded.Targets[1].Items[104]);

        Assert.Equal(bare.Id, Vmti.Uuid(minimal[1]));
        Assert.Equal([1, 2, 3, 4], minimal.Keys.Order());
        Assert.DoesNotContain(104, decoded.Targets[2].Items.Keys);
    }

    [Fact]
    public void A_track_last_seen_before_it_started_is_refused()
    {
        var track = new VmtiTrack(Guid.NewGuid(), VmtiTrackStatus.Active, Timestamp, Timestamp.AddSeconds(-1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => Misb0903.Encode(Frame(new VmtiDetection(1, 10, 20, 30, 40, Track: track))));
    }

    private static (int Column, int Row) Pixel(VmtiPack target, int tag)
        => Vmti.Pixel(Vmti.Integer(target.Items[tag]), 1920);
}
