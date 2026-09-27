using StorageDemo.Core.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Application;

/// <summary>The decoder against a packet built by hand at known values.</summary>
public sealed class Misb0601Tests
{
    [Fact]
    public void The_minimum_set_decodes_to_the_values_it_was_built_from()
    {
        var set = Misb0601.Decode(Misb.MinimumSet());

        Assert.NotNull(set);

        Assert.Equal(Misb.Known.Timestamp, set.Timestamp);
        Assert.Equal(Misb.Known.MissionId, set.MissionId);
        Assert.Equal(Misb.Known.PlatformDesignation, set.PlatformDesignation);
        Assert.Equal(Misb.Known.ImageSourceSensor, set.ImageSourceSensor);
        Assert.Equal(Misb.Known.ImageCoordinateSystem, set.ImageCoordinateSystem);
        Assert.Equal(Misb.Known.Classification, set.Classification);
        Assert.Equal(Misb.Known.Version, set.Version);

        // Half a step of each encoding: a uint16 over 360 degrees resolves to 0.0055.
        Assert.Equal(Misb.Known.PlatformHeading, set.PlatformHeading!.Value, 360.0 / ushort.MaxValue);
        Assert.Equal(Misb.Known.PlatformPitch, set.PlatformPitch!.Value, 20.0 / short.MaxValue);
        Assert.Equal(Misb.Known.PlatformRoll, set.PlatformRoll!.Value, 50.0 / short.MaxValue);
        Assert.Equal(Misb.Known.SensorLatitude, set.SensorLatitude!.Value, 90.0 / int.MaxValue);
        Assert.Equal(Misb.Known.SensorLongitude, set.SensorLongitude!.Value, 180.0 / int.MaxValue);
        Assert.Equal(Misb.Known.SensorTrueAltitude, set.SensorTrueAltitude!.Value, 19900.0 / ushort.MaxValue);
        Assert.Equal(Misb.Known.SensorHorizontalFov, set.SensorHorizontalFov!.Value, 180.0 / ushort.MaxValue);
        Assert.Equal(Misb.Known.SensorVerticalFov, set.SensorVerticalFov!.Value, 180.0 / ushort.MaxValue);
        Assert.Equal(Misb.Known.SensorRelativeAzimuth, set.SensorRelativeAzimuth!.Value, 360.0 / uint.MaxValue);
        Assert.Equal(Misb.Known.SensorRelativeElevation, set.SensorRelativeElevation!.Value, 180.0 / int.MaxValue);
        Assert.Equal(Misb.Known.SensorRelativeRoll, set.SensorRelativeRoll!.Value, 360.0 / uint.MaxValue);
        Assert.Equal(Misb.Known.SlantRange, set.SlantRange!.Value, 5_000_000.0 / uint.MaxValue);
        Assert.Equal(Misb.Known.FrameCenterLatitude, set.FrameCenterLatitude!.Value, 90.0 / int.MaxValue);
        Assert.Equal(Misb.Known.FrameCenterLongitude, set.FrameCenterLongitude!.Value, 180.0 / int.MaxValue);
        Assert.Equal(Misb.Known.FrameCenterElevation, set.FrameCenterElevation!.Value, 19900.0 / ushort.MaxValue);

        // Outside the minimum set, so raw, and nothing else leaked into the raw bag.
        Assert.Equal(Misb.Known.TailNumber, Assert.Single(set.Unparsed).Value);
    }

    [Fact]
    public void A_packet_whose_checksum_does_not_match_is_rejected()
    {
        var packet = Misb.MinimumSet();

        // One bit in the last item before the checksum, which is the error this check exists to
        // catch.
        packet[^5] ^= 0x01;

        Assert.Null(Misb0601.Decode(packet));
        Assert.True(Misb0601.IsUasDatalink(packet));
    }

    [Fact]
    public void A_packet_without_a_checksum_is_rejected()
    {
        var body = Misb.Items((13, [0x40, 0x00, 0x00, 0x00]));
        byte[] packet = [.. Misb0601.Key, (byte)body.Length, .. body];

        Assert.Null(Misb0601.Decode(packet));
    }

    [Fact]
    public void The_error_indicator_decodes_to_nothing_rather_than_a_place()
    {
        var set = Misb0601.Decode(Misb.Packet(
            (13, [0x80, 0x00, 0x00, 0x00]),
            (6, [0x80, 0x00])));

        Assert.NotNull(set);
        Assert.Null(set.SensorLatitude);
        Assert.Null(set.PlatformPitch);
    }

    [Fact]
    public void Something_under_another_key_is_not_a_uas_datalink_packet()
    {
        var packet = Misb.MinimumSet();
        packet[4] ^= 0xFF;

        Assert.False(Misb0601.IsUasDatalink(packet));
        Assert.Null(Misb0601.Decode(packet));
    }

    [Fact]
    public void A_security_set_without_a_classification_leaves_the_marking_null()
    {
        var set = Misb0601.Decode(Misb.Packet((48, Misb.Items((3, "//NOR"u8.ToArray())))));

        Assert.NotNull(set);
        Assert.Null(set.Classification);
    }

    /// <summary>
    /// The two rotations the north arrow is made of, each on its own, at right angles where a sign
    /// slip is unmistakable.
    /// </summary>
    [Theory]
    [InlineData(0, 0, 0, 0)]
    [InlineData(90, 0, 0, 270)]
    [InlineData(0, 90, 0, 270)]
    [InlineData(45, 45, 0, 270)]
    [InlineData(0, 0, 90, 270)]
    [InlineData(180, 0, 180, 0)]
    public void North_in_the_image_turns_with_the_pointing_and_against_the_camera_twist(
        double heading, double azimuth, double roll, double expected)
        => Assert.Equal(expected, SensorGeometry.NorthInImage(heading, azimuth, roll)!.Value, 6);

    [Fact]
    public void Without_a_heading_or_an_azimuth_there_is_no_north_to_draw()
    {
        Assert.Null(SensorGeometry.NorthInImage(null, 12, 0));
        Assert.Null(SensorGeometry.NorthInImage(12, null, 0));

        // An absent roll is the one that still answers: an unrolled camera is the assumption, and
        // the display says which roll it used.
        Assert.Equal(348, SensorGeometry.NorthInImage(10, 2, null)!.Value, 6);
    }

    [Theory]
    [InlineData(0, 0, 0)]
    [InlineData(90, 0, 270)]
    [InlineData(288.58, 0, 71.42)]
    [InlineData(270, 20, 70)]
    public void North_in_the_image_can_use_an_absolute_look_bearing(
        double bearing, double roll, double expected)
        => Assert.Equal(expected, SensorGeometry.NorthInImage(bearing, roll)!.Value, 2);

    /// <summary>Due east along the equator, where the great circle and the rhumb line agree.</summary>
    [Fact]
    public void The_bearing_between_two_positions_is_the_one_a_compass_would_read()
    {
        Assert.Equal(90, SensorGeometry.BearingBetween(0, 10, 0, 11)!.Value, 3);
        Assert.Equal(0, SensorGeometry.BearingBetween(50, 10, 51, 10)!.Value, 3);
        Assert.Equal(180, SensorGeometry.BearingBetween(50, 10, 49, 10)!.Value, 3);
        Assert.Null(SensorGeometry.BearingBetween(50, 10, null, 10));
    }

    [Fact]
    public void The_difference_between_two_bearings_goes_the_short_way_round()
    {
        Assert.Equal(2, SensorGeometry.BearingDifference(1, 359), 6);
        Assert.Equal(-2, SensorGeometry.BearingDifference(359, 1), 6);
    }
}
