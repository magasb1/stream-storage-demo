using StorageDemo.Core.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Application;

/// <summary>
/// The decoder against a packet built by hand at known values. A wrong scale is invisible until
/// somebody looks at a map, so every item of the minimum set is asserted, to within the
/// resolution its integer encoding allows.
/// </summary>
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

    /// <summary>
    /// A synthesised set decodes to the configuration it was built from.
    ///
    /// The round trip is worth more than it looks. The decoder's scale table is already checked
    /// against a real stream in <see cref="Misb0601RealStreamTests"/> - over its 711 packets the
    /// slant range agrees with the distance computed from the positions to within four metres,
    /// which it cannot do if a scale is wrong - so an encoder that agrees with that decoder is
    /// agreeing with a table that has been held against a real sender's arithmetic.
    /// </summary>
    [Fact]
    public void A_synthesised_set_decodes_to_the_configuration_it_was_built_from()
    {
        var sensor = new StaticSensor(
            Longitude: -1.826,
            Latitude: 51.179,
            AltitudeMetres: 143.5,
            TrueBearing: 218.4,
            Depression: -12.75,
            HorizontalFov: 6.2,
            VerticalFov: 3.5,
            Classification: "SECRET");

        // Truncated to whole microseconds, which is ST 0603's resolution and so the most tag 2 can
        // carry: comparing against a DateTimeOffset with ticks on it would be testing the clock.
        var at = new DateTimeOffset(2026, 9, 28, 11, 12, 13, TimeSpan.Zero);

        var packet = Misb0601.Encode(sensor, at, "SYNTHESISED STATIC SENSOR");
        var set = Misb0601.Decode(packet);

        // Not null is the checksum, the BER lengths and the key all being right at once, since
        // Decode returns null for any of them.
        Assert.NotNull(set);

        Assert.Equal(at, set.Timestamp);
        Assert.Equal("SYNTHESISED STATIC SENSOR", set.PlatformDesignation);
        Assert.Equal("SECRET", set.Classification);
        Assert.Equal(Misb0601.Version, set.Version);

        Assert.Equal(sensor.Latitude, set.SensorLatitude!.Value, 90.0 / int.MaxValue);
        Assert.Equal(sensor.Longitude, set.SensorLongitude!.Value, 180.0 / int.MaxValue);
        Assert.Equal(sensor.AltitudeMetres, set.SensorTrueAltitude!.Value, 19900.0 / ushort.MaxValue);
        Assert.Equal(sensor.HorizontalFov, set.SensorHorizontalFov!.Value, 180.0 / ushort.MaxValue);
        Assert.Equal(sensor.VerticalFov, set.SensorVerticalFov!.Value, 180.0 / ushort.MaxValue);
        Assert.Equal(sensor.TrueBearing, set.PlatformHeading!.Value, 360.0 / ushort.MaxValue);
        Assert.Equal(sensor.Depression, set.SensorRelativeElevation!.Value, 180.0 / int.MaxValue);

        // Present although it is zero. SensorBearing is tag 5 plus tag 18 and returns null if
        // either is absent, so a set that left this out would carry a bearing nothing could read.
        Assert.Equal(0, set.SensorRelativeAzimuth!.Value, 360.0 / uint.MaxValue);

        // Absent on purpose, each for a stated reason: an unrolled fixed mount is what a missing
        // tag 20 already means, and a frame centre needs a range or a terrain model this service
        // does not have. Guessing either would give anything built on top of it an invention.
        Assert.Null(set.SensorRelativeRoll);
        Assert.Null(set.SlantRange);
        Assert.Null(set.FrameCenterLatitude);
        Assert.Null(set.FrameCenterLongitude);
        Assert.Null(set.FrameCenterElevation);

        // Nothing rode along that this encoder did not mean to write.
        Assert.Empty(set.Unparsed);
    }

    /// <summary>
    /// The bearing a consumer reads off a synthesised set is the bearing that was configured.
    ///
    /// The end-to-end statement of what tag 5 and tag 18 are for, through the same expression a
    /// client's north arrow is drawn from rather than through the two raw items.
    /// </summary>
    [Fact]
    public void The_bearing_read_off_a_synthesised_set_is_the_one_that_was_configured()
    {
        var sensor = new StaticSensor(
            Longitude: 0,
            Latitude: 0,
            AltitudeMetres: 30,
            TrueBearing: 218.4,
            Depression: -8,
            HorizontalFov: 40,
            VerticalFov: 22);

        var set = Misb0601.Decode(Misb0601.Encode(sensor, DateTimeOffset.UnixEpoch, "SYNTHESISED STATIC SENSOR"));

        Assert.NotNull(set);

        var bearing = SensorGeometry.SensorBearing(set.PlatformHeading, set.SensorRelativeAzimuth);

        Assert.NotNull(bearing);
        Assert.Equal(sensor.TrueBearing, bearing.Value, 360.0 / ushort.MaxValue);

        // An unmarked stream stays unmarked rather than becoming unclassified.
        Assert.Null(set.Classification);
    }

    /// <summary>
    /// A value outside an ST 0601 item's range is refused rather than saturated, because a packet
    /// carrying the range's endpoint is indistinguishable from one carrying a measurement.
    /// </summary>
    [Fact]
    public void A_configuration_outside_an_items_range_is_refused_rather_than_saturated()
    {
        var sensor = new StaticSensor(0, 0, AltitudeMetres: 25_000, TrueBearing: 0, Depression: 0, HorizontalFov: 40, VerticalFov: 22);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => Misb0601.Encode(sensor, DateTimeOffset.UnixEpoch, "SYNTHESISED STATIC SENSOR"));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => Misb0601.Encode(sensor with { AltitudeMetres = 30, Classification = "COSMIC" }, DateTimeOffset.UnixEpoch, "X"));
    }

    [Fact]
    public void A_packet_whose_checksum_does_not_match_is_rejected()
    {
        var packet = Misb.MinimumSet();

        // One bit in the last item before the checksum, which is the error this check exists to catch.
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
    /// slip is unmistakable. A camera looking east has north to the left of the picture; a camera
    /// looking north but twisted a quarter turn clockwise about its lens has north to the left too,
    /// because twisting the camera turns the scene inside the frame the other way.
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
