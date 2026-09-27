namespace StorageDemo.Core.Streaming;

/// <summary>Where the sensor is looking, and where north is in the picture it produced.</summary>
public static class SensorGeometry
{
    /// <summary>
    /// Where the sensor points, degrees clockwise from true north: tag 5 + tag 18, both measured
    /// clockwise, so they add.
    /// </summary>
    public static double? SensorBearing(double? platformHeading, double? sensorRelativeAzimuth)
        => platformHeading is { } heading && sensorRelativeAzimuth is { } azimuth
            ? Wrap(heading + azimuth)
            : null;

    /// <summary>
    /// Where true north lies in the displayed image, degrees clockwise from the top of the picture,
    /// which is what an arrow on screen is rotated by.
    /// </summary>
    public static double? NorthInImage(double? platformHeading, double? sensorRelativeAzimuth, double? sensorRelativeRoll)
        => SensorBearing(platformHeading, sensorRelativeAzimuth) is { } bearing
            ? NorthInImage(bearing, sensorRelativeRoll)
            : null;

    /// <summary>
    /// Where true north lies in the displayed image when the sensor's absolute look bearing is
    /// already known.
    /// </summary>
    public static double? NorthInImage(double? sensorBearing, double? sensorRelativeRoll)
        => sensorBearing is { } bearing ? Wrap(-(bearing + (sensorRelativeRoll ?? 0))) : null;

    /// <summary>
    /// The initial great-circle bearing from one WGS84 position to another, degrees clockwise from
    /// true north.
    /// </summary>
    public static double? BearingBetween(double? fromLatitude, double? fromLongitude, double? toLatitude, double? toLongitude)
    {
        if (fromLatitude is not { } lat1 || fromLongitude is not { } lon1
            || toLatitude is not { } lat2 || toLongitude is not { } lon2)
        {
            return null;
        }

        var (f1, f2) = (Radians(lat1), Radians(lat2));
        var dl = Radians(lon2 - lon1);

        return Wrap(Degrees(Math.Atan2(
            Math.Sin(dl) * Math.Cos(f2),
            Math.Cos(f1) * Math.Sin(f2) - Math.Sin(f1) * Math.Cos(f2) * Math.Cos(dl))));
    }

    /// <summary>The signed difference between two bearings, -180..180, so 359 and 1 are 2 apart.</summary>
    public static double BearingDifference(double a, double b) => Wrap(a - b + 180) - 180;

    private static double Wrap(double degrees) => ((degrees % 360) + 360) % 360;

    private static double Radians(double degrees) => degrees * Math.PI / 180;

    private static double Degrees(double radians) => radians * 180 / Math.PI;
}
