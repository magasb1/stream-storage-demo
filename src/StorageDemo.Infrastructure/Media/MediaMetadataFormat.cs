using System.Globalization;

namespace StorageDemo.Infrastructure.Media;

/// <summary>How a probed value is phrased for a reader.</summary>
public static class MediaMetadataFormat
{
    /// <summary>Skips values that say nothing: empty, or one of libav's placeholders.</summary>
    public static void Add(Dictionary<string, string> metadata, string key, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value) && value != "N/A" && value != "unknown")
        {
            metadata[key] = value.Trim();
        }
    }

    public static string Duration(double seconds)
    {
        var span = TimeSpan.FromSeconds(seconds);

        return span < TimeSpan.FromMinutes(1)
            ? $"{seconds.ToString("0.##", CultureInfo.InvariantCulture)} s"
            : span.ToString(span < TimeSpan.FromHours(1) ? @"m\:ss" : @"h\:mm\:ss");
    }

    public static string Bitrate(long bitsPerSecond)
        => bitsPerSecond >= 1_000_000
            ? $"{bitsPerSecond / 1_000_000.0:0.##} Mbit/s"
            : $"{bitsPerSecond / 1000.0:0.#} kbit/s";

    /// <summary>Frame rates arrive as a rational, such as 30000/1001.</summary>
    public static string? FrameRate(int numerator, int denominator)
        => denominator == 0 || numerator == 0
            ? null
            : $"{(double)numerator / denominator:0.##} fps";

    public static string? AspectRatio(int numerator, int denominator)
        => denominator == 0 || numerator == 0 ? null : $"{numerator}:{denominator}";

    public static string SampleRate(int hertz) => $"{hertz / 1000.0:0.#} kHz";

    public static string Dimensions(int width, int height) => $"{width} x {height} px";

    public static string Capitalise(string value)
        => value.Length == 0 ? value : char.ToUpperInvariant(value[0]) + value[1..];

    /// <summary>Labels a stream.</summary>
    public static string StreamLabel(string type, int ordinal)
        => ordinal > 1 ? $"{Capitalise(type)} {ordinal}" : Capitalise(type);
}
