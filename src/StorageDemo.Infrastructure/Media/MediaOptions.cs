using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Infrastructure.Media;

public sealed class MediaOptions
{
    public const string SectionName = "Media";

    /// <summary>Turn off to skip probing and thumbnails entirely.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Where the libav libraries live.</summary>
    public string? LibraryPath { get; init; }

    /// <summary>JPEG quality, on libav's scale where 1 is best and 31 is worst.</summary>
    [Range(1, 31)]
    public int ThumbnailQuality { get; init; } = 4;

    /// <summary>Long edge of the generated thumbnail, in pixels.</summary>
    [Range(32, 2048)]
    public int ThumbnailSize { get; init; } = 320;

    /// <summary>How far into a video to grab the preview frame.</summary>
    [Range(0, 600)]
    public double VideoFrameSeconds { get; init; } = 3;

    /// <summary>Quality for a snapshot, on the same scale as the thumbnail.</summary>
    [Range(1, 31)]
    public int SnapshotQuality { get; init; } = 2;

}
