namespace StorageDemo.Core.Documents;

/// <param name="Metadata">Everything the probe could read, ready to display as-is.</param>
/// <param name="Thumbnail">Encoded preview image, or null when one could not be produced.</param>
public sealed record MediaAnalysis(
    IReadOnlyDictionary<string, string> Metadata,
    byte[]? Thumbnail);

/// <summary>Reads dimensions, duration and codecs out of a media file and renders a preview image.</summary>
public interface IMediaAnalyzer
{
    /// <summary>True when this file is worth probing at all: an image, a video or audio.</summary>
    bool CanAnalyze(string? contentType, string fileName);

    Task<MediaAnalysis> AnalyzeAsync(
        Stream content,
        string fileName,
        string? contentType,
        CancellationToken cancellationToken = default);

    /// <summary>The last picture in the media, at source resolution, or null when it holds none.</summary>
    Task<byte[]?> LatestFrameAsync(
        Stream content,
        string fileName,
        CancellationToken cancellationToken = default);
}
