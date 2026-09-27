using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Infrastructure.Media;

public sealed class MediaOptions
{
    public const string SectionName = "Media";

    /// <summary>Turn off to skip probing and thumbnails entirely.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// Where the libav libraries live. Empty means the ones bundled with the application, which is
    /// what every normal deployment uses. Point it at a build compiled with libsrt to get SRT.
    /// </summary>
    public string? LibraryPath { get; init; }

    /// <summary>JPEG quality, on libav's scale where 1 is best and 31 is worst.</summary>
    [Range(1, 31)]
    public int ThumbnailQuality { get; init; } = 4;

    /// <summary>Long edge of the generated thumbnail, in pixels.</summary>
    [Range(32, 2048)]
    public int ThumbnailSize { get; init; } = 320;

    /// <summary>
    /// How far into a video to grab the preview frame. The very first frame is often black,
    /// a fade-in or a slate.
    /// </summary>
    [Range(0, 600)]
    public double VideoFrameSeconds { get; init; } = 3;

    /// <summary>
    /// Quality for a snapshot, on the same scale as the thumbnail. Better, because a snapshot is
    /// a document someone opens and is sometimes the only surviving record of what happened.
    /// A lossless format belongs here if snapshots ever have to be evidence-grade.
    /// </summary>
    [Range(1, 31)]
    public int SnapshotQuality { get; init; } = 2;

    /// <summary>
    /// How many calls may be inside this analyzer at once, across every caller: a snapshot of a
    /// live stream, a thumbnail for an upload, anything else that arrives.
    ///
    /// One per processor, because what this bounds is a blocking decode and a JPEG encode. A wider
    /// front does not produce a single frame any sooner; it only spreads the same cores over more
    /// work in flight, each item holding a temp file, a decoder's buffers and a thread pool thread.
    /// That pool is the one serving viewers, the API and the heartbeat, which is what makes the
    /// width everybody else's problem rather than the snapshot's.
    ///
    /// Nothing upstream bounds how many callers arrive. Detection triggers a capture per stream and
    /// a cluster is meant to hold a thousand streams, so a moment when every stream fires at once
    /// is the shape this service is built for rather than a misuse of it. Two hundred simultaneous
    /// snapshots against two hundred streams on four cores did all land - 0.38 s median, 0.77 s at
    /// the slowest - and they landed by taking the pool two hundred wide. A stored snapshot is
    /// itself a JPEG upload, so those two hundred captures queue two hundred thumbnails back
    /// through here as well; the analysis worker takes them one at a time and they are small, so
    /// what they cost is the length of that queue rather than any more width.
    ///
    /// Never fewer than two, whatever the processor count, because an upload's permit covers
    /// reading the object back out of storage: on a single-processor container a width of one would
    /// hand its only slot to a 50 MB GET from S3 and leave every snapshot queued behind it.
    ///
    /// Raising it suits a deployment whose analyses wait on storage rather than on a core. A slow
    /// local disk under the temp file is a reason to lower it instead. The ceiling on the range is
    /// where a valve stops being one, and the default is clamped into the range so that a very
    /// large host cannot fail options validation at startup.
    /// </summary>
    [Range(1, 1024)]
    public int MaxConcurrentDecodes { get; init; }
        = Math.Max(2, Math.Min(1024, Environment.ProcessorCount));

}
