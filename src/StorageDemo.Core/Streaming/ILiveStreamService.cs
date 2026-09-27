namespace StorageDemo.Core.Streaming;

/// <param name="Preroll">How far back the caller asked to start, in seconds.</param>
/// <param name="Relayed">True when this viewer reached another replica and is being fetched from here.</param>
public sealed record ViewerRequest(string Name, double Preroll, bool Relayed = false);

/// <summary>Live streaming, as everything outside the pipeline sees it.</summary>
public interface ILiveStreamService
{
    /// <summary>This replica's name, as it records itself when it claims a stream.</summary>
    string Owner { get; }

    /// <summary>Transport schemes the loaded libraries can carry, such as udp or srt.</summary>
    IReadOnlyList<string> Transports { get; }

    /// <summary>Every stream on air anywhere, not only this replica's.</summary>
    Task<IReadOnlyList<LiveStream>> StreamsAsync(CancellationToken cancellationToken = default);

    Task<LiveStream?> GetAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>True when this replica holds the connection, so it can serve the bytes.</summary>
    bool Owns(string name);

    /// <summary>The current preview, when this replica owns the stream and has one.</summary>
    byte[]? Preview(string name);

    /// <summary>
    /// The newest KLV packet and what was decoded from it, when this replica owns the stream and
    /// has received one.
    /// </summary>
    KlvSample? Klv(string name);

    /// <summary>
    /// Switches detection on or off for a stream this replica owns, at a rate in detections per
    /// second, zero meaning the worker's default.
    /// </summary>
    Task<LiveStream?> SetDetectionAsync(
        string name,
        bool enabled,
        int rate,
        string? model = null,
        IReadOnlyList<string>? labels = null,
        CancellationToken cancellationToken = default);

    /// <summary>A worker taking, or renewing, its hold on a stream this replica owns.</summary>
    /// <exception cref="InvalidOperationException">Another worker holds a live claim.</exception>
    Task<LiveStream?> ClaimDetectorAsync(string name, string worker, CancellationToken cancellationToken = default);

    /// <summary>Gives a claim back.</summary>
    Task<bool> ReleaseDetectorAsync(string name, string worker, CancellationToken cancellationToken = default);

    /// <summary>A worker's VMTI frame for a stream this replica owns.</summary>
    bool PostDetections(string name, VmtiSample sample);

    /// <summary>
    /// The newest VMTI frame, when this replica owns the stream and a worker has posted one.
    /// </summary>
    VmtiSample? Detections(string name);

    /// <summary>
    /// Creates a stream by request, for a protocol that cannot name itself, and opens its input.
    /// </summary>
    /// <exception cref="InvalidOperationException">The name is already live on another replica.</exception>
    Task<LiveStream> CreateManualAsync(string name, string url, CancellationToken cancellationToken = default);

    /// <summary>Takes a picture of the stream now and stores it as a document.</summary>
    Task<Guid?> SnapshotAsync(
        string name,
        DetectionReference? detection = null,
        CancellationToken cancellationToken = default);

    /// <summary>Starts a recording, or extends the one already running.</summary>
    Task<RecordingStatus?> RecordAsync(
        string name,
        TimeSpan? duration,
        DetectionReference? detection = null,
        CancellationToken cancellationToken = default);

    /// <summary>Ends the recording now.</summary>
    Task<bool> StopRecordingAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Drops the connection and ends the stream.</summary>
    Task<bool> StopAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Writes the stream to a viewer as MPEG-TS until they leave or it ends.</summary>
    Task<double> WriteToViewerAsync(
        ViewerRequest request,
        Stream destination,
        double continueFromSeconds = 0,
        CancellationToken cancellationToken = default);

    /// <summary>How far back a viewer asking for this much would actually start, in seconds.</summary>
    double ResolvePreroll(string name, double seconds);
}
