namespace StorageDemo.Core.Streaming;

public enum LiveStreamState
{
    /// <summary>Packets are arriving.</summary>
    Live,

    /// <summary>The feed has stopped arriving but the grace period has not expired.</summary>
    Interrupted,
}

/// <param name="Id">The recording in progress, unique per recording.</param>
/// <param name="EndsAt">When it is due to stop.</param>
/// <param name="Truncated">Set when the recording ended because its queue overflowed.</param>
public sealed record RecordingStatus(
    Guid Id,
    DateTimeOffset StartedAt,
    DateTimeOffset? EndsAt,
    long Bytes,
    bool Truncated = false);

/// <summary>A named live feed, as every replica sees it.</summary>
/// <param name="Owner">The replica holding the connection.</param>
/// <param name="OwnerAddress">Where that replica can be reached, recorded when it claimed the name.</param>
/// <param name="Heartbeat">Last time the owner said it was still alive.</param>
/// <param name="Startable">False while no keyframe has arrived for longer than the buffer can hold.</param>
/// <param name="CeilingBinding">
/// True when the buffer's byte ceiling is evicting before its time window is reached, which
/// silently shortens every pre-roll taken from it.
/// </param>
/// <param name="PacketsLost">
/// Packets the transport never received on this feed during the last heartbeat, as libsrt counts
/// them.
/// </param>
/// <param name="PacketsDropped">
/// Packets that did arrive but too late for the latency window to play them, over the same
/// interval.
/// </param>
/// <param name="HasKlv">Whether the transport carries a MISB KLV metadata stream.</param>
/// <param name="KlvAt">When the last KLV packet arrived, if any has.</param>
/// <param name="Classification">
/// The ST 0102 marking from the newest KLV packet, carried here rather than only on the KLV route
/// because a wall of a thousand tiles has to show its markings without a thousand calls.
/// </param>
/// <param name="DetectionEnabled">The toggle, set through the owner and read by workers.</param>
/// <param name="DetectionRate">Detections per second while enabled; zero means the worker's default.</param>
/// <param name="Forwards">
/// What each configured forward is doing on the owning replica, empty when none is configured.
/// </param>
/// <param name="Link">The transport's own read on this connection, when there is one to ask.</param>
/// <param name="Viewers">
/// Players pulling this stream from this replica right now, counted for the life of each one's own
/// subscription to the packet fan-out - not the demultiplexer's subscriber count, which also
/// carries the recorder, the harvester, the KLV extractor and every forward.
/// </param>
/// <param name="DetectionWorker">
/// Which worker holds the stream, null when none has claimed it or the one that had it has gone
/// quiet.
/// </param>
/// <param name="DetectionModel">The detector family requested for this stream.</param>
/// <param name="DetectionLabels">COCO class names retained before tracking and publication.</param>
public sealed record LiveStream(
    string Name,
    LiveStreamState State,
    DateTimeOffset StartedAt,
    DateTimeOffset Heartbeat,
    string Owner,
    string? OwnerAddress,
    long Packets,
    long Bytes,
    bool HasPreview,
    bool Startable,
    bool CeilingBinding,
    double BufferedSeconds,
    string? Layout,
    RecordingStatus? Recording,
    string? ConnectionId,
    bool Manual = false,
    int PacketsLost = 0,
    int PacketsDropped = 0,
    bool HasKlv = false,
    DateTimeOffset? KlvAt = null,
    string? Classification = null,
    bool DetectionEnabled = false,
    int DetectionRate = 0,
    string? DetectionWorker = null,
    IReadOnlyList<ForwardStatus>? Forwards = null,
    SrtLinkStats? Link = null,
    int Viewers = 0,
    string? DetectionModel = null,
    IReadOnlyList<string>? DetectionLabels = null);

/// <summary>What libsrt itself says about one connection, over the last heartbeat.</summary>
/// <param name="BandwidthMbps">
/// libsrt's own estimate of the link's capacity, independent of what this stream happens to be
/// sending.
/// </param>
/// <param name="ReceiveRateMbps">
/// What is actually arriving, libsrt's own measurement rather than a byte count divided by
/// wall-clock time.
/// </param>
/// <param name="RoundTripTimeMs">The measured round trip.</param>
/// <param name="PacketsRetransmitted">
/// Packets this receiver got that were resent because an earlier attempt was lost or reported lost
/// - the traffic loss recovery cost, distinct from loss itself.
/// </param>
/// <param name="NegotiatedLatencyMs">
/// The latency window actually active on this connection, which the two ends negotiate to the
/// larger of what each asked for.
/// </param>
/// <param name="UndecryptedPacketsTotal">
/// Packets libsrt could not decrypt, over the life of the connection.
/// </param>
public sealed record SrtLinkStats(
    double BandwidthMbps,
    double ReceiveRateMbps,
    double RoundTripTimeMs,
    int PacketsRetransmitted,
    int NegotiatedLatencyMs,
    int UndecryptedPacketsTotal);

/// <summary>
/// Where live streams are recorded so every replica can see them, not just the one holding the
/// connection.
/// </summary>
public interface ILiveStreamRegistry
{
    Task UpsertAsync(LiveStream stream, CancellationToken cancellationToken = default);

    Task RemoveAsync(string name, CancellationToken cancellationToken = default);

    Task<LiveStream?> GetAsync(string name, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<LiveStream>> ListAsync(CancellationToken cancellationToken = default);
}

/// <summary>Decides when a stream whose owner has gone quiet should stop being reported.</summary>
public static class LiveStreamStaleness
{
    public static bool IsGone(LiveStream stream, TimeSpan grace)
        => DateTimeOffset.UtcNow - stream.Heartbeat > grace;

    /// <summary>
    /// Whether the replica that owns this entry is still there, on a much shorter fuse than <see
    /// cref="IsGone"/>.
    /// </summary>
    public static bool OwnerAlive(LiveStream stream, TimeSpan beat)
        => DateTimeOffset.UtcNow - stream.Heartbeat <= beat * 3;
}
