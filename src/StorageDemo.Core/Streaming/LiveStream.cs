namespace StorageDemo.Core.Streaming;

public enum LiveStreamState
{
    /// <summary>Packets are arriving.</summary>
    Live,

    /// <summary>
    /// The feed has stopped arriving but the grace period has not expired. The stream is still
    /// claimed, still listed, and its hub, buffer and any recording are all still alive. A
    /// reconnect inside this window resumes the same stream rather than creating a second one.
    /// </summary>
    Interrupted,
}

/// <param name="Id">
/// The recording in progress, unique per recording. It is <b>not</b> a document id and never
/// becomes one: the recorder mints this when it starts and mints nothing else, and the document id
/// is a separate value that does not exist until the recording ends and its file has been stored.
/// Nothing in this record carries it, so a caller holding a running recording cannot name the
/// document it will become - it can only wait for the recording to finish and find the document by
/// what it was written with.
/// </param>
/// <param name="EndsAt">
/// When it is due to stop. A further trigger moves this later rather than starting a second
/// recording, so continuous detection leaves one clip covering the whole event.
/// </param>
/// <param name="Truncated">
/// Set when the recording ended because its queue overflowed. The document is real but short, and
/// saying so is the point: dropping packets to keep going would write a hole into a file that
/// claims to be a recording.
/// </param>
public sealed record RecordingStatus(
    Guid Id,
    DateTimeOffset StartedAt,
    DateTimeOffset? EndsAt,
    long Bytes,
    bool Truncated = false);

/// <summary>
/// A named live feed, as every replica sees it.
///
/// The name is the identity, not an incidental label. A feed that drops and reconnects under the
/// same name is the same stream resuming, which is why nothing here is keyed by an opaque
/// identifier. The connection identifier below survives only to tell one attempt from the next in
/// a log; nothing looks a stream up by it.
/// </summary>
/// <param name="Owner">
/// The replica holding the connection. Only the owner has the bytes, so requests reaching another
/// replica are forwarded to it. Writing a different owner here is how a name moves, and it is
/// allowed only once the name is free - the feed interrupted, or the owner no longer heartbeating.
/// The replica losing it reads this on its next heartbeat and stands down.
/// </param>
/// <param name="OwnerAddress">
/// Where that replica can be reached, recorded when it claimed the name. Both a forwarded control
/// call and a relayed viewer's media go over it, because inside the cluster the hop is HTTP.
/// </param>
/// <param name="Heartbeat">
/// Last time the owner said it was still alive. A pod that dies holding a socket cannot report its
/// own death, so a stale heartbeat is how the rest of the cluster finds out.
/// </param>
/// <param name="Startable">
/// False while no keyframe has arrived for longer than the buffer can hold. Every pre-roll from
/// this stream is then empty and every snapshot of it is second-hand, and the fix is at the
/// encoder rather than here.
/// </param>
/// <param name="CeilingBinding">
/// True when the buffer's byte ceiling is evicting before its time window is reached, which
/// silently shortens every pre-roll taken from it.
/// </param>
/// <param name="PacketsLost">
/// Packets the transport never received on this feed during the last heartbeat, as libsrt counts
/// them. Zero on a healthy stream, and the first thing to look at on one that is not: a stream can
/// be listed as live, with packets and bytes rising, while most of what was sent to it is missing.
/// An interval rather than a total, so it answers "is this stream broken now"; see
/// <c>SrtSocketStream.Health</c>.
/// </param>
/// <param name="PacketsDropped">
/// Packets that did arrive but too late for the latency window to play them, over the same
/// interval. Distinct from lost, and usually means the window is too small for the link rather
/// than that the link is failing.
/// </param>
/// <param name="HasKlv">Whether the transport carries a MISB KLV metadata stream.</param>
/// <param name="KlvSynthesised">
/// Whether that metadata is this service's own synthesis from a fixed camera's configured position
/// rather than the sender's telemetry. False when the stream carries none at all, so a camera that
/// has a <see cref="StaticSensor"/> configured and reads false here is one whose configuration is
/// not yet on the wire - a pushed stream waiting for its encoder to reconnect, or one whose sender
/// declares KLV of its own and therefore wins.
///
/// True has a quieter second reading worth knowing, and it is <see cref="HasKlv"/>'s as much as
/// this one's. Both are read off the layout, so both say a track is there rather than that anything
/// is going down it. A track can only be added to or taken off a layout at a connection, so when an
/// operator removes a sensor from a pushed camera the track stays until its encoder reconnects
/// while nothing publishes on it: the pair reads true and true, and <see cref="KlvAt"/> is the
/// field that tells the truth by standing still. A client showing metadata as live should be
/// looking at <see cref="KlvAt"/>, not at either of these.
/// </param>
/// <param name="KlvAt">When the last KLV packet arrived, if any has.</param>
/// <param name="Classification">
/// The ST 0102 marking from the newest KLV packet, carried here rather than only on the KLV route
/// because a wall of a thousand tiles has to show its markings without a thousand calls. Null
/// when the stream carries no KLV or no security set, which a client shows as unmarked; that is a
/// different thing from an empty marking.
/// </param>
/// <param name="DetectionEnabled">
/// The toggle, set through the owner and read by workers. Detection is per stream and on demand:
/// a thousand streams ingest and a chosen subset decode (detection-plan.md).
/// </param>
/// <param name="DetectionRate">Detections per second while enabled; zero means the worker's default.</param>
/// <param name="Forwards">
/// What each configured forward is doing on the owning replica, empty when none is configured.
/// Here rather than on <see cref="LiveSource"/> because a forward only exists where the bytes are,
/// and this record is already the answer to "what is this stream doing, on whichever pod has it".
/// </param>
/// <param name="Link">
/// The transport's own read on this connection, when there is one to ask. Only a publisher
/// accepted on the ingest port has a socket this replica holds directly: a pulled stream and every
/// forward are opened by libav instead, which answers for itself in bytes and nothing about the
/// wire underneath, so this is null for both. Absence here means "nothing to ask", never "asked
/// and got zero" - the same reasoning <see cref="Classification"/> uses.
/// </param>
/// <param name="Viewers">
/// Players pulling this stream from this replica right now, counted for the life of each one's own
/// subscription to the packet fan-out - not the demultiplexer's subscriber count, which also
/// carries the recorder, the harvester, the KLV extractor and every forward. A relayed viewer
/// counts here too: a relay ends up subscribing on this same replica, because only the owner has
/// the bytes. Always a real number rather than null, unlike <see cref="Link"/>: whether a stream
/// has any watchers is meaningful whichever way it arrived.
/// </param>
/// <param name="DetectionWorker">
/// Which worker holds the stream, null when none has claimed it or the one that had it has gone
/// quiet. A worker claims by writing its name through the owner and renews by writing it again;
/// nothing pushes work to a worker.
/// </param>
/// <param name="DetectionModel">
/// The detector family requested for this stream. Null lets the worker use its configured default,
/// which preserves streams written by older replicas.
/// </param>
/// <param name="DetectionLabels">
/// COCO class names retained before tracking and publication. Empty means every class.
/// </param>
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
    IReadOnlyList<string>? DetectionLabels = null,
    bool KlvSynthesised = false);

/// <summary>
/// What libsrt itself says about one connection, over the last heartbeat.
///
/// A curated handful, not the roughly ninety fields <c>SRT_TRACEBSTATS</c> actually carries: most
/// of those are congestion-control internals nobody reads to answer "is this link healthy", and a
/// wall of numbers serves an operator worse than the few they would actually look at. These are
/// the ones a stream's own bad day shows up in first, in Haivision's own panels among others -
/// the estimated capacity of the link against what is actually arriving, how long a round trip
/// takes, how much of what should have arrived did not, and what the receiver corrected for it.
///
/// <see cref="PacketsRetransmitted"/> and the loss and drop counts on <see cref="LiveStream"/>
/// itself are all over the same beat: the interval since this replica last asked, which is what
/// makes the answer "broken now" rather than "broken at some point today". <see
/// cref="UndecryptedPacketsTotal"/> is deliberately the running total instead - a decrypt failure
/// is rare enough that resetting its count to zero every two seconds would hide the one that
/// matters between heartbeats.
/// </summary>
/// <param name="BandwidthMbps">
/// libsrt's own estimate of the link's capacity, independent of what this stream happens to be
/// sending. A rate below this is the link coasting; a receive rate climbing to meet it while loss
/// also climbs is the link running out of room.
/// </param>
/// <param name="ReceiveRateMbps">What is actually arriving, libsrt's own measurement rather than a byte count divided by wall-clock time.</param>
/// <param name="RoundTripTimeMs">The measured round trip. The floor under useful latency, and a rising RTT is usually the first sign of a link about to lose packets.</param>
/// <param name="PacketsRetransmitted">Packets this receiver got that were resent because an earlier attempt was lost or reported lost - the traffic loss recovery cost, distinct from loss itself.</param>
/// <param name="NegotiatedLatencyMs">
/// The latency window actually active on this connection, which the two ends negotiate to the
/// larger of what each asked for. Worth showing beside the configured value because an encoder
/// asking for more than this service does is not a misconfiguration here - it is the encoder's.
/// </param>
/// <param name="UndecryptedPacketsTotal">
/// Packets libsrt could not decrypt, over the life of the connection. Zero on every healthy
/// stream; a rising count against a passphrase this replica is not configured to check is the
/// first and often only sign that somebody is sending encrypted media nobody here can use.
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
///
/// Keyed by name. The socket cannot be shared, but knowing that a stream exists, who owns it and
/// how to reach them can be, and that is the difference between a cluster where the API tells the
/// truth about what is on air and one where the answer depends on which pod you happened to hit.
///
/// Entries are removed when a stream ends, so this stays a picture of what is live now. That is
/// the only thing every replica needs to agree on: the documents a stream produced are its trace.
/// </summary>
public interface ILiveStreamRegistry
{
    Task UpsertAsync(LiveStream stream, CancellationToken cancellationToken = default);

    Task RemoveAsync(string name, CancellationToken cancellationToken = default);

    Task<LiveStream?> GetAsync(string name, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<LiveStream>> ListAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Decides when a stream whose owner has gone quiet should stop being reported.
///
/// A pod killed mid-stream leaves an entry saying "live" that nothing will ever update. Rather
/// than show a stream that stopped minutes ago, an entry whose owner has stopped heartbeating for
/// longer than the grace period is treated as gone. The reader decides this rather than the
/// registry rewriting anything, because the owner might yet come back.
/// </summary>
public static class LiveStreamStaleness
{
    public static bool IsGone(LiveStream stream, TimeSpan grace)
        => DateTimeOffset.UtcNow - stream.Heartbeat > grace;

    /// <summary>
    /// Whether the replica that owns this entry is still there, on a much shorter fuse than
    /// <see cref="IsGone"/>.
    ///
    /// The two windows answer different questions and are deliberately not the same number. What a
    /// viewer sees is decided by the grace period: a tile that vanishes and returns is worse than
    /// one showing a state, so an entry stays listed as interrupted for thirty seconds. Who may
    /// publish the name is decided here: a dead pod's encoders are already reconnecting, and making
    /// them wait out the grace period would cost half a minute of black screen to protect a pod
    /// that is not coming back.
    ///
    /// Three beats, because one missed heartbeat is a scheduling hiccup and three is a pod that has
    /// stopped. At a two-second beat a name frees about six seconds after its owner dies.
    /// </summary>
    public static bool OwnerAlive(LiveStream stream, TimeSpan beat)
        => DateTimeOffset.UtcNow - stream.Heartbeat <= beat * 3;
}
