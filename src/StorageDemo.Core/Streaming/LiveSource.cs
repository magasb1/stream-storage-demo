using System.Text.Json.Serialization;

namespace StorageDemo.Core.Streaming;

/// <summary>
/// Somewhere a stream is copied to, besides the local consumption port.
///
/// Every source already has a local output: any player may pull it from this cluster's consumption
/// port, and that costs nothing until somebody asks. A forward is the other direction - this
/// service dials out and pushes, whether or not anyone is watching - and it exists because the far
/// end is often another gateway in another place that cannot reach in.
/// </summary>
/// <param name="Id">
/// Stable across edits, so a target whose URL changes is the same forward rather than a new one.
/// Minted when a forward is first saved without one; callers never have to invent it.
/// </param>
/// <param name="Url">
/// Where to push, under the same scheme allowlist a pulled source is held to. The scheme picks the
/// container: RTP carries MPEG-TS in the RTP muxer, everything else is plain MPEG-TS.
///
/// <c>srt://host:9000?streamid=name</c> dials the far end, <c>srt://0.0.0.0:9100?mode=listener</c>
/// waits to be pulled from, and <c>udp://</c> or <c>rtp://</c> push with no handshake at all. An
/// SRT target also takes <c>latency</c> (milliseconds) and <c>passphrase</c>, read directly rather
/// than passed through: SRT dials out through the same direct-libsrt stack the ingest port
/// accepts on, not through libav, because only that stack can hand a forward the connection
/// statistics <see cref="ForwardStatus.Link"/> carries. UDP and RTP still open through libav,
/// which passes every other query option straight through as it always has.
/// </param>
/// <param name="Enabled">
/// False stops the copy without forgetting where it went. Switching a forward off and on again is
/// a routine operation and should not cost the operator a URL they then have to retype.
/// </param>
public sealed record ForwardTarget(string Id, string Url, bool Enabled = true);

/// <summary>
/// What a forward is actually doing, as opposed to what it was asked to do.
///
/// Carried on the stream's registry entry rather than on the source, because it is per-connection
/// and belongs to whichever replica currently holds the stream. A source is configuration and
/// outlives every pod; this is the state of one attempt.
/// </summary>
/// <param name="Error">
/// Why the last attempt stopped, when one did. Retained after the forward has given up and while
/// it is waiting to retry, because a forward that is simply not running looks identical to one
/// that has never been asked to run, and the operator needs to tell those apart.
/// </param>
/// <param name="PacketsLost">
/// Packets libsrt sent that the far end reported lost, over the last heartbeat - present only for
/// an SRT target, which is the only one with a handshake to report anything back over. Zero for
/// UDP and RTP, which carry no such answer, same as it is for a stream with nothing to ask yet.
/// </param>
/// <param name="PacketsDropped">
/// Packets libsrt gave up on before they could be sent, because they would already have arrived
/// too late to matter. The sending twin of a source's own dropped count.
/// </param>
/// <param name="Link">
/// libsrt's own read on this connection, present only for an SRT target - see
/// <see cref="SrtForwardLinkStats"/> for why it is not <see cref="LiveStream.Link"/> reused. Null
/// for UDP and RTP, which open through libav and answer only in bytes, and for an SRT target with
/// nothing sampled yet.
/// </param>
public sealed record ForwardStatus(
    string Id,
    string Url,
    bool Connected,
    long Bytes,
    DateTimeOffset? ConnectedAt = null,
    string? Error = null,
    int PacketsLost = 0,
    int PacketsDropped = 0,
    SrtForwardLinkStats? Link = null);

/// <summary>
/// What libsrt itself says about a forward's own connection, over the last heartbeat - the sending
/// twin of <see cref="SrtLinkStats"/>, which reads the same struct from a source's receiving side.
///
/// Not the same type reused with different numbers inside it, because the two are genuinely
/// different questions. SRT_TRACEBSTATS keeps a separate counter for almost everything depending
/// on which direction is asking: a source is answering "what is arriving here", and a forward is
/// answering "what is this replica managing to push out", and forcing both into one type under
/// field names written for the receiving case would put a source's honest answer beside a
/// forward's under a label that only ever told the truth for one of them.
///
/// There is no field for a decrypt failure. Decrypting is what a receiver does, and a forward that
/// carried a field for it would either always read zero for a fact nothing here ever asked, or
/// need a comment explaining why - the absence is the honest answer instead.
/// </summary>
/// <param name="BandwidthMbps">libsrt's own estimate of the link's capacity, direction-agnostic.</param>
/// <param name="SendRateMbps">What is actually leaving, libsrt's own measurement rather than a byte count divided by wall-clock time.</param>
/// <param name="RoundTripTimeMs">The measured round trip on this connection.</param>
/// <param name="PacketsRetransmitted">Packets this sender resent because the far end reported one lost.</param>
/// <param name="NegotiatedLatencyMs">The latency window this end of the connection actually negotiated.</param>
public sealed record SrtForwardLinkStats(
    double BandwidthMbps,
    double SendRateMbps,
    double RoundTripTimeMs,
    int PacketsRetransmitted,
    int NegotiatedLatencyMs);

/// <summary>
/// Where a fixed camera is and where it looks, as an operator configured it.
///
/// Configuration rather than telemetry, which is the whole reason this type exists. A mast, tower
/// or perimeter camera has no platform, no INS and nothing to report, so it sends no KLV and is
/// invisible to everything downstream that works in geodetic terms. None of what it would report
/// changes: position is fixed and orientation changes rarely or never, so it can be stated once
/// and synthesised into a valid ST 0601 Local Set on the stream's own metadata track.
///
/// One nullable sub-record rather than eight nullable fields on <see cref="LiveSource"/>, so that
/// half a configuration cannot be stored - a latitude without a longitude places a camera in the
/// Gulf of Guinea - and so that "is this camera configured" is one null check rather than eight.
/// Both stores serialise the whole <see cref="LiveSource"/> with System.Text.Json, so a row
/// written before this existed reads back with a null here and needs no migration.
///
/// Every range is the ST 0601 item's own, so that a configured value has an exact encoding rather
/// than a saturated one; <c>LiveSourceRules.Refuse</c> refuses anything outside them. The ranges
/// are stated there, once, beside the refusal that enforces them.
/// </summary>
/// <param name="Longitude">ST 0601 tag 14, WGS84 degrees, -180..180.</param>
/// <param name="Latitude">ST 0601 tag 13, WGS84 degrees, -90..90.</param>
/// <param name="AltitudeMetres">ST 0601 tag 15, sensor true altitude above MSL, -900..19000.</param>
/// <param name="TrueBearing">
/// Where the camera looks, degrees clockwise from true north, 0..360. Carried as ST 0601 tag 5,
/// Platform Heading Angle, with tag 18, Sensor Relative Azimuth, sent as zero beside it.
///
/// That split rather than the reverse - heading zero and the bearing in tag 18 - because tag 18 is
/// defined against the platform's own longitudinal axis, and a fixed mount has no such axis to be
/// relative to. Saying the mount points this way and the camera points straight along it is the
/// arrangement that stays true if a pan head is ever added: the bearing then moves into tag 18 and
/// tag 5 keeps describing the mount. <see cref="SensorGeometry.SensorBearing"/> adds the two, so
/// either spelling gives a consumer the same answer today.
/// </param>
/// <param name="Depression">
/// How far below the horizontal the camera looks, ST 0601 tag 19 (Sensor Relative Elevation),
/// -180..180. Negative is downwards, which is the standard's sign and the usual case for a mast.
/// </param>
/// <param name="HorizontalFov">ST 0601 tag 16, degrees, 0..180. What lets a client draw a wedge rather than a pin.</param>
/// <param name="VerticalFov">ST 0601 tag 17, degrees, 0..180.</param>
/// <param name="Classification">
/// The ST 0102 marking to carry in ST 0601 tag 48, or null for an unmarked stream. Optional
/// because requiring an operator to declare a marking invents data: absent means unmarked, which
/// <see cref="Misb0601"/> already treats as an answer distinct from "unclassified". One of
/// <see cref="Misb0601.Classifications"/>.
/// </param>
public sealed record StaticSensor(
    double Longitude,
    double Latitude,
    double AltitudeMetres,
    double TrueBearing,
    double Depression,
    double HorizontalFov,
    double VerticalFov,
    string? Classification = null);

/// <summary>
/// A standing instruction about one stream name: fetch it from here, and copy it to there.
///
/// The difference from <see cref="LiveStream"/> is the whole point of this type. A
/// <see cref="LiveStream"/> is what is on air now, is removed the moment it stops, and belongs to
/// the replica holding the socket. A source is what an operator configured, survives every pod
/// that ever served it, and belongs to nobody. One is a reading, the other is the setting.
///
/// Push and pull share the record. An encoder that pushes a name needs no URL, but it may well
/// need forwarding, and splitting that into two types would mean two lists in the interface for
/// what an operator thinks of as one row.
/// </summary>
/// <param name="Url">
/// Where to pull from, or null when an encoder brings the stream in by itself. Null is not
/// "unconfigured": it is the statement that this name arrives on the ingest port.
/// </param>
/// <param name="Enabled">
/// False parks the source: it stays in the list and this service stops acting on it. Deleting the
/// row is how you forget a source; this is how you park one.
///
/// Parking stops what this service itself started, and only that. A pull already running is
/// dropped, because otherwise the toggle would mean "stop trying again later" while the camera
/// carried on arriving. Every forward stops, for the same reason. A stream an encoder is pushing
/// is untouched, because stopping an encoder is not this toggle's business - refusing a name is
/// the lock's, and ending a feed is the stop call's.
/// </param>
/// <param name="StaticSensor">
/// Where this camera is and where it looks, when it is a fixed one and somebody said. Null means
/// nothing is synthesised for this stream, which is the ordinary case and the state every row
/// written before this field existed deserialises to.
///
/// It takes effect on the next connection rather than under the running one - see
/// <c>LiveStreamCoordinator.ReconcileMetadata</c> - because the track it adds is part of the
/// stream's layout, and every consumer already attached subscribed against the layout it joined on.
/// </param>
public sealed record LiveSource(
    string Name,
    string? Url,
    bool Enabled,
    IReadOnlyList<ForwardTarget> Forwards,
    DateTimeOffset UpdatedAt,
    StaticSensor? StaticSensor = null)
{
    /// <summary>
    /// True when this service is meant to fetch the stream rather than wait for it.
    ///
    /// Not serialised. It is derived from <see cref="Url"/> and nothing reads it back, so writing
    /// it out would put a second, redundant statement of the same fact into a file an operator may
    /// open and edit by hand - and into Redis, where it would be one more thing that can disagree
    /// with itself.
    /// </summary>
    [JsonIgnore]
    public bool IsPull => !string.IsNullOrWhiteSpace(Url);
}

/// <summary>
/// Where configured sources are kept, so every replica agrees on what is meant to be running.
///
/// Deliberately a second store rather than more fields on <see cref="ILiveStreamRegistry"/>. That
/// registry is emptied as streams end, because it answers "what is on air"; this one must survive
/// exactly the events that clear it - a stream ending, a pod dying, the whole service restarting -
/// because it answers "what did somebody ask for". Putting both in one structure would mean either
/// configuration that evaporates or a registry that accumulates the dead.
///
/// It is small and rarely written: an operator adds a source, and a thousand replicas read the
/// list on their heartbeat. Every implementation may therefore be read-heavy and unclever.
/// </summary>
public interface ILiveSourceStore
{
    Task<IReadOnlyList<LiveSource>> ListAsync(CancellationToken cancellationToken = default);

    Task<LiveSource?> GetAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces the whole row. Forwards are part of it, not a separate call.</summary>
    Task SaveAsync(LiveSource source, CancellationToken cancellationToken = default);

    Task RemoveAsync(string name, CancellationToken cancellationToken = default);
}
