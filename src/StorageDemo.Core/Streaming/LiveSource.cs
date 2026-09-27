using System.Text.Json.Serialization;

namespace StorageDemo.Core.Streaming;

/// <summary>Somewhere a stream is copied to, besides the local consumption port.</summary>
/// <param name="Id">
/// Stable across edits, so a target whose URL changes is the same forward rather than a new one.
/// </param>
/// <param name="Url">Where to push, under the same scheme allowlist a pulled source is held to.</param>
/// <param name="Enabled">False stops the copy without forgetting where it went.</param>
public sealed record ForwardTarget(string Id, string Url, bool Enabled = true);

/// <summary>What a forward is actually doing, as opposed to what it was asked to do.</summary>
/// <param name="Error">Why the last attempt stopped, when one did.</param>
/// <param name="PacketsLost">
/// Packets libsrt sent that the far end reported lost, over the last heartbeat - present only for
/// an SRT target, which is the only one with a handshake to report anything back over.
/// </param>
/// <param name="PacketsDropped">
/// Packets libsrt gave up on before they could be sent, because they would already have arrived too
/// late to matter.
/// </param>
/// <param name="Link">
/// libsrt's own read on this connection, present only for an SRT target - see <see
/// cref="SrtForwardLinkStats"/> for why it is not <see cref="LiveStream.Link"/> reused.
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
/// </summary>
/// <param name="BandwidthMbps">libsrt's own estimate of the link's capacity, direction-agnostic.</param>
/// <param name="SendRateMbps">
/// What is actually leaving, libsrt's own measurement rather than a byte count divided by
/// wall-clock time.
/// </param>
/// <param name="RoundTripTimeMs">The measured round trip on this connection.</param>
/// <param name="PacketsRetransmitted">
/// Packets this sender resent because the far end reported one lost.
/// </param>
/// <param name="NegotiatedLatencyMs">
/// The latency window this end of the connection actually negotiated.
/// </param>
public sealed record SrtForwardLinkStats(
    double BandwidthMbps,
    double SendRateMbps,
    double RoundTripTimeMs,
    int PacketsRetransmitted,
    int NegotiatedLatencyMs);

/// <summary>A standing instruction about one stream name: fetch it from here, and copy it to there.</summary>
/// <param name="Url">Where to pull from, or null when an encoder brings the stream in by itself.</param>
/// <param name="Enabled">
/// False parks the source: it stays in the list and this service stops acting on it.
/// </param>
public sealed record LiveSource(
    string Name,
    string? Url,
    bool Enabled,
    IReadOnlyList<ForwardTarget> Forwards,
    DateTimeOffset UpdatedAt)
{
    /// <summary>True when this service is meant to fetch the stream rather than wait for it.</summary>
    [JsonIgnore]
    public bool IsPull => !string.IsNullOrWhiteSpace(Url);
}

/// <summary>
/// Where configured sources are kept, so every replica agrees on what is meant to be running.
/// </summary>
public interface ILiveSourceStore
{
    Task<IReadOnlyList<LiveSource>> ListAsync(CancellationToken cancellationToken = default);

    Task<LiveSource?> GetAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces the whole row.</summary>
    Task SaveAsync(LiveSource source, CancellationToken cancellationToken = default);

    Task RemoveAsync(string name, CancellationToken cancellationToken = default);
}
