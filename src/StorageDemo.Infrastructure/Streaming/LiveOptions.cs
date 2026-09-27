using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Infrastructure.Streaming;

public sealed class LiveOptions
{
    public const string SectionName = "Live";

    /// <summary>
    /// The named <see cref="HttpClient"/> one replica reaches another on, for a control call and
    /// for a relayed viewer's media alike.
    /// </summary>
    public const string PeerClient = "LivePeer";

    /// <summary>Off by default.</summary>
    public bool Enabled { get; init; }

    /// <summary>Required in the X-Storage-Token header when set.</summary>
    public string? Token { get; init; }

    /// <summary>Where encoders push.</summary>
    [Range(1, 65535)]
    public int IngestPort { get; init; } = 9000;

    /// <summary>How many consecutive ports ingest binds, starting at <see cref="IngestPort"/>.</summary>
    [Range(1, 16)]
    public int IngestPortCount { get; init; } = 1;

    /// <summary>Where viewers pull live streams, over SRT, symmetric with ingest.</summary>
    [Range(1, 65535)]
    public int ConsumptionPort { get; init; } = 9010;

    /// <summary>The address to listen on.</summary>
    public string IngestAddress { get; init; } = "0.0.0.0";

    /// <summary>How long a sender may fall silent before the transport gives up on it.</summary>
    [Range(1, 300)]
    public int FeedTimeoutSeconds { get; init; } = 5;

    /// <summary>
    /// How long an interrupted stream is kept before it is considered gone: still claimed, still
    /// listed, hub and buffer and any recording all still alive.
    /// </summary>
    [Range(1, 3600)]
    public int GracePeriodSeconds { get; init; } = 30;

    /// <summary>The receiver's buffering delay on both media ports, in milliseconds.</summary>
    [Range(0, 8000)]
    public int SrtLatencyMs { get; init; } = 120;

    /// <summary>
    /// How many streams this replica holds before it starts refusing new names at the handshake,
    /// with <c>SRT_REJX_OVERLOAD</c>.
    /// </summary>
    [Range(0, 100_000)]
    public int MaxStreams { get; init; }

    /// <summary>Identifies this replica.</summary>
    public string? NodeName { get; init; }

    /// <summary>
    /// The address other replicas reach this one on, recorded with the name claim, for example
    /// "http://10.42.0.9:8080".
    /// </summary>
    public string? PeerBaseUrl { get; init; }

    /// <summary>
    /// The SRT address to hand to a player, when this instance knows its own, for example
    /// "srt://live.example.com:9010".
    /// </summary>
    public string? PublicConsumptionUrl { get; init; }

    /// <summary>How often the preview is refreshed.</summary>
    [Range(1, 300)]
    public int PreviewIntervalSeconds { get; init; } = 2;

    /// <summary>How much recent stream every hub keeps, as a promise of at least this many seconds.</summary>
    [Range(5, 300)]
    public double BufferWindowSeconds { get; init; } = 30;

    /// <summary>
    /// The hard memory bound per stream, which is what stops one careless encoder evicting the
    /// service.
    /// </summary>
    [Range(1024 * 1024, 2L * 1024 * 1024 * 1024)]
    public long BufferByteCeiling { get; init; } = 96L * 1024 * 1024;

    /// <summary>
    /// How far either side of a trigger a recording reaches, so an event already under way when it
    /// was noticed is still caught.
    /// </summary>
    [Range(0, 60)]
    public double PrerollSeconds { get; init; } = 5;

    /// <summary>
    /// How long a recording runs past its trigger when no duration was given, and how much longer
    /// each further trigger extends it.
    /// </summary>
    [Range(1, 3600)]
    public double DefaultRecordingSeconds { get; init; } = 30;

    /// <summary>
    /// How much of a recording is muxed to a local file before it is stored and the file deleted.
    /// </summary>
    [Range(0.1, 60)]
    public double RecordingPartMinutes { get; init; } = 5;

    /// <summary>The ceiling on one recording, so a stream nobody stops still ends somewhere.</summary>
    [Range(1, 7 * 24 * 60)]
    public int MaxRecordingMinutes { get; init; } = 12 * 60;

    /// <summary>Queue depth for a viewer, in packets.</summary>
    [Range(64, 100_000)]
    public int ViewerQueuePackets { get; init; } = 2_000;

    /// <summary>Queue depth for a recorder.</summary>
    [Range(64, 1_000_000)]
    public int RecorderQueuePackets { get; init; } = 20_000;

    /// <summary>How long libav may spend working out what an arriving stream contains, in seconds.</summary>
    [Range(0.1, 30)]
    public double ProbeSeconds { get; init; } = 1;

    /// <summary>How many bytes libav may read while working that out.</summary>
    [Range(32 * 1024, 64 * 1024 * 1024)]
    public long ProbeBytes { get; init; } = 1024 * 1024;

    /// <summary>Where recordings are written before they are uploaded as documents.</summary>
    public string? RecordingDirectory { get; init; }

    /// <summary>
    /// Where the file-backed source store keeps its JSON, for the deployments that have no Redis.
    /// </summary>
    public string? SourceFile { get; init; }

    /// <summary>How long a forward that failed waits before it is tried again.</summary>
    [Range(1, 600)]
    public int ForwardRetrySeconds { get; init; } = 5;

    /// <summary>Transports a manual stream may name.</summary>
    public string[] AllowedSchemes { get; init; } = ["udp", "rtp", "srt"];

    /// <summary>Demuxer options for a manual input over udp, rtp or srt.</summary>
    public Dictionary<string, string> ManualInputOptions { get; init; } = new()
    {
        ["timeout"] = "30000000",
        ["fifo_size"] = "1000000",
    };

    /// <summary>
    /// Demuxer options for a manual input pulled over http or https, HLS included: libav auto-
    /// detects the HLS demuxer from the playlist it fetches over this same protocol, so one set of
    /// options covers a raw HTTP container and a live HLS playlist alike.
    /// </summary>
    public Dictionary<string, string> HttpInputOptions { get; init; } = new()
    {
        ["timeout"] = "10000000",
        ["reconnect"] = "1",
        ["reconnect_streamed"] = "1",
        ["reconnect_delay_max"] = "2",
    };

    /// <summary>
    /// How long an http or https pull may spend working out what it is, and how much it may read
    /// doing it - the same pair <see cref="ProbeSeconds"/> and <see cref="ProbeBytes"/> are for
    /// every other manual input, sized differently because the two are not comparable sources.
    /// </summary>
    [Range(0.1, 30)]
    public double HttpProbeSeconds { get; init; } = 5;

    [Range(32 * 1024, 64 * 1024 * 1024)]
    public long HttpProbeBytes { get; init; } = 4 * 1024 * 1024;
}
