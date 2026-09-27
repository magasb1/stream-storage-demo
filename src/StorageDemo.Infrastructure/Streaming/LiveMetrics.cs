using System.Diagnostics.Metrics;
using System.Globalization;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// What one heartbeat pass found, published to the meter as a set so the six numbers describe one
/// moment rather than six.
/// </summary>
/// <param name="Streams">Streams this replica holds the connection for, interrupted ones included.</param>
/// <param name="Interrupted">
/// How many of those have stopped receiving but are inside their grace period.
/// </param>
/// <param name="Unstartable">
/// How many have sent no position a decoder could start from for longer than the buffer holds.
/// </param>
/// <param name="Viewers">Players pulling streams from this replica, direct and relayed alike.</param>
/// <param name="Recordings">Recordings running here, each of which is writing a document.</param>
/// <param name="Forwards">Configured copies this replica is pushing to a far end.</param>
public sealed record LiveCensus(
    int Streams,
    int Interrupted,
    int Unstartable,
    int Viewers,
    int Recordings,
    int Forwards)
{
    public static readonly LiveCensus Empty = new(0, 0, 0, 0, 0, 0);
}

/// <summary>
/// What one stream carried since the meter was last told about it: the interval, never the total.
/// </summary>
public readonly record struct StreamFeed(
    long Packets,
    long Bytes,
    long KlvPackets,
    long KlvRejected,
    int PacketsLost,
    int PacketsDropped);

/// <summary>
/// What this pod publishes about itself, for a dashboard, an alert, an autoscaler and for anyone
/// holding <c>dotnet-counters monitor --counters StorageDemo.Live</c>.
/// </summary>
public sealed class LiveMetrics : IDisposable
{
    public const string MeterName = "StorageDemo.Live";

    /// <summary>Where Linux keeps the UDP counters.</summary>
    private const string Snmp = "/proc/net/snmp";

    private readonly Meter _meter;

    private readonly Counter<long> _accepts;

    private readonly Counter<long> _rejects;

    private readonly Counter<long> _packets;

    private readonly Counter<long> _bytes;

    private readonly Counter<long> _lost;

    private readonly Counter<long> _dropped;

    private readonly Counter<long> _klv;

    private readonly Counter<long> _klvRejected;

    private readonly Counter<long> _claims;

    private readonly Counter<long> _ended;

    private readonly Counter<long> _viewerSessions;

    private readonly Counter<long> _overflows;

    private readonly Counter<long> _snapshots;

    private readonly Counter<long> _recordings;

    private readonly Counter<long> _recordedBytes;

    private readonly Counter<long> _detections;

    private readonly Histogram<double> _heartbeat;

    private readonly Counter<long> _heartbeatFailures;

    private LiveCensus _census = LiveCensus.Empty;

    public LiveMetrics()
    {
        // Scoped to this instance so that two hosts in one process - which is how the replica tests
        // run - publish distinguishable meters rather than one name twice.
        _meter = new Meter(new MeterOptions(MeterName) { Scope = this });

        // Up-down counters rather than gauges, which is what the documentation prescribes for the
        // size of a set.
        _meter.CreateObservableUpDownCounter(
            "live.streams.owned",
            () => Census.Streams,
            description: "Live streams this replica currently holds the connection for.");

        _meter.CreateObservableUpDownCounter(
            "live.streams.interrupted",
            () => Census.Interrupted,
            description: "Of those, the ones whose feed has stopped inside its grace period.");

        _meter.CreateObservableUpDownCounter(
            "live.streams.unstartable",
            () => Census.Unstartable,
            description: "Of those, the ones with no decodable position inside the buffer window.");

        _meter.CreateObservableUpDownCounter(
            "live.viewers",
            () => Census.Viewers,
            description: "Players pulling a stream from this replica, direct and relayed alike.");

        _meter.CreateObservableUpDownCounter(
            "live.recordings.active",
            () => Census.Recordings,
            description: "Recordings running on this replica.");

        _meter.CreateObservableUpDownCounter(
            "live.forwards.active",
            () => Census.Forwards,
            description: "Configured copies this replica is pushing to a far end.");

        _accepts = _meter.CreateCounter<long>(
            "live.accepts",
            description: "Callers accepted, by the port they arrived on.");

        _rejects = _meter.CreateCounter<long>(
            "live.rejects",
            description: "Callers refused at the handshake, by port and by why.");

        _packets = _meter.CreateCounter<long>(
            "live.packets",
            unit: "{packet}",
            description: "Packets demultiplexed into a hub on this replica.");

        _bytes = _meter.CreateCounter<long>(
            "live.bytes",
            unit: "By",
            description: "Bytes demultiplexed into a hub on this replica.");

        _lost = _meter.CreateCounter<long>(
            "live.packets.lost",
            unit: "{packet}",
            description: "Packets libsrt never received on a feed this replica holds.");

        _dropped = _meter.CreateCounter<long>(
            "live.packets.dropped",
            unit: "{packet}",
            description: "Packets that arrived too late for the latency window to play them.");

        _klv = _meter.CreateCounter<long>(
            "live.klv.packets",
            unit: "{packet}",
            description: "MISB KLV packets the extractor decoded.");

        _klvRejected = _meter.CreateCounter<long>(
            "live.klv.rejected",
            unit: "{packet}",
            description: "KLV packets that were ST 0601 but failed their checksum.");

        _claims = _meter.CreateCounter<long>(
            "live.claims",
            description: "Attempts to own a name, by what came of it.");

        _ended = _meter.CreateCounter<long>(
            "live.streams.ended",
            description: "Streams this replica stopped holding, by why.");

        _viewerSessions = _meter.CreateCounter<long>(
            "live.viewer.sessions",
            description: "Viewers served, by whether this replica owned the stream or fetched it.");

        _overflows = _meter.CreateCounter<long>(
            "live.overflows",
            description: "Subscribers that fell behind the fan-out, by what they do about it.");

        _snapshots = _meter.CreateCounter<long>(
            "live.snapshots",
            description: "Snapshots asked for, by what could be captured.");

        _recordings = _meter.CreateCounter<long>(
            "live.recordings",
            description: "Recordings finished, by what they produced.");

        _recordedBytes = _meter.CreateCounter<long>(
            "live.recorded.bytes",
            unit: "By",
            description: "Bytes stored by recordings, counted as each part is stored.");

        _detections = _meter.CreateCounter<long>(
            "live.detections",
            description: "VMTI frames workers posted back to this replica.");

        _heartbeat = _meter.CreateHistogram<double>(
            "live.heartbeat.duration",
            unit: "s",
            description: "How long one heartbeat pass took.",
            tags: null,
            // Buckets of its own, because the default set was chosen for milliseconds: a pass that
            // takes a tenth of a second would land in the first bucket along with one that takes
            // four, and every percentile would read zero.
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
            });

        _heartbeatFailures = _meter.CreateCounter<long>(
            "live.heartbeat.failures",
            description: "Heartbeat work that threw and was swallowed, by which part.");

        _meter.CreateObservableCounter(
            "live.udp.receive.errors",
            ObserveReceiveErrors,
            description: "Kernel UDP receive errors for this pod's whole network namespace.");
    }

    /// <summary>
    /// What the heartbeat last found, and the only writer of every observable instrument above.
    /// </summary>
    public LiveCensus Census
    {
        get => Volatile.Read(ref _census);
        set => Volatile.Write(ref _census, value);
    }

    /// <summary>How many streams this replica holds.</summary>
    public int StreamsOwned
    {
        get => Census.Streams;
        set => Census = Census with { Streams = value };
    }

    public void Accepted(int port)
        => _accepts.Add(1, new KeyValuePair<string, object?>("port", port));

    /// <param name="reason">
    /// One of a handful of words, never anything derived from what the caller sent.
    /// </param>
    public void Rejected(int port, string reason)
        => _rejects.Add(
            1,
            new KeyValuePair<string, object?>("port", port),
            new KeyValuePair<string, object?>("reason", reason));

    /// <summary>
    /// Takes one stream's interval: what it carried and what its transport lost, untagged, so a
    /// thousand streams are one time series each rather than a thousand.
    /// </summary>
    public void Fed(in StreamFeed feed)
    {
        // Zeros are skipped rather than added.
        if (feed.Packets > 0)
        {
            _packets.Add(feed.Packets);
        }

        if (feed.Bytes > 0)
        {
            _bytes.Add(feed.Bytes);
        }

        if (feed.KlvPackets > 0)
        {
            _klv.Add(feed.KlvPackets);
        }

        if (feed.KlvRejected > 0)
        {
            _klvRejected.Add(feed.KlvRejected);
        }

        if (feed.PacketsLost > 0)
        {
            _lost.Add(feed.PacketsLost);
        }

        if (feed.PacketsDropped > 0)
        {
            _dropped.Add(feed.PacketsDropped);
        }
    }

    /// <param name="outcome">
    /// <c>taken</c> for a name nobody held, <c>resumed</c> for the same stream carrying on - a
    /// reconnect, or a move from a replica that has gone - and <c>refused</c> where another replica
    /// holds it.
    /// </param>
    public void Claimed(string outcome)
        => _claims.Add(1, new KeyValuePair<string, object?>("outcome", outcome));

    /// <param name="reason">
    /// Why the stream stopped being this replica's: <c>stopped</c>, <c>expired</c>,
    /// <c>source-off</c>, <c>displaced</c> or <c>shutdown</c>.
    /// </param>
    public void Ended(string reason)
        => _ended.Add(1, new KeyValuePair<string, object?>("reason", reason));

    /// <param name="route">
    /// <c>direct</c> when this replica owns the stream the viewer asked for, <c>relayed</c> when it
    /// fetched it from the replica that does.
    /// </param>
    public void Viewing(string route)
        => _viewerSessions.Add(1, new KeyValuePair<string, object?>("route", route));

    public void Overflowed(OverflowPolicy policy)
        => _overflows.Add(
            1,
            new KeyValuePair<string, object?>(
                "policy",
                policy == OverflowPolicy.Fail ? "fail" : "skip-to-live"));

    /// <param name="outcome">
    /// <c>stored</c> for a fresh decode, <c>preview</c> where the feed had sent no keyframe recent
    /// enough and the harvester's older, smaller picture was stored instead, and <c>none</c> where
    /// there was nothing to capture at all.
    /// </param>
    public void Snapshotted(string outcome)
        => _snapshots.Add(1, new KeyValuePair<string, object?>("outcome", outcome));

    /// <param name="outcome">
    /// <c>stored</c>, <c>truncated</c> where the recorder ran out of queue, <c>empty</c> where the
    /// stream produced nothing to store, or <c>failed</c>.
    /// </param>
    public void Recorded(string outcome)
        => _recordings.Add(1, new KeyValuePair<string, object?>("outcome", outcome));

    /// <summary>Counted as each part is stored, so a six-hour recording reports while it runs.</summary>
    public void RecordedBytes(long bytes)
    {
        if (bytes > 0)
        {
            _recordedBytes.Add(bytes);
        }
    }

    public void Detected() => _detections.Add(1);

    public void Beat(TimeSpan elapsed) => _heartbeat.Record(elapsed.TotalSeconds);

    /// <param name="stage">
    /// <c>stream</c> for one stream's pass, <c>registry</c> for the shared listing, <c>sources</c>
    /// for the configured-source reconcile.
    /// </param>
    public void BeatFailed(string stage)
        => _heartbeatFailures.Add(1, new KeyValuePair<string, object?>("stage", stage));

    /// <summary>
    /// UDP datagrams the kernel counted as receive errors since boot, or null where there is no
    /// such number to read.
    /// </summary>
    public static long? KernelUdpReceiveErrors()
    {
        try
        {
            string? header = null;

            foreach (var line in File.ReadLines(Snmp))
            {
                if (!line.StartsWith("Udp:", StringComparison.Ordinal))
                {
                    continue;
                }

                // The file gives each protocol twice: the column names, then the values.
                if (header is null)
                {
                    header = line;

                    continue;
                }

                var columns = header.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var values = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var at = Array.IndexOf(columns, "InErrors");

                return at > 0 && at < values.Length
                    && long.TryParse(values[at], CultureInfo.InvariantCulture, out var errors)
                        ? errors
                        : null;
            }

            return null;
        }
        catch (Exception)
        {
            // A health signal that throws is worse than one that is absent, and every way this can
            // fail - no such file, no permission, a kernel that words the file differently - has
            // the same answer.
            return null;
        }
    }

    public void Dispose() => _meter.Dispose();

    private static IEnumerable<Measurement<long>> ObserveReceiveErrors()
        => KernelUdpReceiveErrors() is { } errors ? [new Measurement<long>(errors)] : [];
}
