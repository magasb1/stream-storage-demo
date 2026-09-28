using System.Diagnostics.Metrics;
using System.Globalization;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// What one heartbeat pass found, published to the meter as a set so the six numbers describe one
/// moment rather than six.
///
/// A record rather than six fields, because an observable instrument is read on the collection
/// thread and that thread must never take a lock: one reference swapped under
/// <see cref="LiveMetrics.Census"/> means every instrument reads a whole census rather than a
/// half-written set of six.
///
/// It narrows the window rather than closing it. A collection pass calls the six callbacks one
/// after another and the heartbeat can swap the reference between two of them, so a scrape can
/// still straddle two beats and show, say, a stream count from one and an interrupted count from
/// the next. What that costs is two seconds of skew on figures that are a beat stale anyway; what
/// the record buys is that neither figure is ever torn. Closing it entirely would mean one
/// instrument with a state tag, since only measurements from the same callback are collected
/// together, and that is a worse shape for six figures in four different units.
/// </summary>
/// <param name="Streams">Streams this replica holds the connection for, interrupted ones included.</param>
/// <param name="Interrupted">
/// How many of those have stopped receiving but are inside their grace period. A pod whose whole
/// census is interrupted has lost its senders rather than its streams, which is a different fault
/// and a different fix.
/// </param>
/// <param name="Unstartable">
/// How many have sent no position a decoder could start from for longer than the buffer holds.
/// Every pre-roll from such a stream is empty and every snapshot of it is second-hand, and the fix
/// is at the encoder rather than here.
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
///
/// Deltas rather than the hub's running totals because a counter is added to, and adding a total
/// every beat would multiply a stream's traffic by the number of beats it ran for. The two
/// transport figures are already intervals, since libsrt clears them when they are read.
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
///
/// Aggregates only. The per-stream question - which of my thousand streams is broken - is answered
/// by <c>GET /api/live</c>, which already lists every stream and carries each one's loss and drop
/// figures. Nothing here is tagged with a stream name, and nothing here should ever be: a thousand
/// streams times several instruments is thousands of time series, every one of them retained by
/// whatever scrapes it long after the stream ended, to answer a question the API answers for free.
/// Every tag on this class has a small, fixed set of values, and no tag value is ever derived from
/// something a caller sent.
///
/// Nothing here is measured on the demultiplexer's thread either, which is the other half of the
/// same discipline. Packets and bytes are counted by the hub as part of publishing them, because it
/// is holding the lock and incrementing two fields anyway, and the heartbeat reports the interval
/// since it last looked. A counter call per packet per stream would be a million instrument
/// operations a second at the scale this service is sized for, to produce a number that is exact
/// either way.
///
/// The exporter lives in the API, in <c>Observability/Telemetry.cs</c>, and is off unless a
/// collector is configured: OTLP out, and no metrics endpoint on a process that already listens on
/// public ports. The detection worker publishes <c>live.detection.duration</c> on this same meter
/// name from its own process and has no exporter yet. See <c>docs/observability.md</c>.
/// </summary>
public sealed class LiveMetrics : IDisposable
{
    public const string MeterName = "StorageDemo.Live";

    /// <summary>
    /// Where Linux keeps the UDP counters. Nothing else in this service reads /proc, so the path
    /// lives here rather than in a constants file with one member.
    /// </summary>
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

    private readonly Counter<long> _viewersDropped;

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
        // size of a set. Observed rather than incremented, because every one of them counts a
        // collection that several threads add to and remove from, and the count is the only thing
        // that has to be right.
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

        // The two transport failures, and they are not the same failure. Lost never arrived and
        // could not be retransmitted in time, which is the network or a saturated receive path;
        // dropped did arrive, too late for the latency window, which is usually the window being
        // too small for the link. A dashboard that adds them together loses the distinction that
        // decides what to do about it.
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

        // The only observable for a condition that cannot be screened for. A viewer that clears its
        // too-late-packet drop flag and then stops reading is indistinguishable from a healthy one
        // at accept time - SRTO_TLPKTDROP read back on an accepted socket answers with our own
        // setting, not the peer's - so Admit cannot refuse it and nothing else in this service ever
        // names it. Without this counter the only trace of a viewer held for its whole send budget
        // and then dropped is one log line.
        _viewersDropped = _meter.CreateCounter<long>(
            "live.viewer.dropped",
            description: "Viewers dropped for accepting no bytes within the send-stall budget.");

        // The fan-out's fault signal. A viewer overflowing skips forward and loses a moment; a
        // recorder overflowing stops and marks its document truncated. Both mean this pod could not
        // keep up with a consumer, which is the failure a byte count cannot show.
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

        // The control plane's own latency, and the one measurement here that is about this service
        // rather than about media. The beat is two seconds; a pass that takes longer than that is a
        // registry every other replica is reading stale, which is how two pods come to admit one
        // name. It is the first thing to look at when a cluster starts disagreeing with itself.
        _heartbeat = _meter.CreateHistogram<double>(
            "live.heartbeat.duration",
            unit: "s",
            description: "How long one heartbeat pass took.",
            tags: null,
            // Buckets of its own, because the default set was chosen for milliseconds: a pass that
            // takes a tenth of a second would land in the first bucket along with one that takes
            // four, and every percentile would read zero. These straddle the two-second beat, which
            // is the number anyone reading this histogram is comparing against.
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
            });

        _heartbeatFailures = _meter.CreateCounter<long>(
            "live.heartbeat.failures",
            description: "Heartbeat work that threw and was swallowed, by which part.");

        // The counter that sees what never reached libsrt. Every per-socket figure is blind to it:
        // a packet dropped by the kernel because the receive buffer was full was never delivered to
        // any socket, so the only place it is counted is here. It is what the 250-stream baseline
        // actually hit - 319,000 of these in fifteen seconds while every stream reported itself
        // healthy - and it is the reason a count of streams owned cannot be trusted alone.
        _meter.CreateObservableCounter(
            "live.udp.receive.errors",
            ObserveReceiveErrors,
            description: "Kernel UDP receive errors for this pod's whole network namespace.");
    }

    /// <summary>
    /// What the heartbeat last found, and the only writer of every observable instrument above.
    /// Read on the collection thread, which must never block, so it is one reference rather than a
    /// walk of a dictionary.
    /// </summary>
    public LiveCensus Census
    {
        get => Volatile.Read(ref _census);
        set => Volatile.Write(ref _census, value);
    }

    /// <summary>
    /// How many streams this replica holds. Kept as its own property because it is the one figure
    /// an autoscaler reads, and because the heartbeat knows it before it has walked anything.
    /// </summary>
    public int StreamsOwned
    {
        get => Census.Streams;
        set => Census = Census with { Streams = value };
    }

    public void Accepted(int port)
        => _accepts.Add(1, new KeyValuePair<string, object?>("port", port));

    /// <param name="reason">
    /// One of a handful of words, never anything derived from what the caller sent. A reason taken
    /// from a stream identifier would let whoever is connecting choose this metric's cardinality.
    /// </param>
    public void Rejected(int port, string reason)
        => _rejects.Add(
            1,
            new KeyValuePair<string, object?>("port", port),
            new KeyValuePair<string, object?>("reason", reason));

    /// <summary>
    /// Takes one stream's interval: what it carried and what its transport lost, untagged, so a
    /// thousand streams are one time series each rather than a thousand.
    ///
    /// Called once per beat per stream in the ordinary case, and exact when it is called more
    /// often, because every figure in a <see cref="StreamFeed"/> is an interval: the hub's deltas
    /// are measured against what was last reported, and libsrt's counters are cleared by the read
    /// that produced them.
    /// </summary>
    public void Fed(in StreamFeed feed)
    {
        // Zeros are skipped rather than added. A counter add is cheap, but nine hundred of a
        // thousand streams carry no KLV and none of them should cost an instrument operation a beat
        // to say so.
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
    /// holds it. Refusals are the ordinary outcome of the reconcile pass and are not a fault: every
    /// replica reaches for every configured source and all but one loses.
    /// </param>
    public void Claimed(string outcome)
        => _claims.Add(1, new KeyValuePair<string, object?>("outcome", outcome));

    /// <param name="reason">
    /// Why the stream stopped being this replica's: <c>stopped</c>, <c>expired</c>,
    /// <c>source-off</c>, <c>sensor-changed</c>, <c>displaced</c> or <c>shutdown</c>.
    /// <c>sensor-changed</c> is a pulled stream being reopened to pick up a change to its static
    /// sensor, which is the only way a metadata track can be added to or taken off a layout; it is
    /// a reconnect rather than a loss, and the name comes back on a later beat. A rolling update shows up as
    /// <c>shutdown</c> here and <c>resumed</c> on <see cref="Claimed"/> somewhere else, which is
    /// what a move looks like when nothing is lost.
    /// </param>
    public void Ended(string reason)
        => _ended.Add(1, new KeyValuePair<string, object?>("reason", reason));

    /// <param name="route">
    /// How the player reached the replica that owns its stream: <c>direct</c> when it connected to
    /// this replica's own consumption port, <c>relayed</c> when it reached another one and is being
    /// fetched from here. Counted by the owner either way, because that is where a viewer is
    /// actually served, so one viewer is one session however many pods it passed through. A cluster
    /// where most viewers are relayed is paying an extra hop for every one of them, which is a
    /// load-balancer question rather than a fault.
    /// </param>
    public void Viewing(string route)
        => _viewerSessions.Add(1, new KeyValuePair<string, object?>("route", route));

    /// <param name="route">
    /// Which socket stopped taking bytes. <c>direct</c> is the player's own SRT socket on this pod,
    /// which is every case today, whether the stream was owned here or fetched from another replica
    /// - the bytes leave by the same socket either way, and it is that socket's peer that chose not
    /// to read. The tag exists for the forward's dialled socket, which can be stalled by its own far
    /// end in the same way and is a separate change.
    ///
    /// A non-zero rate here is a viewer that cleared <c>SRTO_TLPKTDROP</c> and stopped reading, held
    /// <see cref="LiveOptions.ViewerSendStallSeconds"/> of a pool thread and about twelve megabytes
    /// of send buffer, and was dropped for it. An ordinary slow viewer never appears: libsrt discards
    /// from the send buffer for a peer that advertised drop, so its sends do not time out at all.
    /// </param>
    public void ViewerDropped(string route)
        => _viewersDropped.Add(1, new KeyValuePair<string, object?>("route", route));

    public void Overflowed(OverflowPolicy policy)
        => _overflows.Add(
            1,
            new KeyValuePair<string, object?>(
                "policy",
                policy == OverflowPolicy.Fail ? "fail" : "skip-to-live"));

    /// <param name="outcome">
    /// <c>stored</c> for a fresh decode, <c>preview</c> where the feed had sent no keyframe recent
    /// enough and the harvester's older, smaller picture was stored instead, and <c>none</c> where
    /// there was nothing to capture at all. The middle one is the interesting one: it says a
    /// stream's snapshots are quietly worse than they look.
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
    /// for the configured-source reconcile. All three are swallowed so that a Redis blip cannot
    /// take the ingest port down, which is exactly why they need counting: the log line is the only
    /// other trace they leave.
    /// </param>
    public void BeatFailed(string stage)
        => _heartbeatFailures.Add(1, new KeyValuePair<string, object?>("stage", stage));

    /// <summary>
    /// UDP datagrams the kernel counted as receive errors since boot, or null where there is no
    /// such number to read.
    ///
    /// Linux only, by nature: this is /proc/net/snmp's InErrors, the figure netstat prints as
    /// "packet receive errors". Windows and macOS have no equivalent that is worth faking, and a
    /// developer machine must not throw or report a comforting zero for a counter it cannot see, so
    /// the honest answer there is nothing at all.
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

                // The file gives each protocol twice: the column names, then the values. Read by
                // name rather than by position, because which columns exist depends on the kernel.
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
