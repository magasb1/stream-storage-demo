using Microsoft.Extensions.Logging;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// The per-stream fan-out point. One demultiplexer feeds one hub; packets flow one way through it.
///
/// Two tiers hang off this. The packet tier is the fan-out itself: subscribers receive
/// demultiplexed packets filtered by stream index, with no decoding anywhere near them, which is
/// what keeps a stream cheap when nobody is watching. The recorder, the viewer and a future KLV
/// extractor all live there, and KLV needs no special path because it is a subscriber on a
/// different stream index. The frame tier hangs off the packet tier as one decoder that is itself
/// a packet subscriber, so a stream is decoded once however many things want pictures and
/// consumers that only move bytes never pay for it.
///
/// The hub outlives the demultiplexer that feeds it. A feed that stops arriving keeps its hub, its
/// buffer and any recording alive through the grace period, and a reconnect under the same name
/// resumes into this same hub rather than creating a second stream.
/// </summary>
public sealed class StreamHub : IDisposable
{
    private readonly ILogger _logger;
    private readonly LiveMetrics? _metrics;
    private readonly Lock _gate = new();

    /// <summary>Copy-on-write, so publishing never takes a lock on the demultiplexer's thread.</summary>
    private PacketSubscription[] _subscribers = [];

    private StreamLayout? _layout;

    /// <summary>What <see cref="Synthesise"/> was last told to append to the next layout.</summary>
    private IReadOnlyList<SyntheticTrack> _synthetic = [];

    /// <summary>
    /// Where the arriving feed has reached on the reference clock, or null before anything has
    /// arrived on this connection. See <see cref="PublishAtLiveEdge"/>, which is all it is for.
    /// </summary>
    private long? _liveEdge;

    /// <summary>
    /// Layouts this stream has had. They are kept rather than freed on replacement because a
    /// decoder or a muxer may still be holding one: freeing underneath a consumer would be a
    /// use-after-free for the sake of a few hundred bytes per reconnect.
    /// </summary>
    private readonly List<StreamLayout> _retired = [];

    /// <param name="metrics">
    /// Where a subscriber that falls behind is reported, or null where nothing is measuring: the
    /// detection worker builds a hub of its own and so do the tests, and neither publishes a meter.
    /// The hub itself measures nothing - it already counts packets and bytes as part of publishing
    /// them, and the heartbeat reports the interval since it last looked, which is what keeps the
    /// demultiplexer's thread free of instrument calls.
    /// </param>
    public StreamHub(string name, LiveOptions options, ILogger logger, LiveMetrics? metrics = null)
    {
        Name = name;
        _logger = logger;
        Options = options;
        _metrics = metrics;
    }

    public string Name { get; }

    public LiveOptions Options { get; }

    /// <summary>
    /// What the stream is carrying, or null before the first packet has been demultiplexed.
    /// Replaced when a reconnect brings a different shape, which is also when a recording in
    /// progress has to close.
    /// </summary>
    public StreamLayout? Layout
    {
        get { lock (_gate) { return _layout; } }
    }

    /// <summary>Null until a layout is known, because a buffer needs a clock to measure itself by.</summary>
    public RollingBuffer? Buffer { get; private set; }

    /// <summary>
    /// Tracks to append to the next layout this hub adopts - metadata this replica produces for a
    /// stream whose sender has none of its own.
    ///
    /// Read at <see cref="Adopt"/> and never afterwards, which is what makes a configuration
    /// change take effect on the next connection rather than under a running one. Swapping a live
    /// layout would break every consumer already attached to it, since none of them re-reads it;
    /// see <see cref="StreamLayout"/>'s synthetic-track overload of <c>From</c>.
    /// </summary>
    public void Synthesise(IReadOnlyList<SyntheticTrack> tracks)
    {
        lock (_gate)
        {
            _synthetic = tracks;
        }
    }

    /// <summary>What <see cref="Synthesise"/> was last told, for the demultiplexer about to adopt.</summary>
    public IReadOnlyList<SyntheticTrack> SyntheticTracks
    {
        get { lock (_gate) { return _synthetic; } }
    }

    public long Packets { get; private set; }

    public long Bytes { get; private set; }

    /// <summary>When a packet last arrived. The grace period is measured from this.</summary>
    public DateTimeOffset? LastPacketAt { get; private set; }

    /// <summary>Set when the hub has stopped for good and nothing should attach to it again.</summary>
    public bool Closed { get; private set; }

    /// <summary>
    /// Adopts the layout a freshly connected demultiplexer reports.
    /// </summary>
    /// <returns>
    /// True when the buffer and any recording carry on, false when the shape changed and they
    /// have to start again. A resumed feed appends when its layout matches, as a new segment
    /// boundary; when the encoder was reconfigured while it was away, the file already being
    /// written cannot hold what comes next.
    /// </returns>
    public bool Adopt(StreamLayout layout)
    {
        lock (_gate)
        {
            var matches = _layout is null || _layout.Matches(layout);

            // Forgotten with the connection that established it. A returning encoder counts from
            // its own zero again and the buffer shifts everything after it onto its own clock; a
            // synthetic packet stamped at the departed feed's edge in between would be the packet
            // that shift is computed from, and every real packet after it would inherit it.
            _liveEdge = null;

            if (_layout is not null)
            {
                _retired.Add(_layout);
            }

            _layout = layout;

            if (matches && Buffer is not null)
            {
                // The buffer survives, so the clock it is measured on has to as well: this feed
                // starts counting from its own beginning and the buffer is somewhere else.
                Buffer.Resume();
                return true;
            }

            Buffer = new RollingBuffer(
                Options.BufferWindowSeconds,
                Options.BufferByteCeiling,
                layout.SecondsPerTick);

            return matches;
        }
    }

    /// <summary>
    /// Attaches a consumer to the packet tier.
    /// </summary>
    /// <param name="streamIndexes">
    /// Which streams to deliver, or empty for all of them. A KLV extractor would name one index
    /// and never see a video packet.
    /// </param>
    /// <param name="preroll">
    /// How far back in the buffer to start. Zero joins at the live edge. The actual start is the
    /// segment at or before that point, so a caller routinely gets more than it asked for.
    /// </param>
    public PacketSubscription Subscribe(
        int capacity,
        OverflowPolicy policy,
        int[] streamIndexes,
        double preroll = 0)
    {
        PacketSubscription subscription = null!;

        subscription = new PacketSubscription(
            capacity,
            policy,
            streamIndexes,
            () => Detach(subscription),
            _metrics);

        lock (_gate)
        {
            if (Closed)
            {
                subscription.Complete();
                return subscription;
            }

            // Filled under the same lock that publishing takes, so a packet cannot slip between
            // the history and the live flow and leave a hole in the middle of a recording.
            if (Buffer is { } buffer && buffer.StartingFrom(preroll) is { } from)
            {
                foreach (var packet in buffer.PacketsFrom(from))
                {
                    subscription.Offer(packet, startsSegment: false);
                }
            }

            _subscribers = [.. _subscribers, subscription];
        }

        return subscription;
    }

    /// <summary>How far back a subscription asking for this much would actually begin.</summary>
    public double ResolvePreroll(double seconds)
    {
        lock (_gate)
        {
            return Buffer is { } buffer && buffer.StartingFrom(seconds) is { } from
                ? buffer.SecondsBackTo(from)
                : 0;
        }
    }

    /// <summary>
    /// A copy of the newest position a decoder can start from, or null when there is none.
    ///
    /// A copy, and taken under the same lock publishing takes, because the newest segment is the
    /// one still being written: a caller iterating it directly races the demultiplexer appending
    /// to it, and the buffer says of itself that the hub serialises access. Reaching past that
    /// into <see cref="Buffer"/> is what made a snapshot fail with "collection was modified"
    /// roughly one time in thirty, and muxing takes long enough to make the window wide.
    /// </summary>
    public MediaPacket[]? NewestStartablePackets()
    {
        lock (_gate)
        {
            return Buffer?.Newest() is { } segment ? [.. segment.Packets] : null;
        }
    }

    /// <summary>
    /// What the buffer holds, read in one lock so the three answers describe one moment and
    /// neither of the first two indexes a list the demultiplexer is appending to.
    /// </summary>
    public (double HeldSeconds, bool Startable, bool CeilingBinding) BufferState()
    {
        lock (_gate)
        {
            return Buffer is { } buffer
                ? (buffer.HeldSeconds, buffer.NotStartableSince is null, buffer.CeilingBindingSince is not null)
                : (0, true, false);
        }
    }

    /// <summary>
    /// Takes one packet from the demultiplexer: into the buffer, then out to every subscriber.
    /// </summary>
    public void Publish(MediaPacket packet, long referencePts)
    {
        var startsSegment = StartsSegment(packet);

        lock (_gate)
        {
            if (Closed)
            {
                return;
            }

            _liveEdge = referencePts;

            Packets++;
            Bytes += packet.Bytes;
            LastPacketAt = DateTimeOffset.UtcNow;

            Offer(packet, startsSegment, referencePts);
        }
    }

    /// <summary>
    /// Publishes a packet this replica produced rather than received, stamped where the arriving
    /// feed has got to.
    ///
    /// The stamping is here, under the same lock the arriving packets are published under, rather
    /// than left to the caller, because the two rules it has to satisfy are both about the live
    /// edge and both are unforgiving:
    ///
    /// - More than a second behind it and <c>PacketMuxer.Rebase</c> reads the packet as an encoder
    ///   that has restarted its clock and rebases the whole timeline onto it, which corrupts it for
    ///   every viewer, forward and recording on the stream.
    /// - Ahead of it and <c>Segment.Add</c> advances the buffer's <c>EndPts</c>, which shrinks
    ///   <c>HeldSeconds</c> and evicts the oldest segment before its time.
    ///
    /// Read outside the lock, the edge can be a reconnect old by the time the packet is published -
    /// a returning encoder counts from its own zero again - and that is precisely the first case.
    /// Exactly at the edge is the one stamp that cannot be either.
    /// </summary>
    /// <returns>
    /// False when nothing has arrived yet, so there is no edge to stamp against, or when the hub
    /// has closed. A caller publishing on a schedule tries again on its next tick.
    /// </returns>
    public bool PublishAtLiveEdge(int streamIndex, byte[] data)
    {
        lock (_gate)
        {
            if (Closed || _liveEdge is not { } edge)
            {
                return false;
            }

            // Duration zero: a metadata item describes an instant, not a span. A keyframe because
            // nothing about it depends on a packet before it, which is what every consumer's
            // muxer wants to hear about a track it may be joining mid-flight.
            var packet = new MediaPacket(streamIndex, data, edge, edge, Duration: 0, IsKeyframe: true);

            // Never starting a segment, whatever StartsSegment would say. A segment boundary is
            // where a viewer may join, and joining on a metadata packet gives it no picture; on a
            // transport with no video at all StartsSegment would otherwise say yes to this, on the
            // rule that every packet of an audio-only stream is a starting point.
            Offer(packet, startsSegment: false, edge);

            return true;
        }
    }

    /// <summary>
    /// The buffer and the fan-out, for a packet however it was produced. Assumes <see cref="_gate"/>
    /// and that the hub is open.
    ///
    /// The counters are not here, and that is the point of the split. <see cref="LastPacketAt"/> is
    /// what the grace period is measured from: moved by a packet this replica produced, a stream
    /// whose camera had gone away would report itself live for as long as the process ran, and
    /// would never be interrupted, never expire and never be released to another replica.
    /// <see cref="Packets"/> and <see cref="Bytes"/> are the feed's own totals and the meter turns
    /// them into an ingest rate, so they stay what arrived. What the synthetic track carries is
    /// counted where it is decoded, by <see cref="KlvExtractor"/>, and reported as this stream's
    /// KLV rate like any other metadata.
    /// </summary>
    private void Offer(MediaPacket packet, bool startsSegment, long referencePts)
    {
        Buffer?.Add(packet, startsSegment, referencePts);

        foreach (var subscriber in _subscribers)
        {
            subscriber.Offer(packet, startsSegment);
        }
    }

    /// <summary>
    /// Whether a decoder could begin here. A stream carrying video says so with a keyframe; one
    /// carrying only audio can be joined anywhere, so every packet qualifies.
    /// </summary>
    private bool StartsSegment(MediaPacket packet)
    {
        var video = _layout?.VideoIndex ?? -1;

        return video < 0 ? packet.IsKeyframe : packet.StreamIndex == video && packet.IsKeyframe;
    }

    /// <summary>Ends the hub. Every attached consumer sees its queue finish rather than stall.</summary>
    public void Close()
    {
        PacketSubscription[] subscribers;

        lock (_gate)
        {
            if (Closed)
            {
                return;
            }

            Closed = true;
            subscribers = _subscribers;
            _subscribers = [];
        }

        foreach (var subscriber in subscribers)
        {
            subscriber.Complete();
        }

        _logger.LogInformation(
            "Stream '{Name}' closed after {Packets} packets and {Bytes} bytes",
            Name,
            Packets,
            Bytes);
    }

    private void Detach(PacketSubscription subscription)
    {
        lock (_gate)
        {
            _subscribers = [.. _subscribers.Where(existing => !ReferenceEquals(existing, subscription))];
        }
    }

    public void Dispose()
    {
        Close();

        lock (_gate)
        {
            foreach (var layout in _retired)
            {
                layout.Dispose();
            }

            _retired.Clear();

            _layout?.Dispose();
            _layout = null;
            Buffer?.Clear();
        }
    }
}
