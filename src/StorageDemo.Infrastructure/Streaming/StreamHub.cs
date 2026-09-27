using Microsoft.Extensions.Logging;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>The per-stream fan-out point.</summary>
public sealed class StreamHub : IDisposable
{
    private readonly ILogger _logger;
    private readonly LiveMetrics? _metrics;
    private readonly Lock _gate = new();

    /// <summary>Copy-on-write, so publishing never takes a lock on the demultiplexer's thread.</summary>
    private PacketSubscription[] _subscribers = [];

    private StreamLayout? _layout;

    /// <summary>Layouts this stream has had.</summary>
    private readonly List<StreamLayout> _retired = [];

    /// <param name="metrics">
    /// Where a subscriber that falls behind is reported, or null where nothing is measuring: the
    /// detection worker builds a hub of its own and so do the tests, and neither publishes a meter.
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
    /// </summary>
    public StreamLayout? Layout
    {
        get { lock (_gate) { return _layout; } }
    }

    /// <summary>Null until a layout is known, because a buffer needs a clock to measure itself by.</summary>
    public RollingBuffer? Buffer { get; private set; }

    public long Packets { get; private set; }

    public long Bytes { get; private set; }

    /// <summary>When a packet last arrived.</summary>
    public DateTimeOffset? LastPacketAt { get; private set; }

    /// <summary>Set when the hub has stopped for good and nothing should attach to it again.</summary>
    public bool Closed { get; private set; }

    /// <summary>Adopts the layout a freshly connected demultiplexer reports.</summary>
    public bool Adopt(StreamLayout layout)
    {
        lock (_gate)
        {
            var matches = _layout is null || _layout.Matches(layout);

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

    /// <summary>Attaches a consumer to the packet tier.</summary>
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

            // Filled under the same lock that publishing takes, so a packet cannot slip between the
            // history and the live flow and leave a hole in the middle of a recording.
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

    /// <summary>A copy of the newest position a decoder can start from, or null when there is none.</summary>
    public MediaPacket[]? NewestStartablePackets()
    {
        lock (_gate)
        {
            return Buffer?.Newest() is { } segment ? [.. segment.Packets] : null;
        }
    }

    /// <summary>
    /// What the buffer holds, read in one lock so the three answers describe one moment and neither
    /// of the first two indexes a list the demultiplexer is appending to.
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

            Buffer?.Add(packet, startsSegment, referencePts);

            Packets++;
            Bytes += packet.Bytes;
            LastPacketAt = DateTimeOffset.UtcNow;

            foreach (var subscriber in _subscribers)
            {
                subscriber.Offer(packet, startsSegment);
            }
        }
    }

    /// <summary>Whether a decoder could begin here.</summary>
    private bool StartsSegment(MediaPacket packet)
    {
        var video = _layout?.VideoIndex ?? -1;

        return video < 0 ? packet.IsKeyframe : packet.StreamIndex == video && packet.IsKeyframe;
    }

    /// <summary>Ends the hub.</summary>
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
