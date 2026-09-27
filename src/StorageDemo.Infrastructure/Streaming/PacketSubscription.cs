using System.Threading.Channels;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>What a subscriber wants done when it cannot keep up.</summary>
public enum OverflowPolicy
{
    /// <summary>
    /// Throw the backlog away and rejoin at the next position a decoder can start from.
    ///
    /// A viewer never accumulates delay. Deliberate rollback is a chosen position and is not this;
    /// unintended lag is a fault, and the correction is to stop being late.
    /// </summary>
    SkipToLive,

    /// <summary>
    /// Stop, and let whoever owns this know it stopped.
    ///
    /// A recorder overflowing is a hard failure. Dropping packets to keep going would write a hole
    /// into a file that claims to be a recording, and silence is worse than stopping.
    /// </summary>
    Fail,
}

/// <summary>
/// One consumer's attachment to a hub: a bounded queue and a rule for what happens when it fills.
///
/// Bounded, always. An unbounded queue turns a slow viewer into a memory leak, and blocking the
/// writer turns one into a stall on ingest, which would take the stream down for everybody else.
/// </summary>
public sealed class PacketSubscription : IDisposable
{
    private readonly Channel<MediaPacket> _packets;
    private readonly OverflowPolicy _policy;
    private readonly Action _detach;

    /// <summary>
    /// Told when this subscriber falls behind, or null where nothing is measuring - a hub built by
    /// a test or by the detection worker, neither of which publishes a meter. Overflowing is a
    /// fault and therefore rare, so it is the one thing on this path counted as it happens rather
    /// than sampled by the heartbeat.
    /// </summary>
    private readonly LiveMetrics? _metrics;

    /// <summary>Only these stream indexes are delivered. Empty means all of them.</summary>
    private readonly int[] _streamIndexes;

    /// <summary>Set after an overflow, until the next position a decoder can start from.</summary>
    private bool _resynchronising;

    internal PacketSubscription(
        int capacity,
        OverflowPolicy policy,
        int[] streamIndexes,
        Action detach,
        LiveMetrics? metrics = null)
    {
        _policy = policy;
        _streamIndexes = streamIndexes;
        _detach = detach;
        _metrics = metrics;

        _packets = Channel.CreateBounded<MediaPacket>(new BoundedChannelOptions(capacity)
        {
            // Wait, so that TryWrite reports a full queue rather than quietly dropping a packet.
            // What to do about it differs per consumer and is decided below, not here.
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = true,
        });
    }

    public ChannelReader<MediaPacket> Packets => _packets.Reader;

    /// <summary>How many times this subscriber has fallen behind and had to rejoin.</summary>
    public int Overflows { get; private set; }

    /// <summary>Set when a <see cref="OverflowPolicy.Fail"/> subscriber overflowed.</summary>
    public bool Faulted { get; private set; }

    /// <summary>
    /// Offers a packet. Never blocks and never throws: it runs on the demultiplexer's thread, and
    /// one slow consumer must not become everybody's problem.
    /// </summary>
    internal void Offer(MediaPacket packet, bool startsSegment)
    {
        if (_streamIndexes.Length > 0 && Array.IndexOf(_streamIndexes, packet.StreamIndex) < 0)
        {
            return;
        }

        if (_resynchronising)
        {
            if (!startsSegment)
            {
                return;
            }

            _resynchronising = false;
        }

        if (_packets.Writer.TryWrite(packet))
        {
            return;
        }

        Overflows++;
        _metrics?.Overflowed(_policy);

        if (_policy == OverflowPolicy.Fail)
        {
            Faulted = true;
            _packets.Writer.TryComplete();

            return;
        }

        // Everything queued is old news to someone who is meant to be watching live. Throwing it
        // away and waiting for the next start point costs a skip; keeping it costs a growing delay
        // that never recovers.
        while (_packets.Reader.TryRead(out _))
        {
        }

        _resynchronising = true;
    }

    /// <summary>Ends the queue, so a reader draining it stops rather than waiting forever.</summary>
    internal void Complete() => _packets.Writer.TryComplete();

    public void Dispose()
    {
        _detach();
        _packets.Writer.TryComplete();
    }
}
