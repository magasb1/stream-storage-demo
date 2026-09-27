namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// A stretch of stream beginning at a position a decoder can start from and running to the next.
/// </summary>
public sealed class Segment(long startPts, bool startable)
{
    public List<MediaPacket> Packets { get; } = [];

    /// <summary>Presentation timestamp of the first packet, in the reference stream's time base.</summary>
    public long StartPts { get; } = startPts;

    public long EndPts { get; private set; } = startPts;

    /// <summary>
    /// False only for the stretch left over when the buffer had to restart mid-flow, which is what
    /// happens to a feed that runs a long way without sending a keyframe.
    /// </summary>
    public bool Startable { get; } = startable;

    public long Bytes { get; private set; }

    public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;

    public void Add(MediaPacket packet, long pts)
    {
        Packets.Add(packet);
        Bytes += packet.Bytes;

        if (pts > EndPts)
        {
            EndPts = pts;
        }
    }
}

/// <summary>The recent past of one stream, held in memory as segments.</summary>
public sealed class RollingBuffer(double windowSeconds, long byteCeiling, double secondsPerTick)
{
    private readonly List<Segment> _segments = [];

    /// <summary>Added to every position a feed reports, so one buffer measures one clock.</summary>
    private long _shift;

    private bool _resumed;

    /// <summary>Every retained segment, oldest first.</summary>
    public IReadOnlyList<Segment> Segments => _segments;

    public long Bytes { get; private set; }

    /// <summary>Since when this stream has had no position a decoder could start from.</summary>
    public DateTimeOffset? NotStartableSince { get; private set; }

    /// <summary>Since when the byte ceiling has been what evicts, rather than the time window.</summary>
    public DateTimeOffset? CeilingBindingSince { get; private set; }

    /// <summary>How much stream is held, which is at least the window unless it is still filling.</summary>
    public long EndPts => _segments.Count == 0 ? 0 : _segments[^1].EndPts;

    public double HeldSeconds => _segments.Count == 0
        ? 0
        : (_segments[^1].EndPts - _segments[0].StartPts) * secondsPerTick;

    /// <summary>The newest position a decoder can start from, or null while there is none.</summary>
    public Segment? Newest() => _segments.LastOrDefault(segment => segment.Startable);

    /// <summary>
    /// Where a viewer joining, a viewer rolling back, and a recording's pre-roll all begin: the
    /// startable position at or before <paramref name="secondsBack"/> from the live edge.
    /// </summary>
    public Segment? StartingFrom(double secondsBack)
    {
        if (_segments.Count == 0)
        {
            return null;
        }

        var target = _segments[^1].EndPts - (long)(Math.Max(0, secondsBack) / secondsPerTick);

        // At or before, never after: a segment starting later than asked would drop the very moment
        // the caller reached back for.
        for (var index = _segments.Count - 1; index >= 0; index--)
        {
            if (_segments[index].Startable && _segments[index].StartPts <= target)
            {
                return _segments[index];
            }
        }

        // Everything held is newer than the request, so the oldest startable segment is as far back
        // as this stream goes.
        return _segments.FirstOrDefault(segment => segment.Startable);
    }

    /// <summary>Everything from <paramref name="from"/> onward, in order.</summary>
    public IEnumerable<MediaPacket> PacketsFrom(Segment from)
    {
        var reached = false;

        foreach (var segment in _segments)
        {
            reached |= ReferenceEquals(segment, from);

            if (reached)
            {
                foreach (var packet in segment.Packets)
                {
                    yield return packet;
                }
            }
        }
    }

    /// <summary>How far back a caller may actually reach, which is what a response should report.</summary>
    public double SecondsBackTo(Segment segment)
        => _segments.Count == 0 ? 0 : (_segments[^1].EndPts - segment.StartPts) * secondsPerTick;

    /// <param name="startsSegment">True when a decoder could begin at this packet.</param>
    /// <param name="pts">The packet's position on the reference stream's clock.</param>
    public void Add(MediaPacket packet, bool startsSegment, long pts)
    {
        if (_resumed)
        {
            _resumed = false;

            // An encoder that reconnects starts counting from its own beginning again, and this
            // buffer is somewhere else entirely.
            _shift = EndPts == 0 ? 0 : EndPts - pts + 1;
        }

        pts += _shift;

        if (startsSegment || _segments.Count == 0)
        {
            _segments.Add(new Segment(pts, startsSegment));
        }

        _segments[^1].Add(packet, pts);
        Bytes += packet.Bytes;

        Evict();

        NotStartableSince = _segments.Any(segment => segment.Startable)
            ? null
            : NotStartableSince ?? DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Says the next packet comes from a feed that has just connected, whose clock has no relation
    /// to the one already held.
    /// </summary>
    public void Resume() => _resumed = true;

    public void Clear()
    {
        _resumed = false;
        _shift = 0;
        _segments.Clear();
        Bytes = 0;
        NotStartableSince = null;
        CeilingBindingSince = null;
    }

    /// <summary>
    /// One rule, applied oldest first: the buffer holds neither more seconds than the window nor
    /// more bytes than the ceiling.
    /// </summary>
    private void Evict()
    {
        while (_segments.Count > 0)
        {
            var overBytes = Bytes > byteCeiling;
            var overWindow = HeldSeconds > windowSeconds;

            if (!overBytes && !overWindow)
            {
                return;
            }

            // The window promise is a floor, so the last segment stays however long it runs.
            if (_segments.Count == 1 && !overBytes)
            {
                return;
            }

            // Which constraint is doing the evicting, rather than whether the buffer is over one
            // right now.
            if (overBytes)
            {
                CeilingBindingSince ??= DateTimeOffset.UtcNow;
            }
            else
            {
                CeilingBindingSince = null;
            }

            Bytes -= _segments[0].Bytes;
            _segments.RemoveAt(0);
        }

        CeilingBindingSince ??= DateTimeOffset.UtcNow;
    }
}
