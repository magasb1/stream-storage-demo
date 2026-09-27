using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The buffer does three jobs with one answer: where a viewer joins, how far a viewer may roll
/// back, and where a recording's pre-roll is cut.
/// </summary>
public sealed class RollingBufferTests
{
    /// <summary>Milliseconds per tick, so a tick is a millisecond and the arithmetic reads plainly.</summary>
    private const double Tick = 0.001;

    private static RollingBuffer Buffer(double windowSeconds = 30, long ceiling = 1024 * 1024)
        => new(windowSeconds, ceiling, Tick);

    private static MediaPacket Packet(bool keyframe, int bytes = 1000)
        => new(0, new byte[bytes], 0, 0, 0, keyframe);

    /// <summary>Fills the buffer with segments of the given length, in milliseconds.</summary>
    private static void Fill(RollingBuffer buffer, int segments, int segmentMilliseconds, int bytes = 1000)
    {
        var pts = 0L;

        for (var segment = 0; segment < segments; segment++)
        {
            buffer.Add(Packet(keyframe: true, bytes), startsSegment: true, pts);

            for (var step = 1; step < 10; step++)
            {
                buffer.Add(Packet(keyframe: false, bytes), startsSegment: false, pts + (segmentMilliseconds * step / 10));
            }

            pts += segmentMilliseconds;
        }
    }

    [Fact]
    public void An_empty_buffer_has_nowhere_to_start()
    {
        var buffer = Buffer();

        Assert.Null(buffer.Newest());
        Assert.Null(buffer.StartingFrom(5));
        Assert.Equal(0, buffer.HeldSeconds);
    }

    /// <summary>
    /// A pre-roll starts at the segment boundary at or before the point asked for, so a recording
    /// routinely begins earlier than requested.
    /// </summary>
    [Fact]
    public void A_position_resolves_to_the_segment_at_or_before_it()
    {
        var buffer = Buffer();

        // Ten segments of one second, so the live edge is at ten seconds.
        Fill(buffer, segments: 10, segmentMilliseconds: 1000);

        var from = buffer.StartingFrom(3.5);

        Assert.NotNull(from);

        // At or before, never after: a segment starting later than asked would drop the very moment
        // the caller reached back for.
        Assert.True(buffer.SecondsBackTo(from) >= 3.5, $"gave only {buffer.SecondsBackTo(from)}s");
        Assert.True(buffer.SecondsBackTo(from) < 5, "reached back further than one segment too far");
    }

    /// <summary>The cost of segments is granularity, and it is the sender's to control.</summary>
    [Fact]
    public void A_coarse_sender_gives_a_coarse_window()
    {
        var coarse = Buffer();
        Fill(coarse, segments: 4, segmentMilliseconds: 10_000);

        var from = coarse.StartingFrom(5);

        Assert.NotNull(from);

        // Asked for five seconds and given nine, because that is where the segment begins.
        Assert.True(coarse.SecondsBackTo(from) >= 5, $"gave only {coarse.SecondsBackTo(from)}s");
        Assert.True(coarse.SecondsBackTo(from) >= 8, $"the step was finer than the sender's keyframes");
    }

    [Fact]
    public void Asking_for_more_than_is_held_gives_everything_there_is()
    {
        var buffer = Buffer();
        Fill(buffer, segments: 3, segmentMilliseconds: 1000);

        var from = buffer.StartingFrom(300);

        Assert.NotNull(from);
        Assert.Same(buffer.Segments[0], from);
    }

    [Fact]
    public void Nothing_older_than_the_window_is_kept()
    {
        var buffer = Buffer(windowSeconds: 5);

        Fill(buffer, segments: 20, segmentMilliseconds: 1000);

        Assert.True(buffer.HeldSeconds <= 6, $"held {buffer.HeldSeconds}s of a 5s window");

        // At least the window, which is the promise; a segment is never split to hit it exactly.
        Assert.True(buffer.HeldSeconds >= 4, $"held only {buffer.HeldSeconds}s");
    }

    /// <summary>
    /// The ceiling is what stops one careless encoder evicting the service, so it binds even when
    /// the time window would have kept more.
    /// </summary>
    [Fact]
    public void The_byte_ceiling_binds_before_the_window_and_says_so()
    {
        var buffer = Buffer(windowSeconds: 60, ceiling: 20_000);

        Fill(buffer, segments: 10, segmentMilliseconds: 1000, bytes: 1000);

        Assert.True(buffer.Bytes <= 20_000, $"held {buffer.Bytes} bytes over a 20000 ceiling");
        Assert.NotNull(buffer.CeilingBindingSince);
        Assert.True(buffer.HeldSeconds < 60);
    }

    /// <summary>The edge the design left open.</summary>
    [Fact]
    public void A_feed_with_no_keyframe_reports_that_it_cannot_be_started()
    {
        var buffer = Buffer(windowSeconds: 60, ceiling: 10_000);

        for (var index = 0; index < 100; index++)
        {
            buffer.Add(Packet(keyframe: false), startsSegment: false, index * 100);
        }

        // Memory stays bounded: the open segment is discarded rather than grown past the ceiling.
        Assert.True(buffer.Bytes <= 10_000, $"held {buffer.Bytes} bytes");

        // And nothing pretends to be a start point, so a viewer joins live, a pre-roll is empty and
        // a snapshot falls back to the preview.
        Assert.NotNull(buffer.NotStartableSince);
        Assert.Null(buffer.Newest());
        Assert.Null(buffer.StartingFrom(5));
    }

    [Fact]
    public void A_keyframe_after_a_long_silence_makes_the_stream_startable_again()
    {
        var buffer = Buffer(windowSeconds: 60, ceiling: 10_000);

        for (var index = 0; index < 100; index++)
        {
            buffer.Add(Packet(keyframe: false), startsSegment: false, index * 100);
        }

        Assert.NotNull(buffer.NotStartableSince);

        buffer.Add(Packet(keyframe: true), startsSegment: true, 10_000);

        Assert.Null(buffer.NotStartableSince);
        Assert.NotNull(buffer.Newest());
    }

    /// <summary>
    /// A viewer joining gets everything from its segment onward, in order and with no hole.
    /// </summary>
    [Fact]
    public void Packets_are_handed_over_in_order_from_the_chosen_segment()
    {
        var buffer = Buffer();
        Fill(buffer, segments: 5, segmentMilliseconds: 1000);

        var from = buffer.StartingFrom(2);
        Assert.NotNull(from);

        var packets = buffer.PacketsFrom(from).ToList();

        Assert.NotEmpty(packets);
        Assert.True(packets[0].IsKeyframe, "a viewer was handed a position a decoder cannot start at");

        var expected = buffer.Segments
            .SkipWhile(segment => !ReferenceEquals(segment, from))
            .Sum(segment => segment.Packets.Count);

        Assert.Equal(expected, packets.Count);
    }

    [Fact]
    public void The_newest_startable_position_is_what_a_snapshot_decodes()
    {
        var buffer = Buffer();
        Fill(buffer, segments: 4, segmentMilliseconds: 1000);

        Assert.Same(buffer.Segments[^1], buffer.Newest());
    }

    /// <summary>
    /// An encoder that drops and reconnects presents its clock from the beginning again, and the
    /// buffer it rejoins is four seconds along.
    /// </summary>
    [Fact]
    public void A_feed_that_reconnects_carries_on_the_clock_it_rejoined()
    {
        var buffer = Buffer();
        Fill(buffer, segments: 4, segmentMilliseconds: 1000);

        var before = buffer.HeldSeconds;

        buffer.Resume();
        Fill(buffer, segments: 2, segmentMilliseconds: 1000);

        Assert.True(
            buffer.HeldSeconds > before,
            $"the buffer went backwards when the feed reconnected: {buffer.HeldSeconds:0.##}s");

        // What the second feed covers on its own, laid down one tick after what was already held.
        var second = buffer.Segments[^1].EndPts - buffer.Segments[4].StartPts;

        Assert.Equal(before + Tick + (second * Tick), buffer.HeldSeconds, precision: 3);
    }

    /// <summary>The first feed of all has nothing to rejoin, so it keeps its own clock.</summary>
    [Fact]
    public void The_first_feed_is_not_shifted()
    {
        var buffer = Buffer();

        buffer.Resume();
        Fill(buffer, segments: 2, segmentMilliseconds: 1000);

        Assert.Equal(0, buffer.Segments[0].StartPts);
    }
}
