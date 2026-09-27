using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// Real-time pacing's one piece of actual logic, pulled out of the read loop for the same reason
/// <c>ForwardPlan.Decide</c> is a pure function: proving "a packet running two seconds ahead of the
/// wall clock waits two seconds" needs none of a real stream, a real clock or a real thread behind
/// it, only the four numbers <see cref="StreamDemuxer.Pace"/> actually looks at.
/// </summary>
public sealed class StreamDemuxerTests
{
    private const double SecondsPerTick = 1.0 / 90_000;

    private static long Ticks(double seconds) => (long)(seconds * 90_000);

    [Fact]
    public void A_packet_exactly_on_schedule_neither_waits_nor_reanchors()
    {
        var decision = StreamDemuxer.Pace(
            referencePts: Ticks(3),
            origin: 0,
            SecondsPerTick,
            elapsed: TimeSpan.FromSeconds(3));

        Assert.Null(decision.Wait);
        Assert.False(decision.Reanchor);
    }

    [Fact]
    public void A_packet_running_ahead_of_the_wall_clock_waits_the_difference()
    {
        // Nominally two seconds into the stream, and only half a second of wall clock has passed -
        // exactly what a burst of already-published HLS segments looks like.
        var decision = StreamDemuxer.Pace(
            referencePts: Ticks(2),
            origin: 0,
            SecondsPerTick,
            elapsed: TimeSpan.FromSeconds(0.5));

        Assert.Equal(TimeSpan.FromSeconds(1.5), decision.Wait);
        Assert.False(decision.Reanchor);
    }

    [Fact]
    public void A_packet_running_behind_the_wall_clock_is_forwarded_at_once()
    {
        // The ordinary case of catching up after a slow read: never sleeps negative, which would
        // make an already-late pull later still.
        var decision = StreamDemuxer.Pace(
            referencePts: Ticks(1),
            origin: 0,
            SecondsPerTick,
            elapsed: TimeSpan.FromSeconds(4));

        Assert.Null(decision.Wait);
        Assert.False(decision.Reanchor);
    }

    [Fact]
    public void A_gap_at_exactly_the_ceiling_still_waits_rather_than_reanchors()
    {
        var decision = StreamDemuxer.Pace(
            referencePts: Ticks(5),
            origin: 0,
            SecondsPerTick,
            elapsed: TimeSpan.Zero);

        Assert.Equal(TimeSpan.FromSeconds(5), decision.Wait);
        Assert.False(decision.Reanchor);
    }

    [Fact]
    public void A_forward_jump_past_the_ceiling_reanchors_instead_of_stalling()
    {
        // A discontinuity, or a playlist that skipped ahead: waiting out the full nominal gap would
        // stall the whole pull for however large the jump happened to be.
        var decision = StreamDemuxer.Pace(
            referencePts: Ticks(120),
            origin: 0,
            SecondsPerTick,
            elapsed: TimeSpan.Zero);

        Assert.Null(decision.Wait);
        Assert.True(decision.Reanchor);
    }

    [Fact]
    public void A_backward_jump_past_the_ceiling_reanchors_instead_of_reading_as_forever_late()
    {
        // The origin is far in the stream's future relative to this packet - a timeline reset.
        var decision = StreamDemuxer.Pace(
            referencePts: 0,
            origin: Ticks(120),
            SecondsPerTick,
            elapsed: TimeSpan.FromSeconds(120));

        Assert.Null(decision.Wait);
        Assert.True(decision.Reanchor);
    }
}
