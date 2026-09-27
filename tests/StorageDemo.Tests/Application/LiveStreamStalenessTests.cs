using StorageDemo.Core.Streaming;

namespace StorageDemo.Tests.Application;

/// <summary>The two windows a silent owner is judged by, which are deliberately different numbers.</summary>
public sealed class LiveStreamStalenessTests
{
    private static readonly TimeSpan Beat = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan Grace = TimeSpan.FromSeconds(30);

    [Fact]
    public void The_owner_is_alive_for_three_beats_and_no_longer()
    {
        Assert.True(LiveStreamStaleness.OwnerAlive(Beating(5.9), Beat));
        Assert.False(LiveStreamStaleness.OwnerAlive(Beating(6.1), Beat));
    }

    [Fact]
    public void A_stream_is_gone_only_after_the_grace_period()
    {
        Assert.False(LiveStreamStaleness.IsGone(Beating(29.5), Grace));
        Assert.True(LiveStreamStaleness.IsGone(Beating(30.5), Grace));
    }

    /// <summary>
    /// The gap between them, which is the whole point: a stream is still shown for twenty-four more
    /// seconds after its name has become free.
    /// </summary>
    [Fact]
    public void A_name_frees_long_before_its_stream_stops_being_listed()
    {
        var stream = Beating(10);

        Assert.False(LiveStreamStaleness.OwnerAlive(stream, Beat));
        Assert.False(LiveStreamStaleness.IsGone(stream, Grace));
    }

    private static LiveStream Beating(double secondsAgo)
    {
        var heartbeat = DateTimeOffset.UtcNow.AddSeconds(-secondsAgo);

        return new LiveStream(
            "demo",
            LiveStreamState.Live,
            heartbeat,
            heartbeat,
            "pod-a",
            null,
            0,
            0,
            false,
            true,
            false,
            0,
            null,
            null,
            null);
    }
}
