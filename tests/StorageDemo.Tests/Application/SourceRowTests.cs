using StorageDemo.Api.Streaming;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Application;

/// <summary>The arithmetic behind the operator page, tested without a renderer.</summary>
public sealed class SourceRowTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.UnixEpoch;

    [Fact]
    public void The_configured_consumption_address_wins_over_the_host_the_browser_used()
    {
        // Behind an ingress the API port and the media port are rarely the same address, so a URL
        // built from the host the page was served on would be one no player could reach.
        var options = new LiveOptions { PublicConsumptionUrl = "srt://live.example.com:9010", ConsumptionPort = 9010 };

        Assert.Equal(
            "srt://live.example.com:9010?streamid=camera1",
            StreamingRows.LocalOutput(options, "internal-pod-7", "camera1"));
    }

    [Fact]
    public void Without_one_the_address_is_the_browsers_host_and_the_consumption_port()
    {
        var options = new LiveOptions { ConsumptionPort = 9011 };

        Assert.Equal(
            "srt://box.local:9011?streamid=camera1",
            StreamingRows.LocalOutput(options, "box.local", "camera1"));
    }

    /// <summary>A stream name may contain slashes, and it rides in a query string.</summary>
    [Fact]
    public void A_name_with_a_slash_is_escaped_into_the_stream_identifier()
    {
        var options = new LiveOptions { ConsumptionPort = 9010 };

        Assert.Equal(
            "srt://box.local:9010?streamid=live%2Fcamera1",
            StreamingRows.LocalOutput(options, "box.local", "live/camera1"));
    }

    [Fact]
    public void A_source_with_no_stream_is_not_on_air_rather_than_interrupted()
    {
        var row = StreamingRows.Build(Source(), stream: null, new LiveOptions(), "box.local");

        Assert.Equal(SourceState.NotOnAir, row.State);
        Assert.Null(row.BitsPerSecond);
    }

    [Theory]
    [InlineData(LiveStreamState.Live, SourceState.Live)]
    [InlineData(LiveStreamState.Interrupted, SourceState.Interrupted)]
    public void A_stream_under_the_same_name_carries_its_state_onto_the_row(
        LiveStreamState streamState,
        SourceState expected)
    {
        var row = StreamingRows.Build(Source(), Stream(streamState), new LiveOptions(), "box.local");

        Assert.Equal(expected, row.State);
    }

    /// <summary>
    /// The state this page exists to make legible: three forwards asked for, one connected, one
    /// refused with a reason and one the owner is reporting nothing at all for.
    /// </summary>
    [Fact]
    public void A_configured_forward_the_owner_reports_nothing_for_still_appears()
    {
        var source = Source() with
        {
            Forwards =
            [
                new ForwardTarget("a", "srt://one:9000"),
                new ForwardTarget("b", "srt://two:9000"),
                new ForwardTarget("c", "udp://239.0.0.1:5000"),
            ],
        };

        var stream = Stream(LiveStreamState.Live, forwards:
        [
            new ForwardStatus("a", "srt://one:9000", Connected: true, Bytes: 4096),
            new ForwardStatus("b", "srt://two:9000", Connected: false, Bytes: 0, Error: "connection refused"),
        ]);

        var row = StreamingRows.Build(source, stream, new LiveOptions(), "box.local");

        Assert.Equal(3, row.ForwardsConfigured);
        Assert.Equal(1, row.ForwardsConnected);

        var detail = row.Detail.ToList();

        Assert.Equal(3, detail.Count);
        Assert.Equal("connection refused", detail[1].Status!.Error);
        Assert.Null(detail[2].Status);
    }

    [Theory]
    [InlineData("srt://host:9000?streamid=name", true)]
    [InlineData("SRT://host:9000", true)]
    [InlineData("udp://239.0.0.1:5000?ttl=2", true)]
    [InlineData("rtp://host:5004", true)]
    [InlineData("http://host/stream.ts", false)]
    // No scheme is "file", which is the case the allowlist exists to refuse: it would make the
    // service read whatever libav can open off its own disk.
    [InlineData("/etc/passwd", false)]
    [InlineData("file:///etc/passwd", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void Only_the_allowed_transports_pass_the_form(string? url, bool allowed)
        => Assert.Equal(allowed, StreamingRows.SchemeAllowed(url, new LiveOptions().AllowedSchemes));

    /// <summary>
    /// Two samples give a rate; one gives nothing, and so does a total that went backwards, which
    /// is what an encoder reconnecting under the same name looks like from here.
    /// </summary>
    [Fact]
    public void Throughput_needs_two_rising_samples_and_says_so_when_it_has_none()
    {
        var meter = new ThroughputMeter();

        Assert.Null(meter.Sample("camera1", 1_000, Now));
        Assert.Equal(8_000, meter.Sample("camera1", 3_000, Now.AddSeconds(2)));
        Assert.Null(meter.Sample("camera1", 100, Now.AddSeconds(4)));
    }

    private static LiveSource Source()
        => new("camera1", "srt://camera:9000", Enabled: true, Forwards: [], Now);

    private static LiveStream Stream(
        LiveStreamState state,
        long bytes = 0,
        IReadOnlyList<ForwardStatus>? forwards = null)
        => new(
            "camera1",
            state,
            Now,
            Now,
            "pod-a",
            null,
            0,
            bytes,
            false,
            true,
            false,
            0,
            null,
            null,
            null,
            Forwards: forwards);
}
