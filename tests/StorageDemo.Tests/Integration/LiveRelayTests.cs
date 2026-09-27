using FFmpeg.AutoGen.Abstractions;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// A viewer on the replica that does not own the stream, which is what the load balancer produces
/// most of the time and what nothing on one host can show.
/// </summary>
public sealed class LiveRelayTests : IAsyncLifetime
{
    private readonly LiveReplicas _replicas = new();

    private int _aIngest;
    private int _bIngest;

    public ValueTask InitializeAsync()
    {
        _aIngest = SrtSenders.FreePort();
        _bIngest = SrtSenders.FreePort();

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => _replicas.DisposeAsync();

    /// <summary>
    /// The whole hop, end to end: an encoder on A, a player on B's consumption port, pictures out
    /// of the player.
    /// </summary>
    [Fact]
    public async Task A_viewer_on_the_pod_that_does_not_own_the_stream_receives_it()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "relayed-camera";

        var a = _replicas.Start("pod-a", _aIngest);
        _replicas.Start("pod-b", _bIngest, peer: a);

        _replicas.Send(_aIngest, name);

        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync(name) is { State: LiveStreamState.Live, Packets: > 0 },
            TimeSpan.FromSeconds(40),
            "A never went live");

        var viewer = _replicas.Watch(_bIngest, name);

        await SrtSenders.WaitUntilAsync(
            () => SrtSenders.Decoded(viewer) > 0,
            TimeSpan.FromSeconds(20),
            () => $"the relayed viewer decoded nothing: {SrtSenders.Complaints([viewer])}");
    }

    /// <summary>A rollback survives the hop.</summary>
    [Fact]
    public async Task A_viewer_following_a_rollback_gets_at_least_what_it_asked_for()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "rolled-back-camera";

        var a = _replicas.Start("pod-a", _aIngest);

        _replicas.Send(_aIngest, name);

        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync(name) is { BufferedSeconds: > 6 },
            TimeSpan.FromSeconds(40),
            "A never buffered enough to roll back through");

        using var client = _replicas.Client(a);
        using var response = await client.GetAsync(
            $"api/live/peer/view/{name}?from=5&continue=0",
            HttpCompletionOption.ResponseHeadersRead);

        response.EnsureSuccessStatusCode();

        Assert.Equal("video/mp2t", response.Content.Headers.ContentType?.MediaType);

        var given = double.Parse(
            response.Headers.GetValues("X-Live-Preroll").Single(),
            System.Globalization.CultureInfo.InvariantCulture);

        Assert.True(given >= 5, $"asked for 5 seconds back and was given {given}");
    }

    /// <summary>
    /// What the relay leans on: handed a point to continue from, the muxer starts there rather than
    /// at the incoming stream's own zero.
    /// </summary>
    [Fact]
    public void The_timeline_does_not_go_backwards_across_a_re_attach()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = OneVideoStream();
        using var output = new MemoryStream();
        using var muxer = new PacketMuxer(output, layout, "mpegts", continueFromSeconds: 12);

        // A feed that has just reconnected and whose clock is back at zero.
        muxer.Write(new MediaPacket(0, new byte[188], Pts: 0, Dts: 0, Duration: 3600, IsKeyframe: true));

        Assert.True(
            muxer.TimelineSeconds >= 12,
            $"a timeline continued from 12s came out at {muxer.TimelineSeconds}s");
    }

    /// <summary>The smallest layout a muxer will accept, built without a real transport.</summary>
    private static unsafe StreamLayout OneVideoStream()
    {
        var format = ffmpeg.avformat_alloc_context();

        try
        {
            var stream = ffmpeg.avformat_new_stream(format, null);

            stream->time_base = new AVRational { num = 1, den = 90000 };
            stream->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_VIDEO;
            stream->codecpar->codec_id = AVCodecID.AV_CODEC_ID_MPEG2VIDEO;
            stream->codecpar->width = 320;
            stream->codecpar->height = 240;

            return StreamLayout.From(format);
        }
        finally
        {
            ffmpeg.avformat_free_context(format);
        }
    }
}
