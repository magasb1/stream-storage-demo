using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// A viewer on the replica that does not own the stream, which is what the load balancer produces
/// most of the time and what nothing on one host can show.
///
/// The hop between the two replicas is HTTP; only the last hop, to the player, is SRT. Two
/// applications in one process, sharing a registry and a lock; see <see cref="LiveReplicas"/>.
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
    /// of the player. Fails if the route, the peer client, the token or the copy breaks.
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

    /// <summary>
    /// A rollback survives the hop. Asked of the owner's peer route directly, because that is where
    /// the figure is resolved and it is the one thing a query string can silently drop: a viewer
    /// asking for five seconds and quietly getting the live edge looks exactly like success.
    ///
    /// At least what was asked for, never exactly: a stream can only be joined where a decoder can
    /// start, so five seconds back is rounded out to the keyframe before it.
    /// </summary>
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
    /// The sibling claim, and the one that used to be a quiet lie: a rollback deeper than the
    /// viewer's queue is cut to what will fit, and the header says the shorter figure.
    ///
    /// A queue is seeded from the buffer before a live packet reaches it, and a seed bigger than the
    /// queue does not lose the part that did not fit - every seeded packet is offered as not starting
    /// a segment, so the first overflow throws the whole history away and waits for the next
    /// keyframe. A viewer whose header promised it eight seconds then starts at the live edge, which
    /// is the worst of the three answers available.
    ///
    /// The queue's ceiling is set to 150 packets here, because it is what has to bind and the
    /// reference sender is fifteen frames a second of video and nothing else: the configured ceiling
    /// of two thousand is over two minutes of that, and no test could reach it. 150 packets is ten
    /// seconds, four of which are held back for the live flow, so six is what a rollback can have.
    /// </summary>
    [Fact]
    public async Task A_rollback_deeper_than_the_viewers_queue_is_cut_to_what_fits()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "deep-rollback-camera";
        const double asked = 8;

        var a = _replicas.Start("pod-a", _aIngest, viewerQueuePackets: 150);

        using var meters = new Meters(a.Services.GetRequiredService<LiveMetrics>());

        _replicas.Send(_aIngest, name);

        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync(name) is { BufferedSeconds: > asked },
            TimeSpan.FromSeconds(40),
            $"A never buffered the {asked} seconds this test asks to roll back through");

        using var client = _replicas.Client(a);
        using var response = await client.GetAsync(
            $"api/live/peer/view/{name}?from={asked}&continue=0",
            HttpCompletionOption.ResponseHeadersRead);

        response.EnsureSuccessStatusCode();

        var given = double.Parse(
            response.Headers.GetValues("X-Live-Preroll").Single(),
            System.Globalization.CultureInfo.InvariantCulture);

        Assert.True(
            given > 0 && given < asked,
            $"asked for {asked} seconds back against a queue holding six and was promised {given}");

        // Promised is not given. Reading the stream is what proves the history actually arrived:
        // an overflow on the way in would have discarded it, and the skip is counted.
        await using var body = await response.Content.ReadAsStreamAsync();

        var buffer = new byte[64 * 1024];

        Assert.True(await body.ReadAsync(buffer) > 0, "the relayed viewer received nothing");

        Assert.Empty(meters.Of("live.overflows"));
    }

    /// <summary>
    /// What the relay leans on: handed a point to continue from, the muxer starts there rather than
    /// at the incoming stream's own zero. A viewer whose stream moves replicas mid-connection is
    /// otherwise asked to accept timestamps jumping backwards, which is what a player breaks on.
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
