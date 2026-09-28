using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The arithmetic behind a stream's contribution to the meter.
///
/// It is the one part of the measurement that can be wrong rather than merely absent: the hub keeps
/// running totals, because that is what survives a reconnect, and a counter wants the interval. Add
/// a total every beat and a stream that ran for an hour reports its traffic eighteen hundred times.
/// </summary>
public sealed class LiveStreamEntryTests
{
    [Fact]
    public async Task What_a_stream_carried_is_reported_as_an_interval_rather_than_a_total()
    {
        await using var entry = Entry(out var hub);

        Publish(hub, packets: 2, bytes: 3);

        var first = entry.TakeFeed(lost: 4, dropped: 2);

        Assert.Equal(2, first.Packets);
        Assert.Equal(6, first.Bytes);

        // The transport's own figures pass straight through: libsrt cleared them when the caller
        // read them, so they are already an interval and are not this class's to difference.
        Assert.Equal(4, first.PacketsLost);
        Assert.Equal(2, first.PacketsDropped);

        Publish(hub, packets: 1, bytes: 3);

        var second = entry.TakeFeed(0, 0);

        Assert.Equal(1, second.Packets);
        Assert.Equal(3, second.Bytes);
    }

    /// <summary>
    /// Describing a stream twice inside one beat is ordinary - a claim describes it, and so does a
    /// manual creation - and it must not count the same packets twice. Nothing arrived in between,
    /// so the second reading is nothing.
    /// </summary>
    [Fact]
    public async Task Asking_twice_in_one_beat_reports_nothing_the_second_time()
    {
        await using var entry = Entry(out var hub);

        Publish(hub, packets: 5, bytes: 10);

        Assert.Equal(5, entry.TakeFeed(0, 0).Packets);

        var again = entry.TakeFeed(0, 0);

        Assert.Equal(0, again.Packets);
        Assert.Equal(0, again.Bytes);
        Assert.Equal(0, again.KlvPackets);
    }

    /// <summary>
    /// What is actually arriving, in packets a second, taken over the interval the meter's own
    /// reading spans. Issue #26: a declared rate that is too low cannot be detected from the
    /// declaration, so the traffic has to be measured.
    /// </summary>
    [Fact]
    public async Task What_is_arriving_is_measured_over_the_window_between_readings()
    {
        var time = new SteppedTime();

        await using var entry = Entry(out var hub, time);

        // Nothing measured yet, which is what a stream that has just started reports. Zero rather
        // than a guess, because the consumer takes the larger of this and the declaration.
        Assert.Equal(0, entry.ObservedPacketsPerSecond);

        Publish(hub, packets: 50, bytes: 1);

        time.Advance(TimeSpan.FromSeconds(2));
        entry.TakeFeed(0, 0);

        // Fifty packets over two seconds is twenty-five a second, whatever the sender declared.
        Assert.Equal(25, entry.ObservedPacketsPerSecond);

        // A feed that has stopped reads zero once a window passes with nothing arriving, which is
        // how an interrupted stream falls back to its declaration without being asked.
        time.Advance(TimeSpan.FromSeconds(2));
        entry.TakeFeed(0, 0);

        Assert.Equal(0, entry.ObservedPacketsPerSecond);
    }

    /// <summary>
    /// A reading taken a moment after another is not a rate, and must not be read as one.
    ///
    /// The heartbeat is two seconds, so an ordinary sample is never this short - but a claim and a
    /// manual creation both describe a stream too, and either can land a fraction of a second after
    /// a beat. Five packets over a hundredth of a second reads as five hundred a second on a stream
    /// sending twenty-five.
    /// </summary>
    [Fact]
    public async Task A_reading_taken_too_soon_after_the_last_does_not_become_a_rate()
    {
        var time = new SteppedTime();

        await using var entry = Entry(out var hub, time);

        Publish(hub, packets: 50, bytes: 1);

        time.Advance(TimeSpan.FromSeconds(2));
        entry.TakeFeed(0, 0);

        Assert.Equal(25, entry.ObservedPacketsPerSecond);

        Publish(hub, packets: 5, bytes: 1);

        time.Advance(TimeSpan.FromMilliseconds(10));
        entry.TakeFeed(0, 0);

        // Unchanged, rather than the five hundred a second the short interval would have said.
        Assert.Equal(25, entry.ObservedPacketsPerSecond);

        // And nothing was lost by skipping it: the window carries on from where it was, so the
        // five packets above are counted in the next reading rather than dropped. Fifty-five over
        // two and a bit seconds, not the fifty the meter's own interval would have differenced.
        time.Advance(TimeSpan.FromSeconds(2) - TimeSpan.FromMilliseconds(10));
        Publish(hub, packets: 50, bytes: 1);
        entry.TakeFeed(0, 0);

        Assert.Equal(27.5, entry.ObservedPacketsPerSecond);
    }

    /// <summary>
    /// The whole of issue #26 through the parts that actually produce the figures: a hub carrying
    /// traffic, the entry that measures it, and the layout that decides what a viewer's queue is
    /// sized from.
    ///
    /// The stream declares one frame a second and sends fifty, which is the case the issue
    /// describes and the one <c>StreamLayout.Believable</c> cannot reach - a low declaration is
    /// indistinguishable from a slow stream. It also carries #35's synthetic track, which is what
    /// makes this more than the sum of the arithmetic tested in <c>StreamLayoutTests</c>:
    /// <c>StreamHub.PublishAtLiveEdge</c> leaves <c>StreamHub.Packets</c> alone, so the
    /// measurement below has never seen those packets, and the 51 asserted at the end is only
    /// reachable if the layout adds its own declared rate back. Measured bare it would read 50,
    /// and every stream carrying that track would be sized short of a figure this service computed
    /// itself.
    ///
    /// The one test here that adopts a layout, and so the one that needs libav.
    /// </summary>
    [Fact]
    public async Task A_stream_sending_fifty_times_what_it_declared_is_queued_for_what_it_sends()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();
        var time = new SteppedTime();

        using var layout = Misdeclaring();
        using var hub = new StreamHub("misdeclaring", options, NullLogger.Instance);

        hub.Adopt(layout);

        await using var entry = new LiveStreamEntry(
            hub,
            new Harvester(320, 4, TimeSpan.FromSeconds(2)),
            NullLogger.Instance,
            manual: false,
            manualUrl: null,
            time);

        // A frame a second declared, video and metadata together.
        Assert.Equal(2, layout.PacketsPerSecond);

        // Two seconds of fifty a second arriving, and the two synthetic sets that go with it.
        for (var index = 0; index < 100; index++)
        {
            hub.Publish(new MediaPacket(0, new byte[16], index, index, 1, IsKeyframe: index % 25 == 0), index);
        }

        Assert.True(hub.PublishAtLiveEdge(SyntheticTrackRole.PlatformMetadata, new byte[32]));
        Assert.True(hub.PublishAtLiveEdge(SyntheticTrackRole.PlatformMetadata, new byte[32]));

        time.Advance(TimeSpan.FromSeconds(2));
        entry.TakeFeed(0, 0);

        // Arrivals only: the two sets published above moved nothing here, by the design that keeps
        // a locally produced packet from holding a dead camera's stream open.
        Assert.Equal(50, entry.ObservedPacketsPerSecond);

        var effective = layout.EffectivePacketsPerSecond(entry.ObservedPacketsPerSecond);

        // Fifty arriving plus the one this service publishes, against a declared two.
        Assert.Equal(51, effective);

        // What the viewer would have been given on the declaration: four seconds of a stream said
        // to send two packets a second is eight, against the two hundred and four its traffic
        // actually needs. Every viewer of it skips to live almost continuously while the stream
        // reports itself healthy.
        Assert.Equal(
            8,
            layout.QueueDepth(layout.PacketsPerSecond, options.ViewerQueueSeconds, options.ViewerQueuePackets));

        // And what it is given now.
        Assert.Equal(
            204,
            layout.QueueDepth(effective, options.ViewerQueueSeconds, options.ViewerQueuePackets));
    }

    /// <summary>
    /// A sender declaring one frame a second, carrying the synthetic ST 0601 track #35 appends.
    /// Built without a transport, because a frame rate is only what a demultiplexer reported after
    /// probing and none of this needs a real one.
    /// </summary>
    private static unsafe StreamLayout Misdeclaring()
    {
        var format = ffmpeg.avformat_alloc_context();

        try
        {
            var video = ffmpeg.avformat_new_stream(format, null);

            video->time_base = new AVRational { num = 1, den = 90_000 };
            video->avg_frame_rate = new AVRational { num = 1, den = 1 };
            video->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_VIDEO;
            video->codecpar->codec_id = AVCodecID.AV_CODEC_ID_H264;
            video->codecpar->width = 640;
            video->codecpar->height = 360;

            return StreamLayout.From(format, [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 1)]);
        }
        finally
        {
            ffmpeg.avformat_free_context(format);
        }
    }

    private static LiveStreamEntry Entry(out StreamHub hub, TimeProvider? time = null)
    {
        hub = new StreamHub("metered", new LiveOptions(), NullLogger.Instance);

        // No layout is ever adopted, so the decoder and the KLV extractor both sit in their waiting
        // loop and nothing here touches libav.
        return new LiveStreamEntry(
            hub,
            new Harvester(320, 4, TimeSpan.FromSeconds(2)),
            NullLogger.Instance,
            manual: false,
            manualUrl: null,
            time);
    }

    private static void Publish(StreamHub hub, int packets, int bytes)
    {
        for (var i = 0; i < packets; i++)
        {
            hub.Publish(new MediaPacket(0, new byte[bytes], i, i, 1, IsKeyframe: false), i);
        }
    }
}
