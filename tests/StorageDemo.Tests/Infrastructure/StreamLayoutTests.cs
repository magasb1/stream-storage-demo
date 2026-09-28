using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The arithmetic that turns a queue depth stated in seconds into the packets a channel can be
/// asked for.
///
/// It is worth its own tests because the figure it replaces was wrong by a factor of twenty and
/// nothing said so: 2000 packets read as a generous viewer queue until somebody worked out that a
/// packet is a frame, which at twenty-five a second made it eighty seconds deep and meant
/// skip-to-live never engaged.
/// </summary>
public sealed unsafe class StreamLayoutTests
{
    [Fact]
    public void A_queue_asked_for_in_seconds_holds_that_many_seconds_of_the_stream()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = Layout(videoFrameRate: 25);

        Assert.Equal(25, layout.PacketsPerSecond);
        Assert.Equal(100, layout.QueueDepth(seconds: 4, ceiling: 2_000));
    }

    /// <summary>
    /// The reason this is not the video frame rate. Audio is more packets than the picture it
    /// accompanies, and a queue sized from the frame rate alone is less than half as deep in
    /// seconds as it was asked to be - which for a viewer is the difference between riding out a
    /// hiccup and skipping.
    /// </summary>
    [Fact]
    public void Audio_counts_towards_the_depth_as_well_as_the_picture()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = Layout(videoFrameRate: 25, sampleRate: 48_000, frameSize: 1_024);

        // Forty-eight kilohertz in frames of 1024 samples is 46.875 packets a second, not 48000.
        Assert.Equal(71.875, layout.PacketsPerSecond, 3);
        Assert.Equal(288, layout.QueueDepth(seconds: 4, ceiling: 2_000));
    }

    /// <summary>
    /// What the ceiling is for. A queue in seconds is the intent, but the memory it costs is
    /// packets, and a sender is free to claim any frame rate it likes.
    ///
    /// A claim of ten thousand frames a second is not believed at all, which is the point: believed,
    /// it would pin every viewer of that stream to the ceiling and so to a fifth of a second of
    /// queue, letting a sender opt its own viewers out of ever riding out a hiccup. It is treated as
    /// a stream that said nothing, and a low ceiling then binds on the assumption rather than on the
    /// claim.
    /// </summary>
    [Fact]
    public void A_stream_claiming_an_impossible_rate_is_not_believed()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var claimed = Layout(videoFrameRate: 10_000);
        using var impossible = Layout(videoFrameRate: 25, sampleRate: 48_000, frameSize: 1);

        Assert.Equal(60, claimed.PacketsPerSecond);
        Assert.Equal(240, claimed.QueueDepth(seconds: 4, ceiling: 2_000));

        // The audio track claims 48000 packets a second, from a frame of one sample. Discarded, so
        // this transport is twenty-five frames of picture plus the assumption for the track that
        // said something impossible - not the forty-eight thousand it asked to be sized for.
        Assert.Equal(85, impossible.PacketsPerSecond);
    }

    /// <summary>
    /// The ceiling still binds where the stream is fast and honest, which is the case a viewer's
    /// deepest rollback runs into.
    /// </summary>
    [Fact]
    public void A_ceiling_below_what_was_asked_for_is_what_is_given()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = Layout(videoFrameRate: 50);

        Assert.Equal(2_000, layout.QueueDepth(seconds: 60, ceiling: 2_000));
    }

    /// <summary>
    /// The floor, which is in seconds because a floor in packets is the whole bug in miniature:
    /// sixty-four packets is a moment at twenty-five frames a second and over a minute at one.
    /// </summary>
    [Fact]
    public void A_slow_stream_gets_a_short_queue_rather_than_a_deep_one()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = Layout(videoFrameRate: 1);

        // Four seconds of a one-frame-a-second stream is four packets, not sixty-four.
        Assert.Equal(4, layout.QueueDepth(seconds: 4, ceiling: 2_000));

        // And asking for less than the floor in seconds gets the floor, in that stream's own terms:
        // half a second of this stream rounds up to one packet, and a queue has to hold a few.
        Assert.Equal(4, layout.QueueDepth(seconds: 0.1, ceiling: 2_000));
    }

    /// <summary>
    /// A synthetic track declares the rate it will actually be published at, and a viewer's deepest
    /// rollback is sized from that rather than from the assumption.
    ///
    /// The regression this pins is silent, which is why it is here rather than left to be noticed.
    /// A track that declares no rate falls through to the sixty a stream that said nothing gets, and
    /// on a 25 fps stream that takes the transport from 25 packets a second to 85. That figure is
    /// what LiveStreamCoordinator.Fitting divides the viewer queue ceiling by, so the deepest
    /// rollback this service can honour collapses from 76 seconds to 19.5 - in exactly the number
    /// two rounds of work already went into getting right, and with nothing anywhere saying so.
    /// Declared truthfully at one hertz it reads 26 and 72.9 seconds, which is the second of video
    /// that the metadata track costs and nothing more.
    /// </summary>
    [Fact]
    public void A_declared_synthetic_rate_is_what_a_viewers_rollback_is_sized_from()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var bare = Layout(videoFrameRate: 25);
        using var declared = Layout(videoFrameRate: 25, synthetic: [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 1)]);

        // What the assumption would have cost, expressed the way RateOf reaches it: a data track
        // that declares nothing is taken to send sixty.
        using var undeclared = Layout(videoFrameRate: 25, synthetic: [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 60)]);

        Assert.Equal(25, bare.PacketsPerSecond);
        Assert.Equal(26, declared.PacketsPerSecond);
        Assert.Equal(85, undeclared.PacketsPerSecond);

        // Fitting's own expression, reproduced rather than called because it is private to the
        // coordinator and a viewer's rollback is the only thing it computes. If that expression
        // changes this test is wrong and should be changed with it; what it must never do is keep
        // passing while the figure it names moves.
        Assert.Equal(76, Rollback(bare), 1);
        Assert.Equal(72.9, Rollback(declared), 1);
        Assert.Equal(19.5, Rollback(undeclared), 1);
    }

    /// <summary>
    /// A synthetic track cannot declare a rate that would wreck the arithmetic it feeds.
    ///
    /// The same ceiling a sender's claim is held to, reached by the other door. A declared 999,999
    /// adds straight into the transport's rate - past a million for a 25 fps stream - and pins
    /// every viewer's queue to the packet ceiling, which is a fraction of a second: exactly what
    /// <c>Believable</c> and the work behind it exist to prevent. Unreachable from configuration
    /// today, because the only caller passes a constant, which is why it is worth closing now
    /// rather than after something else starts declaring one.
    /// </summary>
    [Fact]
    public void A_synthetic_track_cannot_declare_an_impossible_rate()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 999_999));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 0));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, double.PositiveInfinity));

        // Thrown rather than replaced with the assumption, which is the difference from a sender's
        // claim: a sender is not ours to fix and gets the fallback, and there is no rate that would
        // be right for a track whose publisher we wrote as well.
        Assert.Equal(1, new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 1).PacketsPerSecond);
    }

    /// <summary>
    /// A stream that declares KLV of its own gets no synthetic track, and keeps its own index.
    ///
    /// Precedence, decided at the one moment it can be decided at. A camera that starts reporting
    /// its own telemetry wins over what somebody typed about it, and the transition is a layout
    /// change a reconnect makes visible rather than a quiet swap of what a consumer is reading.
    /// </summary>
    [Fact]
    public void A_stream_that_sends_its_own_metadata_gets_no_synthetic_track()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var asked = new SyntheticTrack[] { new(SyntheticTrackRole.PlatformMetadata, 1) };

        using var reporting = Layout(videoFrameRate: 25, klv: true, synthetic: asked);

        Assert.Equal(2, reporting.Count);
        Assert.Equal(1, reporting.KlvIndex);
        Assert.False(reporting.KlvIsSynthetic);
        Assert.Equal(-1, reporting.SyntheticIndexOf(SyntheticTrackRole.PlatformMetadata));

        // And the rate is the sender's, not ours: nothing was appended, so nothing was declared.
        // A data track that declares no rate is taken to send sixty, which is where 85 comes from.
        Assert.Equal(85, reporting.PacketsPerSecond);

        using var silent = Layout(videoFrameRate: 25, synthetic: asked);

        Assert.Equal(2, silent.Count);
        Assert.Equal(1, silent.KlvIndex);
        Assert.True(silent.KlvIsSynthetic);
        Assert.Equal(1, silent.SyntheticIndexOf(SyntheticTrackRole.PlatformMetadata));
    }

    /// <summary>
    /// A reconnect that brings the same shape matches, so the buffer survives and a recording in
    /// progress carries on appending.
    ///
    /// This is why the layout is augmented before <see cref="StreamHub.Adopt"/> rather than swapped
    /// into a running hub. Adopt compares what arrives with what it holds; a hub holding an
    /// augmented layout and a reconnect handing it a bare one do not match, and every reconnect of
    /// a static camera would then rebuild the buffer and close its own recording. Augmenting first
    /// means both sides of that comparison are the same kind of thing.
    ///
    /// The second half is the case the role exists for. A camera that starts sending its own KLV
    /// presents a layout whose codec parameters are identical to the synthesised one - both are
    /// SMPTE KLV data streams with no width, no format and no extradata - so without comparing the
    /// roles it would match, the buffer would survive, and the publisher would carry on writing
    /// onto the index the sender is now using.
    /// </summary>
    [Fact]
    public void A_reconnect_carrying_the_same_synthetic_track_keeps_the_buffer()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var asked = new SyntheticTrack[] { new(SyntheticTrackRole.PlatformMetadata, 1) };
        var options = new LiveOptions();

        using var hub = new StreamHub("reconnecting", options, NullLogger.Instance);

        using var first = Layout(videoFrameRate: 25, synthetic: asked);
        using var again = Layout(videoFrameRate: 25, synthetic: asked);
        using var bare = Layout(videoFrameRate: 25);
        using var reporting = Layout(videoFrameRate: 25, klv: true, synthetic: asked);

        Assert.True(hub.Adopt(first));

        var buffer = hub.Buffer;

        Assert.NotNull(buffer);

        Assert.True(again.Matches(first));
        Assert.True(hub.Adopt(again), "a reconnect with the same synthetic track rebuilt the buffer");
        Assert.Same(buffer, hub.Buffer);

        // The same feed with its configuration removed - one fewer track - is a real change and is
        // reported as one.
        Assert.False(bare.Matches(again));

        // And the same count with a different provenance is too, although nothing in the codec
        // parameters says so.
        Assert.Equal(again.Count, reporting.Count);
        Assert.False(reporting.Matches(again));
        Assert.False(hub.Adopt(reporting), "a camera that started reporting for itself matched the layout that was synthesising for it");
        Assert.NotSame(buffer, hub.Buffer);
    }

    /// <summary>
    /// The deepest rollback <c>LiveStreamCoordinator.Fitting</c> would give a viewer of this
    /// stream: the viewer queue ceiling in seconds of this stream, less the room held back for the
    /// live flow.
    /// </summary>
    private static double Rollback(StreamLayout layout)
    {
        var options = new LiveOptions();

        return (options.ViewerQueuePackets / layout.PacketsPerSecond) - options.ViewerQueueSeconds;
    }

    /// <summary>
    /// A layout with the fields the depth is derived from and nothing else, built without a
    /// transport: a frame rate is what a demultiplexer reports after probing, and none of this
    /// needs a real one.
    /// </summary>
    private static StreamLayout Layout(
        int videoFrameRate,
        int sampleRate = 0,
        int frameSize = 0,
        bool klv = false,
        IReadOnlyList<SyntheticTrack>? synthetic = null)
    {
        var format = ffmpeg.avformat_alloc_context();

        try
        {
            var video = ffmpeg.avformat_new_stream(format, null);

            video->time_base = new AVRational { num = 1, den = 90_000 };
            video->avg_frame_rate = new AVRational { num = videoFrameRate, den = 1 };
            video->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_VIDEO;
            video->codecpar->codec_id = AVCodecID.AV_CODEC_ID_MPEG2VIDEO;
            video->codecpar->width = 320;
            video->codecpar->height = 240;

            if (sampleRate > 0)
            {
                var audio = ffmpeg.avformat_new_stream(format, null);

                audio->time_base = new AVRational { num = 1, den = sampleRate };
                audio->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_AUDIO;
                audio->codecpar->codec_id = AVCodecID.AV_CODEC_ID_AAC;
                audio->codecpar->sample_rate = sampleRate;
                audio->codecpar->frame_size = frameSize;
            }

            if (klv)
            {
                // What a real KLV-carrying transport presents: a data stream whose registration
                // descriptor says KLVA, which libav reports as the SMPTE KLV codec.
                var data = ffmpeg.avformat_new_stream(format, null);

                data->time_base = new AVRational { num = 1, den = 90_000 };
                data->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_DATA;
                data->codecpar->codec_id = AVCodecID.AV_CODEC_ID_SMPTE_KLV;
            }

            return StreamLayout.From(format, synthetic ?? []);
        }
        finally
        {
            ffmpeg.avformat_free_context(format);
        }
    }
}
