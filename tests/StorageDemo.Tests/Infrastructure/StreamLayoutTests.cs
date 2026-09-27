using FFmpeg.AutoGen.Abstractions;
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
    /// A layout with the fields the depth is derived from and nothing else, built without a
    /// transport: a frame rate is what a demultiplexer reports after probing, and none of this
    /// needs a real one.
    /// </summary>
    private static StreamLayout Layout(int videoFrameRate, int sampleRate = 0, int frameSize = 0)
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

            return StreamLayout.From(format);
        }
        finally
        {
            ffmpeg.avformat_free_context(format);
        }
    }
}
