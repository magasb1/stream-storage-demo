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
        Assert.Equal(100, layout.QueueDepth(layout.PacketsPerSecond, seconds: 4, ceiling: 2_000));
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
        Assert.Equal(288, layout.QueueDepth(layout.PacketsPerSecond, seconds: 4, ceiling: 2_000));
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
        Assert.Equal(240, claimed.QueueDepth(claimed.PacketsPerSecond, seconds: 4, ceiling: 2_000));

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

        Assert.Equal(2_000, layout.QueueDepth(layout.PacketsPerSecond, seconds: 60, ceiling: 2_000));
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
        Assert.Equal(4, layout.QueueDepth(layout.PacketsPerSecond, seconds: 4, ceiling: 2_000));

        // And asking for less than the floor in seconds gets the floor, in that stream's own terms:
        // half a second of this stream rounds up to one packet, and a queue has to hold a few.
        Assert.Equal(4, layout.QueueDepth(layout.PacketsPerSecond, seconds: 0.1, ceiling: 2_000));
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
    /// A sender that declares far below what it sends has its viewers' queues sized from what
    /// arrives. Issue #26, and the case that is unreachable from the declaration alone.
    ///
    /// <c>Believable</c> rejects a rate that cannot be true and cannot reject one that is merely
    /// too low: a 1 fps time-lapse and a 50 fps camera lying about itself present the same thing.
    /// Believed, the liar's viewers get the floor - four packets - against the two hundred four
    /// seconds of its traffic actually needs, so every one of them skips to live almost
    /// continuously while the stream reports itself healthy.
    /// </summary>
    [Fact]
    public void A_sender_declaring_far_below_what_it_sends_is_sized_from_what_arrives()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var lying = Layout(videoFrameRate: 1);

        Assert.Equal(1, lying.PacketsPerSecond);

        // What it was believed for: four seconds of a stream said to send one packet a second is
        // one packet, raised to the floor a transport's interleaving needs.
        Assert.Equal(4, lying.QueueDepth(lying.PacketsPerSecond, seconds: 4, ceiling: 2_000));

        // And what fifty arriving a second is worth instead.
        Assert.Equal(50, lying.EffectivePacketsPerSecond(observedArrivals: 50));
        Assert.Equal(
            200,
            lying.QueueDepth(lying.EffectivePacketsPerSecond(50), seconds: 4, ceiling: 2_000));
    }

    /// <summary>
    /// A stream carrying #35's synthetic track is not disturbed by any of this, which is the
    /// regression this change most needs guarded.
    ///
    /// <c>StreamHub.PublishAtLiveEdge</c> deliberately leaves <c>StreamHub.Packets</c> alone, so an
    /// observation taken from those counters is arrivals only and omits the synthetic track by
    /// construction. A 25 fps stream with a 1 Hz track therefore observes 25 against a declared 26.
    ///
    /// The 26 below is necessary but not sufficient, and saying so is the point of this note: at
    /// 25 observed against 26 declared, max(26, 25 + 1) and max(26, 25) are both 26, so that
    /// assertion alone passes with the addition deleted. Verified by mutation rather than assumed.
    /// The case that discriminates is one where the observation wins, so the addend is inside the
    /// figure that is returned - 30 arriving reads 31, not 30 - and it is asserted below beside
    /// the invariance it exists to protect.
    /// </summary>
    [Fact]
    public void A_synthetic_tracks_own_rate_is_added_back_to_what_was_observed()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var declared = Layout(
            videoFrameRate: 25,
            synthetic: [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 1)]);

        Assert.Equal(26, declared.PacketsPerSecond);
        Assert.Equal(1, declared.SyntheticPacketsPerSecond);

        // What the change must not do: 25 arriving plus the 1 this service publishes is 26, which
        // is what was declared, so nothing moves.
        Assert.Equal(26, declared.EffectivePacketsPerSecond(observedArrivals: 25));

        // And the assertion that actually pins the composition, because it is the one that fails
        // when the synthetic rate is not added: 30 arriving is 31, not 30. Above the declaration,
        // so the addend survives into the answer instead of being masked by the maximum.
        Assert.Equal(31, declared.EffectivePacketsPerSecond(observedArrivals: 30));

        Assert.Equal(104, declared.QueueDepth(declared.EffectivePacketsPerSecond(25), seconds: 4, ceiling: 2_000));
        Assert.Equal(104, declared.QueueDepth(declared.PacketsPerSecond, seconds: 4, ceiling: 2_000));

        // Had the observation been used bare it would have read 25 - the figure this must not
        // produce, and the one a stream of exactly this shape yields every beat.
        Assert.NotEqual(25, declared.EffectivePacketsPerSecond(25));

        // A layout with no synthetic track has nothing to add back, and one whose sender sends its
        // own KLV got no synthetic track at all.
        using var bare = Layout(videoFrameRate: 25);
        using var reporting = Layout(
            videoFrameRate: 25,
            klv: true,
            synthetic: [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 1)]);

        Assert.Equal(0, bare.SyntheticPacketsPerSecond);
        Assert.Equal(0, reporting.SyntheticPacketsPerSecond);
        Assert.Equal(25, bare.EffectivePacketsPerSecond(observedArrivals: 25));

        // The same 30 arrivals against the same declaration, with nothing to add back, reads 30 -
        // which is what makes the 31 above a statement about the addend rather than about the
        // arithmetic around it.
        Assert.Equal(30, bare.EffectivePacketsPerSecond(observedArrivals: 30));
    }

    /// <summary>
    /// Before the first sample, on an interrupted stream, and on a sender that overstates, the
    /// declaration is what stands - which is the case for taking the larger of the two rather than
    /// the observation.
    /// </summary>
    [Fact]
    public void An_unmeasured_or_overstating_stream_keeps_its_declaration()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = Layout(videoFrameRate: 25);

        // Zero is what a stream that has not completed a sample window reports, and it is also
        // what an interrupted one reports once a window passes with nothing arriving: neither is
        // asked to say which it is, because both want the same answer.
        Assert.Equal(25, layout.EffectivePacketsPerSecond(observedArrivals: 0));

        // A sender that overstates keeps today's behaviour exactly - a queue shallower in seconds
        // than asked for, bounded by the ceiling - rather than gaining a new one out of a change
        // aimed at the opposite case.
        using var overstating = Layout(videoFrameRate: 100);

        Assert.Equal(100, overstating.PacketsPerSecond);
        Assert.Equal(100, overstating.EffectivePacketsPerSecond(observedArrivals: 25));

        // Nothing that cannot be read moves it either. These do not arise from
        // LiveStreamEntry's arithmetic - a difference of packet totals over a measured interval -
        // but the figure is multiplied by seconds and cast to a queue depth, so it is closed here
        // rather than argued about.
        Assert.Equal(25, layout.EffectivePacketsPerSecond(double.NaN));
        Assert.Equal(25, layout.EffectivePacketsPerSecond(double.PositiveInfinity));
        Assert.Equal(25, layout.EffectivePacketsPerSecond(-5));
    }

    /// <summary>
    /// An honest stream's measured rate straddles its declaration, which is why
    /// <c>LiveStreamCoordinator.MisdeclaredBeyond</c> exists and why it is a band rather than a
    /// strict comparison.
    ///
    /// This repository's own pinned case is the demonstration. 25 fps video beside 48 kHz AAC in
    /// 1024-sample frames declares 71.875 packets a second - 25 and 46.875 - and neither is a
    /// whole number of packets in a two-second beat: the stream delivers 50 video packets and
    /// either 93 or 94 audio ones, so the window reads 71.5 or 72. The high one is above the
    /// declaration through no fault of the sender, and about half of all windows are the high one.
    ///
    /// Compared strictly, that stream is called a misdeclarer. The band is what stops it; the
    /// figures here are the premise the band rests on, and they are pinned so that a change to the
    /// rate arithmetic cannot quietly move them out from under it. The band itself is the
    /// coordinator's, because only the log line reads it - a queue is sized from the larger of the
    /// two either way, which is why 72 is still what comes back below.
    /// </summary>
    [Fact]
    public void An_honest_streams_measured_rate_straddles_its_declaration()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var honest = Layout(videoFrameRate: 25, sampleRate: 48_000, frameSize: 1_024);

        Assert.Equal(71.875, honest.PacketsPerSecond, 3);

        // Both windows an honest sender produces, and the high one is above the declaration.
        Assert.Equal(71.875, honest.EffectivePacketsPerSecond(observedArrivals: 71.5));
        Assert.Equal(72, honest.EffectivePacketsPerSecond(observedArrivals: 72));

        // And the overshoot is a fraction of a percent, nowhere near the 25% band and further
        // still from the fifty times the case in issue #26 is about.
        Assert.True(
            72 < honest.PacketsPerSecond * 1.25,
            "an honest stream's high window reached the band that marks a sender as misdeclaring");
    }

    /// <summary>
    /// What this does to a viewer's deepest rollback, which is the other consumer of the rate and
    /// the one with a measured figure already pinned to it.
    ///
    /// Two regions. <c>Fitting</c>'s room is <c>ViewerQueuePackets / R - ViewerQueueSeconds</c>,
    /// and <c>ResolvePreroll</c> then clamps it to what the rolling buffer actually holds. Room
    /// exceeds thirty seconds whenever R is below 2000/34 = 58.8, so around and below that the
    /// buffer binds and Fitting is moot: the headline 76 and 72.9 seconds pinned above are both
    /// well inside that region and both already undeliverable. Above it Fitting binds, and there
    /// this change moves the figure toward the truth rather than away from it.
    ///
    /// 58.8 is a floor on the boundary rather than the boundary, and the difference is the
    /// buffer's own rule: <c>RollingBuffer.Evict</c> treats <c>BufferWindowSeconds</c> as a floor
    /// and keeps the last segment however long it runs, so <c>HeldSeconds</c> sits in
    /// [30, 30 + one keyframe interval). A sender with a ten-second GOP holds about forty, and its
    /// boundary is 2000/44 = 45.5 - so between 45.5 and 58.8 a coarse-GOP stream's delivered
    /// rollback does move. That movement is a correction, not a regression: the figure it moves
    /// toward is what the stream is actually sending. The assertions below use the configured
    /// window, which is the floor, so they are the conservative end of the claim rather than all
    /// of it.
    /// </summary>
    [Fact]
    public void A_rollback_is_unchanged_below_the_rate_at_which_fitting_binds()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();

        // The boundary at the buffer's floor, from the options rather than from the prose above. A
        // stream whose keyframe interval makes HeldSeconds larger has a lower one; see the note.
        Assert.Equal(58.8, options.ViewerQueuePackets / (options.BufferWindowSeconds + options.ViewerQueueSeconds), 1);

        using var bare = Layout(videoFrameRate: 25);
        using var declared = Layout(
            videoFrameRate: 25,
            synthetic: [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 1)]);

        // An honest stream below the boundary: the observation agrees with the declaration, so the
        // room is the same figure it was, and the buffer binds at 30 either way.
        Assert.Equal(76, Rollback(bare, observedArrivals: 25), 1);
        Assert.Equal(72.9, Rollback(declared, observedArrivals: 25), 1);
        Assert.True(Rollback(declared, observedArrivals: 25) > options.BufferWindowSeconds);

        // The liar from the issue, which is the case that crosses the boundary. Declared, it claims
        // a rollback of 1996 seconds against a buffer holding 30; observed at fifty arriving a
        // second it says 36, and with #35's track 35.2 - still more than the buffer holds, so the
        // thirty seconds is delivered either way and the figure the header promises stops being a
        // fiction.
        using var lying = Layout(videoFrameRate: 1);
        using var lyingWithTrack = Layout(
            videoFrameRate: 1,
            synthetic: [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, 1)]);

        Assert.Equal(1_996, Rollback(lying, observedArrivals: 0), 1);
        Assert.Equal(36, Rollback(lying, observedArrivals: 50), 1);
        Assert.Equal(35.2, Rollback(lyingWithTrack, observedArrivals: 50), 1);
        Assert.True(Rollback(lyingWithTrack, observedArrivals: 50) > options.BufferWindowSeconds);
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
    /// <param name="observedArrivals">
    /// What the stream is measured to be sending, excluding synthetic tracks; zero for a stream
    /// that has not been measured, which is what makes the declaration stand.
    /// </param>
    private static double Rollback(StreamLayout layout, double observedArrivals = 0)
    {
        var options = new LiveOptions();
        var rate = layout.EffectivePacketsPerSecond(observedArrivals);

        return (options.ViewerQueuePackets / rate) - options.ViewerQueueSeconds;
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
