using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// What every consumer that writes bytes depends on, and what nothing tested until a synthetic
/// track needed it.
///
/// Three things are asserted here and each is load-bearing for metadata this replica produces
/// rather than receives. That KLV survives the muxer byte for byte with its timestamp, which the
/// whole idea rests on and which had been believed rather than checked. That a packet on an index
/// the muxer's layout never had is dropped, because four consumers subscribe to every index and
/// never re-read the layout. And that a metadata packet stamped at the live edge leaves the
/// timeline alone - with the same test showing what stamping it two seconds late would have done,
/// because a guard nobody can see fail is a guard nobody can trust.
/// </summary>
public sealed unsafe class PacketMuxerTests
{
    /// <summary>The MPEG-TS clock, which is also what a synthetic track is given.</summary>
    private static readonly AVRational Clock = new() { num = 1, den = 90_000 };

    /// <summary>One second on that clock.</summary>
    private const long Second = 90_000;

    /// <summary>
    /// A KLV packet goes through the muxer and comes back out of the container unchanged, at the
    /// presentation time it was written with.
    ///
    /// The premise of carrying synthesised ST 0601 in band. If the muxer rewrote a metadata
    /// packet's bytes - the five-byte ST 1402 metadata AU cell header is exactly the way that
    /// happens, and is why the synthetic track leaves <c>codecpar-&gt;profile</c> unset so the
    /// stream stays type 0x06 - then every consumer downstream would read a truncated local set
    /// whose checksum fails, and <see cref="KlvExtractor"/> would count it as rejected rather than
    /// decode it.
    /// </summary>
    [Fact]
    public void A_metadata_packet_comes_back_out_of_the_container_unchanged()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = Augmented(videoFrameRate: 25);

        var klv = layout.SyntheticIndexOf(SyntheticTrackRole.PlatformMetadata);
        var payload = Payload(seed: 7, length: 96);

        using var written = new MemoryStream();

        using (var muxer = new PacketMuxer(written, layout))
        {
            // Video first, because a muxer's timeline starts at its first packet and a metadata
            // track alone would be measuring itself.
            for (var frame = 0; frame < 25; frame++)
            {
                muxer.Write(Video(frame * Second / 25));
            }

            muxer.Write(new MediaPacket(klv, payload, Second, Second, 0, IsKeyframe: true));

            muxer.Close();
        }

        var (data, pts) = ReadBackMetadata(written.ToArray());

        Assert.Equal(payload, data);

        // The same instant, on whatever clock the container chose to write it on. Compared in
        // ticks rather than asserted equal to 90000, because the muxer is free to rescale.
        Assert.Equal(1d, pts * ffmpeg.av_q2d(Clock), 3);
    }

    /// <summary>
    /// A packet on a stream index this muxer's layout never had is dropped rather than thrown.
    ///
    /// Not a hypothetical. <c>Serve</c>, <see cref="StreamRecorder"/>, every
    /// <see cref="StreamForwarder"/> and the snapshot muxer all subscribe to every index and none
    /// of them re-reads the layout afterwards, so any arrangement in which a stream's layout gains
    /// a track while a consumer is attached hands that consumer an index its own muxer does not
    /// have. Unguarded this is an <see cref="IndexOutOfRangeException"/> on the hub's publishing
    /// thread, which ends the stream for every viewer, forward and recording on it; guarded, the
    /// stale consumer misses the one track and carries on.
    /// </summary>
    [Fact]
    public void A_packet_on_an_index_the_layout_never_had_is_dropped_rather_than_thrown()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        // A bare layout: one video stream, so index 1 is one the muxer has never heard of.
        using var layout = Bare(videoFrameRate: 25);
        using var written = new MemoryStream();
        using var muxer = new PacketMuxer(written, layout);

        muxer.Write(Video(0));

        var before = written.Length;

        // One index past the end of the mapping, and one below it. Both were an
        // IndexOutOfRangeException before the guard; the negative one is what a caller computing an
        // index from a layout it no longer holds produces.
        muxer.Write(new MediaPacket(1, Payload(seed: 3, length: 32), Second, Second, 0, IsKeyframe: true));
        muxer.Write(new MediaPacket(-1, Payload(seed: 4, length: 32), Second, Second, 0, IsKeyframe: true));

        // Read before the trailer, so this is about what those two writes did and nothing else.
        Assert.Equal(before, written.Length);
        Assert.Null(muxer.Fault);

        // And the muxer still works afterwards, which is the point of dropping rather than
        // faulting: the consumer misses the one track it cannot hold, not the stream.
        muxer.Write(Video(Second / 25));

        Assert.True(written.Length > before);

        muxer.Close();

        Assert.Null(muxer.Fault);
    }

    /// <summary>
    /// A metadata packet published through the hub lands at the live edge, and the muxer's timeline
    /// is the same afterwards as if it had never been published.
    ///
    /// This is the test the synthetic track exists to be held to. <c>PacketMuxer.Rebase</c> treats a
    /// packet landing more than <c>ClockReset</c> - one second - behind what it has already written
    /// as an encoder that has restarted its clock, and moves the entire timeline onto it. That is
    /// right for a returning encoder and catastrophic for a locally produced packet: every viewer,
    /// forward and recording on the stream shares the muxer's timeline, and a metadata packet
    /// stamped from its own clock rather than from the stream's would rebase all of them.
    ///
    /// The second half of the test is the control. It writes the same packet two seconds behind the
    /// edge instead, and shows the timeline collapsing - so the first half is a guard that has been
    /// seen to fail rather than an assertion that happens to pass.
    /// </summary>
    [Fact]
    public void Metadata_published_at_the_live_edge_leaves_the_timeline_alone()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        using var layout = Augmented(videoFrameRate: 25);

        var klv = layout.SyntheticIndexOf(SyntheticTrackRole.PlatformMetadata);
        var options = new LiveOptions();

        using var hub = new StreamHub("live-edge", options, NullLogger.Instance);

        hub.Adopt(layout);

        MediaPacket? published = null;

        using (var subscription = hub.Subscribe(capacity: 64, OverflowPolicy.SkipToLive, streamIndexes: [klv]))
        {
            // Five seconds of video, so anything stamped from a clock of its own rather than from
            // the stream's would be seconds behind by the time it was published.
            for (var frame = 0; frame < 125; frame++)
            {
                var pts = frame * Second / 25;

                hub.Publish(Video(pts), pts);
            }

            Assert.True(hub.PublishAtLiveEdge(SyntheticTrackRole.PlatformMetadata, Payload(seed: 11, length: 64)));
            Assert.True(subscription.Packets.TryRead(out published));
        }

        Assert.NotNull(published);

        // Exactly at the edge. Not behind it, which rebases the timeline; not ahead of it, which
        // advances the buffer's EndPts, shrinks HeldSeconds and evicts the oldest segment early.
        Assert.Equal(124 * Second / 25, published.Pts);
        Assert.Equal(published.Pts, published.Dts);
        Assert.Equal(0, published.Duration);

        // Three timelines from the same video: one with no metadata at all, one with the packet
        // the hub stamped, and one with the same packet two seconds behind the edge.
        var alone = Timeline(layout, klv, at: null);
        var stamped = Timeline(layout, klv, published.Pts);
        var late = Timeline(layout, klv, published.Pts - (2 * Second));

        // Six seconds of video went in, so six seconds is where the timeline ends - and the hub's
        // packet left it exactly where it would have been had nothing been published at all.
        Assert.Equal(149 / 25d, alone, 2);
        Assert.Equal(alone, stamped, 3);

        // The control. The rebase does not show up on the packet that caused it - Rebase keeps
        // _lastOut at the maximum it has reached - it shows up on every packet after it, which is
        // the whole reason it is so damaging: the video carries on and lands two seconds further
        // along than it should, in every viewer's stream and in every recording.
        Assert.True(
            late > alone + 1.5,
            $"the control did not rebase, so this test proves nothing: {late:F2}s against {alone:F2}s");
    }

    /// <summary>
    /// After a reconnect to a sender that carries its own KLV, nothing is published at all - and in
    /// particular nothing is published onto the sender's track.
    ///
    /// The window is a reconnect between a publisher resolving a track index and the hub writing
    /// to it, and what makes it worth a test is not its width but what it does. Index 1 of the
    /// departed layout is this service's synthetic track; index 1 of the arriving one can be a real
    /// platform's telemetry track. A packet on the stale index does not fail - it lands, at the
    /// live edge, in every viewer, forward and recording, carrying a configured position under
    /// ST 0601 tag 10 saying it was synthesised, while <see cref="KlvExtractor"/> reports it as
    /// genuine, because provenance is read from the layout and the layout now says that index is
    /// the sender's. Real sensor telemetry, silently falsified.
    ///
    /// So the track is named by its role and resolved inside the same lock that publishes, and
    /// re-reading the layout once a tick is not a substitute: it closes the window between ticks
    /// and leaves the one inside a tick exactly where it was.
    /// </summary>
    [Fact]
    public void Nothing_is_published_onto_a_track_the_sender_has_taken_back()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();

        using var hub = new StreamHub("taken-back", options, NullLogger.Instance);

        using var synthesising = Augmented(videoFrameRate: 25);
        using var reporting = Reporting(videoFrameRate: 25);

        hub.Adopt(synthesising);

        // Attached to index 1 and never re-reading the layout, which is every consumer that writes
        // bytes: Serve, the recorder, every forward and the snapshot muxer.
        using var subscription = hub.Subscribe(capacity: 64, OverflowPolicy.SkipToLive, streamIndexes: [1]);

        hub.Publish(Video(0), 0);

        Assert.True(hub.PublishAtLiveEdge(SyntheticTrackRole.PlatformMetadata, Payload(seed: 11, length: 64)));
        Assert.True(subscription.Packets.TryRead(out _));

        // The camera comes back reporting its own telemetry. Index 1 is now the sender's KLV track:
        // same count, same codec parameters, different provenance.
        Assert.Equal(synthesising.Count, reporting.Count);
        Assert.Equal(1, reporting.KlvIndex);
        Assert.False(reporting.KlvIsSynthetic);

        hub.Adopt(reporting);

        // A live edge on the new layout, so the refusal below is about the track rather than about
        // there being nothing to stamp against. Without this the test would pass for the wrong
        // reason and prove nothing.
        hub.Publish(Video(Second), Second);

        Assert.False(
            hub.PublishAtLiveEdge(SyntheticTrackRole.PlatformMetadata, Payload(seed: 11, length: 64)),
            "a synthesised set was published after the sender took the track back");

        // Nothing of ours reached a real telemetry track.
        Assert.False(subscription.Packets.TryRead(out var leaked), $"a packet reached the sender's own KLV track: {leaked}");

        // And that emptiness is the refusal rather than a subscription that had stopped delivering:
        // the sender's own packet on the same index arrives. Without this the assertion above would
        // pass whatever the hub did.
        var reported = new MediaPacket(1, Payload(seed: 21, length: 48), Second, Second, 0, IsKeyframe: true);

        hub.Publish(reported, Second);

        Assert.True(subscription.Packets.TryRead(out var arrived));
        Assert.Equal(reported.Data, arrived!.Data);
    }

    /// <summary>
    /// Six seconds of video with one metadata packet five seconds in, and how far the muxer's
    /// timeline reached. A null <paramref name="at"/> writes the video alone.
    /// </summary>
    private static double Timeline(StreamLayout layout, int klv, long? at)
    {
        using var written = new MemoryStream();
        using var muxer = new PacketMuxer(written, layout);

        for (var frame = 0; frame < 150; frame++)
        {
            if (frame == 125 && at is { } pts)
            {
                muxer.Write(new MediaPacket(klv, Payload(seed: 11, length: 64), pts, pts, 0, IsKeyframe: true));
            }

            muxer.Write(Video(frame * Second / 25));
        }

        var timeline = muxer.TimelineSeconds;

        muxer.Close();

        return timeline;
    }

    /// <summary>
    /// The first metadata packet in a written MPEG-TS, with its presentation time rescaled onto
    /// <see cref="Clock"/>.
    /// </summary>
    private static (byte[] Data, long Pts) ReadBackMetadata(byte[] container)
    {
        var format = ffmpeg.avformat_alloc_context();
        var reader = new AvioReader(new MemoryStream(container));
        AVPacket* packet = null;

        format->pb = reader.Context;
        format->flags |= ffmpeg.AVFMT_FLAG_CUSTOM_IO;

        try
        {
            Assert.True(
                ffmpeg.avformat_open_input(&format, null, ffmpeg.av_find_input_format("mpegts"), null) >= 0,
                "the muxer wrote something libav will not read back");

            Assert.True(ffmpeg.avformat_find_stream_info(format, null) >= 0);

            packet = ffmpeg.av_packet_alloc();

            while (ffmpeg.av_read_frame(format, packet) >= 0)
            {
                var stream = format->streams[packet->stream_index];

                if (stream->codecpar->codec_id != AVCodecID.AV_CODEC_ID_SMPTE_KLV)
                {
                    ffmpeg.av_packet_unref(packet);
                    continue;
                }

                var data = new byte[packet->size];
                Marshal.Copy((IntPtr)packet->data, data, 0, packet->size);

                var pts = ffmpeg.av_rescale_q(packet->pts, stream->time_base, Clock);

                ffmpeg.av_packet_unref(packet);

                return (data, pts);
            }

            Assert.Fail("the container carried no KLV packet at all");

            return ([], 0);
        }
        finally
        {
            if (packet is not null)
            {
                ffmpeg.av_packet_free(&packet);
            }

            if (format is not null)
            {
                ffmpeg.avformat_close_input(&format);
            }

            reader.Dispose();
        }
    }

    private static MediaPacket Video(long pts) => new(0, Payload(seed: 1, length: 256), pts, pts, Second / 25, true);

    private static byte[] Payload(byte seed, int length)
    {
        var payload = new byte[length];

        for (var index = 0; index < length; index++)
        {
            payload[index] = (byte)(seed + index);
        }

        return payload;
    }

    private static StreamLayout Bare(int videoFrameRate) => Layout(videoFrameRate, []);

    /// <summary>A sender carrying KLV of its own, which gets no synthetic track however much is asked for.</summary>
    private static StreamLayout Reporting(int videoFrameRate)
        => Layout(videoFrameRate, [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, StaticSensorPublisher.PacketsPerSecond)], klv: true);

    private static StreamLayout Augmented(int videoFrameRate)
        => Layout(videoFrameRate, [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, StaticSensorPublisher.PacketsPerSecond)]);

    private static StreamLayout Layout(int videoFrameRate, IReadOnlyList<SyntheticTrack> synthetic, bool klv = false)
    {
        var format = ffmpeg.avformat_alloc_context();

        try
        {
            var video = ffmpeg.avformat_new_stream(format, null);

            video->time_base = Clock;
            video->avg_frame_rate = new AVRational { num = videoFrameRate, den = 1 };
            video->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_VIDEO;
            video->codecpar->codec_id = AVCodecID.AV_CODEC_ID_MPEG2VIDEO;
            video->codecpar->width = 320;
            video->codecpar->height = 240;

            if (klv)
            {
                var data = ffmpeg.avformat_new_stream(format, null);

                data->time_base = Clock;
                data->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_DATA;
                data->codecpar->codec_id = AVCodecID.AV_CODEC_ID_SMPTE_KLV;
            }

            return StreamLayout.From(format, synthetic);
        }
        finally
        {
            ffmpeg.avformat_free_context(format);
        }
    }
}
