using System.Diagnostics;
using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>Why a feed stopped, which is what decides whether the stream waits or ends.</summary>
public enum DemuxOutcome
{
    /// <summary>The sender went away or fell silent. The stream becomes interrupted, not gone.</summary>
    FeedEnded,

    /// <summary>The transport never produced a readable stream at all.</summary>
    NeverStarted,

    /// <summary>The service asked it to stop.</summary>
    Stopped,
}

/// <summary>
/// Reads one accepted connection and feeds one hub. This is the demultiplexing half of what used
/// to be a single remux loop; the multiplexing half now belongs to each consumer that writes bytes.
///
/// It takes an already-open transport rather than a URL, because the accept happened on the
/// listener's thread and that thread had to move on. libav is told the container is MPEG-TS
/// rather than left to probe it: this is a contribution ingest, and probing costs a read before
/// the first packet reaches anybody.
/// </summary>
public sealed unsafe class StreamDemuxer(
    IOptions<LiveOptions> options,
    ILogger<StreamDemuxer> logger)
{
    private readonly LiveOptions _options = options.Value;

    /// <summary>
    /// How long libav may spend working out what is arriving, and how much it may read doing it.
    ///
    /// This is dead time between a camera connecting and its stream being on air, which for a
    /// service whose purpose is being live is the number that matters after a reconnect. libav's
    /// own defaults are five seconds and five megabytes; MPEG-TS repeats its tables every hundred
    /// milliseconds, so far less is enough for a source that presents everything at once.
    ///
    /// http is given its own, larger budget - see <see cref="LiveOptions.HttpProbeSeconds"/> for
    /// why a TCP handshake and a playlist fetch cost more than this default assumes.
    /// </summary>
    private void LimitProbe(AVDictionary** options, bool http)
    {
        var seconds = http ? _options.HttpProbeSeconds : _options.ProbeSeconds;
        var bytes = http ? _options.HttpProbeBytes : _options.ProbeBytes;

        ffmpeg.av_dict_set(options, "analyzeduration", ((long)(seconds * 1_000_000)).ToString(), 0);
        ffmpeg.av_dict_set(options, "probesize", bytes.ToString(), 0);
    }

    /// <summary>Whether a URL is pulled over http or https, where nothing paces the read but this service.</summary>
    private static bool IsHttpLike(string url)
        => url.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            || url.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Runs until the feed stops or the token is cancelled, publishing every packet to the hub.
    /// Takes ownership of <paramref name="transport"/> and closes it on the way out.
    /// </summary>
    public DemuxOutcome Run(AVIOContext* transport, StreamHub hub, CancellationToken cancellationToken)
    {
        var format = ffmpeg.avformat_alloc_context();

        if (format is null)
        {
            ffmpeg.avio_closep(&transport);
            return DemuxOutcome.NeverStarted;
        }

        format->pb = transport;

        // Tells avformat the transport is ours: avformat_close_input leaves it alone, and closing
        // it is this method's job.
        format->flags |= ffmpeg.AVFMT_FLAG_CUSTOM_IO;

        AVDictionary* options = null;

        try
        {
            LimitProbe(&options, http: false);

            // The container is known rather than probed. This is a contribution ingest, and
            // guessing the format costs a read before the first packet can reach anybody.
            if (ffmpeg.avformat_open_input(&format, null, ffmpeg.av_find_input_format("mpegts"), &options) < 0)
            {
                logger.LogWarning("'{Name}' connected but sent nothing readable", hub.Name);
                return DemuxOutcome.NeverStarted;
            }

            // Never paced: an accepted socket is fed by an encoder pushing in real time already,
            // and pacing it in software besides would only ever add latency to a live connection.
            return Read(format, hub, cancellationToken, realtime: false);
        }
        finally
        {
            ffmpeg.av_dict_free(&options);

            if (format is not null)
            {
                ffmpeg.avformat_close_input(&format);
            }

            // Ours to close, because of AVFMT_FLAG_CUSTOM_IO above.
            if (transport is not null)
            {
                ffmpeg.avio_closep(&transport);
            }
        }
    }

    /// <summary>
    /// The manual path: this replica opens the input itself, for a protocol that cannot name
    /// itself and so could never have arrived at a listening port. Everything after the open is the
    /// same, which is the point - once a demultiplexer exists the two are indistinguishable.
    ///
    /// <paramref name="inputOptions"/> is used as given for every scheme except http and https,
    /// which use <see cref="LiveOptions.HttpInputOptions"/> instead regardless of what was passed:
    /// those options are udp's, and an http source has no use for a fifo it does not have.
    /// </summary>
    public DemuxOutcome Run(
        string url,
        IReadOnlyDictionary<string, string>? inputOptions,
        StreamHub hub,
        CancellationToken cancellationToken)
    {
        AVFormatContext* format = null;
        AVDictionary* options = null;
        var http = IsHttpLike(url);

        try
        {
            LimitProbe(&options, http);

            foreach (var (key, value) in (http ? _options.HttpInputOptions : inputOptions) ?? new Dictionary<string, string>())
            {
                ffmpeg.av_dict_set(&options, key, value, 0);
            }

            if (ffmpeg.avformat_open_input(&format, url, null, &options) < 0)
            {
                logger.LogWarning("Nothing arrived on {Url} for '{Name}'", url, hub.Name);
                return DemuxOutcome.NeverStarted;
            }

            // http and https are paced in software because nothing else paces them: a pull is an
            // ordinary read against whatever a server has already published, and nothing stops
            // libav fetching every available segment back to back well ahead of the wall clock the
            // video was recorded against. udp, rtp and srt need none of this - the encoder's own
            // send rate already paces av_read_frame for those.
            return Read(format, hub, cancellationToken, realtime: http);
        }
        finally
        {
            ffmpeg.av_dict_free(&options);

            if (format is not null)
            {
                ffmpeg.avformat_close_input(&format);
            }
        }
    }

    private DemuxOutcome Read(AVFormatContext* format, StreamHub hub, CancellationToken cancellationToken, bool realtime)
    {
        AVPacket* packet = null;

        try
        {
            if (ffmpeg.avformat_find_stream_info(format, null) < 0)
            {
                logger.LogWarning("'{Name}' carries no stream information", hub.Name);
                return DemuxOutcome.NeverStarted;
            }

            // Augmented here, between reading what arrived and handing it to the hub, and nowhere
            // else. Adopt is what compares the new layout with the one the hub holds, so both
            // sides of that comparison have to be the same kind of thing: a hub holding an
            // augmented layout and a reconnect handing it a bare one do not match, the buffer is
            // rebuilt and any recording in progress closes - which would mean every reconnect of a
            // static camera broke its own recording. Swapping the layout after Adopt instead would
            // be worse still: every consumer already attached subscribed to the indexes of the old
            // one and none of them re-reads it.
            var layout = StreamLayout.From(format, hub.SyntheticTracks);

            if (!hub.Adopt(layout))
            {
                // The encoder was reconfigured while it was away. Anything writing a file has to
                // close it: a container whose codec configuration changes halfway is not something
                // that will reliably play.
                logger.LogInformation(
                    "'{Name}' came back with a different layout ({Layout}), so its buffer starts fresh",
                    hub.Name,
                    layout.Describe());
            }

            packet = ffmpeg.av_packet_alloc();

            return packet is null
                ? DemuxOutcome.NeverStarted
                : Pump(format, packet, hub, cancellationToken, realtime);
        }
        finally
        {
            if (packet is not null)
            {
                ffmpeg.av_packet_free(&packet);
            }
        }
    }

    /// <summary>
    /// How far a single timestamp jump is trusted before pacing gives up waiting for it and starts
    /// again from wherever the stream now is.
    ///
    /// Both directions of a jump this large mean the same thing: whatever the pacing clock thought
    /// it knew about this stream's timeline no longer holds - an HLS playlist restarting, a
    /// discontinuity where the source's own clock stepped, or this service itself having fallen
    /// behind while a segment fetch was slow. Re-anchoring is safe either way; the alternative for
    /// a forward jump is stalling the whole pull for however large the jump was, and for a backward
    /// one is every packet after it reading as permanently behind schedule.
    /// </summary>
    private static readonly TimeSpan MaxPaceGap = TimeSpan.FromSeconds(5);

    private DemuxOutcome Pump(
        AVFormatContext* format,
        AVPacket* packet,
        StreamHub hub,
        CancellationToken cancellationToken,
        bool realtime)
    {
        var layout = hub.Layout!;
        var reference = layout.ReferenceTimeBase;
        var lastReferencePts = 0L;

        // Wall-clock pacing for a source nothing else paces - see realtime's caller for which
        // sources that is. Anchored on the first packet actually carrying a timestamp rather than
        // assumed to start at zero: HLS in particular routinely starts a live playlist's timeline
        // partway through an arbitrary running count, not at the beginning of anything.
        Stopwatch? clock = null;
        long? origin = null;

        // ponytail: cancellation is noticed between reads rather than during one, so a shutdown
        // can wait out the transport's read timeout. An AVIOInterruptCB would cut that short;
        // worth adding if a slow shutdown ever matters.
        while (!cancellationToken.IsCancellationRequested)
        {
            var read = ffmpeg.av_read_frame(format, packet);

            if (read < 0)
            {
                // The sender went away, or nothing arrived within the transport's read timeout.
                // Either way this feed has stopped; whether the stream has is not decided here.
                return DemuxOutcome.FeedEnded;
            }

            try
            {
                if (packet->stream_index < 0 || packet->stream_index >= layout.Count)
                {
                    continue;
                }

                var data = new byte[packet->size];
                Marshal.Copy((IntPtr)packet->data, data, 0, packet->size);

                if (packet->pts != ffmpeg.AV_NOPTS_VALUE)
                {
                    // Every stream carries its own clock; the buffer measures itself on one, so
                    // each packet is placed on the reference stream's scale as it arrives.
                    lastReferencePts = ffmpeg.av_rescale_q(
                        packet->pts,
                        layout.TimeBase(packet->stream_index),
                        reference);

                    if (realtime)
                    {
                        clock ??= Stopwatch.StartNew();
                        origin ??= lastReferencePts;

                        var decision = Pace(lastReferencePts, origin.Value, layout.SecondsPerTick, clock.Elapsed);

                        if (decision.Reanchor)
                        {
                            origin = lastReferencePts;
                            clock.Restart();
                        }
                        else if (decision.Wait is { } wait)
                        {
                            cancellationToken.WaitHandle.WaitOne(wait);
                        }
                    }
                }

                hub.Publish(
                    new MediaPacket(
                        packet->stream_index,
                        data,
                        packet->pts,
                        packet->dts,
                        packet->duration,
                        (packet->flags & ffmpeg.AV_PKT_FLAG_KEY) != 0),
                    lastReferencePts);
            }
            finally
            {
                ffmpeg.av_packet_unref(packet);
            }
        }

        return DemuxOutcome.Stopped;
    }

    /// <summary>What one packet's timestamp means for real-time pacing.</summary>
    /// <param name="Wait">How long to hold this packet before forwarding it, when it is running ahead of the wall clock.</param>
    /// <param name="Reanchor">
    /// Set when the gap between this packet's nominal time and the wall clock was too large to be
    /// ordinary jitter - true of both a forward jump and a backward one, for different reasons. The
    /// caller starts the clock over from this packet rather than either waiting out the full gap or
    /// letting every packet after it read as permanently behind schedule.
    /// </param>
    public readonly record struct PaceDecision(TimeSpan? Wait, bool Reanchor);

    /// <summary>
    /// Decided without touching a clock, taking wall-clock elapsed time as a plain value instead of
    /// reading one itself - the one part of pacing worth being able to prove on its own, the same
    /// reason <see cref="ForwardPlan.Decide"/> is a pure function rather than a method on the class
    /// that owns a real thread and a real socket.
    /// </summary>
    /// <param name="referencePts">This packet's position on the reference time base.</param>
    /// <param name="origin">The first packet's position on the same scale - this pull's time zero.</param>
    /// <param name="secondsPerTick">Converts a tick difference on the reference time base to seconds.</param>
    /// <param name="elapsed">Wall-clock time since this pull's clock started.</param>
    public static PaceDecision Pace(long referencePts, long origin, double secondsPerTick, TimeSpan elapsed)
    {
        var due = TimeSpan.FromSeconds((referencePts - origin) * secondsPerTick);
        var wait = due - elapsed;

        return wait > MaxPaceGap || wait < -MaxPaceGap
            ? new PaceDecision(Wait: null, Reanchor: true)
            : new PaceDecision(Wait: wait > TimeSpan.Zero ? wait : null, Reanchor: false);
    }
}
