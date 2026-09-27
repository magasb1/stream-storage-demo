using FFmpeg.AutoGen.Abstractions;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// What the streams inside one transport are: their codecs, their parameters and their clocks.
///
/// It is held apart from the demultiplexer that produced it because it outlives one. A stream
/// whose feed stops keeps its hub, its buffer and any recording alive through the grace period,
/// and everything attached to it still needs to know what it is carrying. It is also what decides
/// whether a returning encoder can be appended to or has to start a new document: a file whose
/// codec configuration changes halfway is not something anything will reliably play.
///
/// The parameters are libav's own, copied and owned here, because that is the form both the
/// muxers and the decoder want them in.
/// </summary>
public sealed unsafe class StreamLayout : IDisposable
{
    /// <summary>
    /// What a stream is assumed to send when it says nothing about itself. A probe that ended
    /// before libav could average a frame rate leaves zero there, and a queue sized from zero is
    /// no queue at all, so the assumption errs high: a queue slightly deeper than it needs to be
    /// costs a few hundred kilobytes, and one too shallow makes a healthy viewer skip.
    /// </summary>
    private const double AssumedPacketsPerSecond = 60;

    /// <summary>
    /// The floor under any queue derived from seconds, in packets. A transport interleaves its
    /// streams rather than aligning them, so even a second of media arrives in bursts, and a queue
    /// small enough to be filled by one of those would report a viewer falling behind when nothing
    /// is wrong with it.
    /// </summary>
    private const int MinimumQueue = 64;

    private readonly IntPtr[] _parameters;
    private readonly AVRational[] _timeBases;

    private StreamLayout(
        IntPtr[] parameters,
        AVRational[] timeBases,
        double packetsPerSecond,
        int videoIndex,
        int klvIndex)
    {
        _parameters = parameters;
        _timeBases = timeBases;
        PacketsPerSecond = packetsPerSecond;
        VideoIndex = videoIndex;
        KlvIndex = klvIndex;
    }

    /// <summary>The stream a keyframe means something on, or -1 when there is no picture.</summary>
    public int VideoIndex { get; }

    /// <summary>
    /// The MISB metadata stream, or -1 when there is none. In MPEG-TS it is a data stream whose
    /// registration descriptor says KLVA, which libav reports as the SMPTE KLV codec; a data
    /// stream carrying anything else is not metadata this service understands.
    /// </summary>
    public int KlvIndex { get; }

    public int Count => _parameters.Length;

    /// <summary>
    /// Roughly how many demultiplexed packets a second this transport carries, across every stream
    /// in it.
    ///
    /// It is what turns a queue depth in seconds into a queue depth in packets, which is the only
    /// unit a channel has. A packet here is one <c>AVPacket</c> - a video frame, an audio frame, a
    /// metadata item - so the figure is the sum of the streams' own rates rather than the video
    /// frame rate: audio at forty-seven frames a second is more packets than the picture it
    /// accompanies, and a queue sized from the picture alone would be less than half as deep in
    /// seconds as it was asked to be.
    ///
    /// Roughly, and deliberately. It is read once from what the sender presented and never
    /// corrected, because what it is used for is sizing a queue at the moment a consumer attaches;
    /// a variable frame rate or a sender that lied costs a queue somewhat deeper or shallower in
    /// seconds than asked for, which is the difference between four seconds and five rather than
    /// between four and eighty.
    /// </summary>
    public double PacketsPerSecond { get; }

    /// <summary>
    /// The clock everything is measured against. The video stream when there is one, so a segment
    /// boundary and a rollback position are on the same scale as the pictures they refer to.
    /// </summary>
    public AVRational ReferenceTimeBase => _timeBases[VideoIndex >= 0 ? VideoIndex : 0];

    public double SecondsPerTick => ffmpeg.av_q2d(ReferenceTimeBase);

    public AVRational TimeBase(int index) => _timeBases[index];

    public AVCodecParameters* Parameters(int index) => (AVCodecParameters*)_parameters[index];

    public static StreamLayout From(AVFormatContext* format)
    {
        var count = (int)format->nb_streams;
        var parameters = new IntPtr[count];
        var timeBases = new AVRational[count];
        var packetsPerSecond = 0d;
        var videoIndex = -1;
        var klvIndex = -1;

        for (var index = 0; index < count; index++)
        {
            var stream = format->streams[index];

            packetsPerSecond += RateOf(stream);

            var copy = ffmpeg.avcodec_parameters_alloc();

            ffmpeg.avcodec_parameters_copy(copy, stream->codecpar);

            // Meaningless once these parameters are used to build a different container, and left
            // set it confuses some muxers.
            copy->codec_tag = 0;

            parameters[index] = (IntPtr)copy;
            timeBases[index] = stream->time_base;

            if (videoIndex < 0 && stream->codecpar->codec_type == AVMediaType.AVMEDIA_TYPE_VIDEO)
            {
                videoIndex = index;
            }

            if (klvIndex < 0
                && stream->codecpar->codec_type == AVMediaType.AVMEDIA_TYPE_DATA
                && stream->codecpar->codec_id == AVCodecID.AV_CODEC_ID_SMPTE_KLV)
            {
                klvIndex = index;
            }
        }

        return new StreamLayout(parameters, timeBases, packetsPerSecond, videoIndex, klvIndex);
    }

    /// <summary>
    /// How many packets a subscriber has to hold to keep this many seconds of this stream, never
    /// fewer than a burst's worth and never more than <paramref name="ceiling"/>.
    ///
    /// The ceiling is the reason a queue can be asked for in seconds at all. Seconds are what the
    /// intent is written in - a viewer must never accumulate more delay than it can be asked to
    /// tolerate - but the memory the queue costs is packets, and a sender is free to claim a
    /// thousand frames a second. Whichever of the two binds first is the answer.
    /// </summary>
    public int QueueDepth(double seconds, int ceiling)
    {
        var packets = Math.Ceiling(Math.Max(seconds, 0) * PacketsPerSecond);

        // Clamped this way round rather than with Math.Clamp, which throws when a deployment has
        // set a ceiling below the floor rather than quietly giving it the smaller of the two.
        return Math.Min(ceiling, Math.Max(MinimumQueue, (int)Math.Min(packets, int.MaxValue)));
    }

    /// <summary>
    /// What one stream sends a second, in packets, from what the sender presented about it.
    ///
    /// Each kind of stream says it differently. A picture says it as a frame rate, and the average
    /// rather than the base rate because a telecined or variable source sends the average. Audio
    /// says it as a sample rate over the samples in one frame, which is how forty-eight kilohertz
    /// AAC comes out at forty-seven packets a second rather than forty-eight thousand. Anything
    /// else - KLV metadata, a subtitle track - has no such figure, and in MISB transports arrives
    /// about once a frame, so it is assumed to.
    /// </summary>
    private static double RateOf(AVStream* stream)
    {
        var parameters = stream->codecpar;

        if (parameters->codec_type == AVMediaType.AVMEDIA_TYPE_AUDIO
            && parameters->sample_rate > 0
            && parameters->frame_size > 0)
        {
            return (double)parameters->sample_rate / parameters->frame_size;
        }

        var average = ffmpeg.av_q2d(stream->avg_frame_rate);

        if (average > 0)
        {
            return average;
        }

        var declared = ffmpeg.av_q2d(stream->r_frame_rate);

        return declared > 0 ? declared : AssumedPacketsPerSecond;
    }

    /// <summary>
    /// Recreates these streams on a container being written, so a consumer's muxer carries the
    /// same tracks the sender presented.
    /// </summary>
    /// <returns>Input stream index to output stream index; -1 for a stream this container refused.</returns>
    public int[] ApplyTo(AVFormatContext* output)
    {
        var mapping = new int[Count];
        var next = 0;

        for (var index = 0; index < Count; index++)
        {
            var stream = ffmpeg.avformat_new_stream(output, null);

            if (stream is null || ffmpeg.avcodec_parameters_copy(stream->codecpar, Parameters(index)) < 0)
            {
                mapping[index] = -1;
                continue;
            }

            stream->time_base = _timeBases[index];
            mapping[index] = next++;
        }

        if (next == 0)
        {
            throw new InvalidOperationException("The stream carries nothing this container can hold.");
        }

        return mapping;
    }

    /// <summary>
    /// Whether a returning feed is the same shape as the one that went away.
    ///
    /// Compared field by field rather than by identity, because the encoder that comes back opened
    /// a new connection and libav built a fresh context for it. What matters is whether the bytes
    /// still fit the file already being written.
    /// </summary>
    public bool Matches(StreamLayout other)
    {
        if (Count != other.Count)
        {
            return false;
        }

        for (var index = 0; index < Count; index++)
        {
            var mine = Parameters(index);
            var theirs = other.Parameters(index);

            if (mine->codec_id != theirs->codec_id
                || mine->codec_type != theirs->codec_type
                || mine->width != theirs->width
                || mine->height != theirs->height
                || mine->format != theirs->format
                || mine->sample_rate != theirs->sample_rate
                || mine->ch_layout.nb_channels != theirs->ch_layout.nb_channels
                || mine->extradata_size != theirs->extradata_size)
            {
                return false;
            }

            // Extradata carries the decoder configuration itself: the sequence header, the SPS and
            // PPS. Two streams agreeing on codec and size but not on this are not interchangeable.
            if (mine->extradata_size > 0
                && new ReadOnlySpan<byte>(mine->extradata, mine->extradata_size)
                    .SequenceEqual(new ReadOnlySpan<byte>(theirs->extradata, theirs->extradata_size)) is false)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>A short description for a log line or a client's metadata panel.</summary>
    public string Describe()
    {
        var parts = new List<string>(Count);

        for (var index = 0; index < Count; index++)
        {
            var parameters = Parameters(index);
            var codec = ffmpeg.avcodec_get_name(parameters->codec_id);

            parts.Add(parameters->codec_type == AVMediaType.AVMEDIA_TYPE_VIDEO
                ? $"{codec} {parameters->width}x{parameters->height}"
                : codec);
        }

        return string.Join(", ", parts);
    }

    public void Dispose()
    {
        for (var index = 0; index < _parameters.Length; index++)
        {
            if (_parameters[index] == IntPtr.Zero)
            {
                continue;
            }

            var parameters = (AVCodecParameters*)_parameters[index];
            ffmpeg.avcodec_parameters_free(&parameters);

            _parameters[index] = IntPtr.Zero;
        }
    }
}
