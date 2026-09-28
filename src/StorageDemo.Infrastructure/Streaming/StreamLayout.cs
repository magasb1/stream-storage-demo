using FFmpeg.AutoGen.Abstractions;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// What a synthetic track carries, which is what decides whether <see cref="StreamLayout.KlvIndex"/>
/// may point at it.
///
/// The role is here from the first track rather than added when a second one appears, because the
/// second one is already decided: VMTI rides a standalone track of its own (issue #30, and the
/// entry in TODO beginning "Decided: VMTI rides standalone"), and both
/// tracks are SMPTE KLV data streams. Without the role, <c>KlvIndex</c> would be "the first data
/// track we appended" and would silently start meaning the detections once that lands, which
/// <see cref="KlvExtractor"/> would then try to decode as ST 0601.
/// </summary>
public enum SyntheticTrackRole
{
    /// <summary>
    /// ST 0601 platform metadata: where the sensor is and where it looks. The only role
    /// <see cref="StreamLayout.KlvIndex"/> ever points at.
    /// </summary>
    PlatformMetadata,
}

/// <summary>
/// A track this replica produces rather than receives, appended to what a sender presented.
///
/// Both the role's carriages are SMPTE KLV data streams, so there is no codec to choose, and the
/// time base is not chosen here either: <see cref="StreamLayout"/>'s synthetic-track overload of
/// <c>From</c> gives every synthetic track the reference time base, which is what makes
/// <c>PacketMuxer</c>'s rescaling of a locally produced packet the identity rather than an
/// arithmetic step that has to be got right in two places.
/// </summary>
/// <param name="PacketsPerSecond">
/// How often this track will actually be published. The single most consequential figure in a
/// synthetic track: an undeclared one falls through to <see cref="StreamLayout"/>'s assumption of
/// sixty, which takes a 25 fps stream from 25 packets a second to 85 and collapses a viewer's
/// maximum rollback from 76 seconds to 19.5 - measured, and pinned by
/// <c>StreamLayoutTests.A_declared_synthetic_rate_is_what_a_viewers_rollback_is_sized_from</c>.
/// Declared truthfully
/// at one hertz it reads 26 and 72.9 seconds.
/// </param>
public sealed record SyntheticTrack(SyntheticTrackRole Role, double PacketsPerSecond)
{
    /// <summary>
    /// Thrown rather than clamped. A sender's impossible rate is a claim to be disbelieved; this
    /// one is this service's own arithmetic, and a wrong figure here silently resizes every
    /// viewer's queue on the stream.
    /// </summary>
    public double PacketsPerSecond { get; } = double.IsFinite(PacketsPerSecond) && PacketsPerSecond > 0
        ? PacketsPerSecond
        : throw new ArgumentOutOfRangeException(nameof(PacketsPerSecond), PacketsPerSecond, "A synthetic track has to declare a real rate.");
}

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
    /// What a stream is assumed to send when it says nothing usable about itself - a probe that
    /// ended before libav could average a frame rate leaves zero there, and a queue sized from zero
    /// is no queue at all.
    ///
    /// Sixty is high for a picture and about right for everything else a transport carries. It errs
    /// deep for anything slower, which costs a queue a few hundred kilobytes it did not need, and
    /// shallow for anything faster, which costs that viewer a skip; of the two, a skip is the one
    /// worth avoiding. A KLV stream beside 25 fps video measured nearer eighty-five packets a second
    /// in total than the fifty a packet-per-frame assumption predicts, so this is also the closer
    /// guess for the metadata case it is most often used for.
    /// </summary>
    private const double AssumedPacketsPerSecond = 60;

    /// <summary>
    /// The most one stream is believed to send. Above this the figure is a sender's claim rather
    /// than a measurement: a thousand packets a second is four times the fastest camera anything
    /// here has seen and twenty times a live audio track, and the claims that reach it are
    /// arithmetic rather than video - an <c>avg_frame_rate</c> of 10000/1, or an audio frame size of
    /// one sample. Both would otherwise pin every viewer of that stream to the packet ceiling, and so
    /// to a fraction of a second of queue, which is a sender opting its own viewers out of ever
    /// riding out a hiccup.
    /// </summary>
    private const double MaxPacketsPerSecond = 1_000;

    /// <summary>
    /// The floor under a queue, in seconds, because a floor in packets is the bug this arithmetic
    /// exists to fix in miniature: sixty-four packets is a reasonable queue at 25 fps, thirteen
    /// seconds of one at five, and over a minute at one frame a second. Half a second is about as
    /// short as a queue can be and still ride out one scheduling hiccup.
    /// </summary>
    private const double MinimumSeconds = 0.5;

    /// <summary>
    /// The floor under a queue in packets, which is mechanical rather than a policy about seconds: a
    /// transport interleaves its streams rather than aligning them, so a queue has to hold at least
    /// a packet from each of them and one being read, however slow the stream is.
    /// </summary>
    private const int MinimumPackets = 4;

    private readonly IntPtr[] _parameters;
    private readonly AVRational[] _timeBases;

    /// <summary>
    /// What each track is, for the tracks this service produces; null at every index that came off
    /// the wire. Indexed the same as <see cref="_parameters"/> so a role is one lookup rather than
    /// a search.
    /// </summary>
    private readonly SyntheticTrackRole?[] _roles;

    private StreamLayout(
        IntPtr[] parameters,
        AVRational[] timeBases,
        SyntheticTrackRole?[] roles,
        double packetsPerSecond,
        int videoIndex,
        int klvIndex)
    {
        _parameters = parameters;
        _timeBases = timeBases;
        _roles = roles;
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
    ///
    /// It points at a synthetic track only when the sender declared no KLV of its own - see the
    /// synthetic-track overload of <see cref="From(AVFormatContext*)"/> - so a camera that starts
    /// reporting its own telemetry keeps its own index.
    /// </summary>
    public int KlvIndex { get; }

    /// <summary>
    /// Whether the metadata on <see cref="KlvIndex"/> is this service's own synthesis from
    /// configuration rather than the sender's telemetry. False when there is no KLV at all.
    /// </summary>
    public bool KlvIsSynthetic => KlvIndex >= 0 && _roles[KlvIndex] is not null;

    /// <summary>Where the track carrying this role sits, or -1 when this layout has none.</summary>
    public int SyntheticIndexOf(SyntheticTrackRole role)
    {
        for (var index = 0; index < _roles.Length; index++)
        {
            if (_roles[index] == role)
            {
                return index;
            }
        }

        return -1;
    }

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
    /// between four and eighty. A declaration that cannot be true - an infinity, or ten thousand
    /// frames a second - is replaced rather than believed, because this figure is sender-controlled
    /// and a viewer's queue is sized from it.
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

    public static StreamLayout From(AVFormatContext* format) => From(format, []);

    /// <summary>
    /// The same, with tracks this replica produces appended to what the sender presented.
    ///
    /// Appended last, never inserted, so every index a sender declared keeps its number and a
    /// stream that carries real KLV keeps its own <see cref="KlvIndex"/>. The synthetic tracks are
    /// dropped entirely when the sender declared KLV of its own: synthesis fills a gap rather than
    /// competing with telemetry, and a camera that starts reporting for itself wins.
    ///
    /// This runs before <c>StreamHub.Adopt</c> and never after. Swapping an augmented layout into
    /// a hub that is already running would break every consumer attached to the old one -
    /// <c>Serve</c>, the recorder, every forward and the snapshot muxer subscribe to every index
    /// and none of them re-reads the layout - and would make <see cref="Matches"/> compare an
    /// augmented layout against a bare one on the next reconnect, which closes any recording in
    /// progress. Augmenting here means <c>Matches</c> compares like with like.
    /// </summary>
    public static StreamLayout From(AVFormatContext* format, IReadOnlyList<SyntheticTrack> synthetic)
    {
        ArgumentNullException.ThrowIfNull(synthetic);

        var arrived = (int)format->nb_streams;
        var packetsPerSecond = 0d;
        var videoIndex = -1;
        var klvIndex = -1;

        for (var index = 0; index < arrived; index++)
        {
            var stream = format->streams[index];

            if (klvIndex < 0
                && stream->codecpar->codec_type == AVMediaType.AVMEDIA_TYPE_DATA
                && stream->codecpar->codec_id == AVCodecID.AV_CODEC_ID_SMPTE_KLV)
            {
                klvIndex = index;
            }
        }

        // Read before anything is allocated, so the "the sender already sends KLV" case costs
        // nothing to decide and leaves nothing to free.
        IReadOnlyList<SyntheticTrack> appended = klvIndex >= 0 ? [] : synthetic;
        var count = arrived + appended.Count;
        var parameters = new IntPtr[count];
        var timeBases = new AVRational[count];
        var roles = new SyntheticTrackRole?[count];

        for (var index = 0; index < arrived; index++)
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
        }

        // The clock the appended tracks are put on. The same expression ReferenceTimeBase uses,
        // read here because the layout does not exist yet; 90 kHz is MPEG-TS's own clock and is
        // what an arriving transport with no picture at all would have to be given anyway.
        var reference = arrived > 0
            ? format->streams[videoIndex >= 0 ? videoIndex : 0]->time_base
            : new AVRational { num = 1, den = 90_000 };

        for (var offset = 0; offset < appended.Count; offset++)
        {
            var index = arrived + offset;
            var track = appended[offset];
            var copy = ffmpeg.avcodec_parameters_alloc();

            // A data stream libav's MPEG-TS muxer writes with the KLVA registration descriptor a
            // STANAG 4609 consumer keys off, on stream type 0x06 private data. profile is left at
            // what avcodec_parameters_alloc set it to, deliberately: AV_PROFILE_KLVA_SYNC selects
            // stream type 0x15 instead, and libav's demuxer then strips the five-byte ST 1402
            // metadata AU cell header that its own muxer never writes, so its own round trip loses
            // the first five bytes of every packet. Measured; see Misb.WriteTransportStream, which
            // writes a KLV track the same way for the same reason.
            copy->codec_type = AVMediaType.AVMEDIA_TYPE_DATA;
            copy->codec_id = AVCodecID.AV_CODEC_ID_SMPTE_KLV;

            parameters[index] = (IntPtr)copy;
            timeBases[index] = reference;
            roles[index] = track.Role;

            // Declared rather than left to the assumption. This is the line the rollback figure
            // hangs on; SyntheticTrack.PacketsPerSecond says what it costs to get wrong.
            packetsPerSecond += track.PacketsPerSecond;

            if (klvIndex < 0 && track.Role == SyntheticTrackRole.PlatformMetadata)
            {
                klvIndex = index;
            }
        }

        return new StreamLayout(parameters, timeBases, roles, packetsPerSecond, videoIndex, klvIndex);
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
        // The floor is in seconds, so that a slow stream gets a short queue rather than a deep one:
        // a floor in packets is how a queue meant to hold a moment came to hold eighty seconds.
        var packets = Math.Ceiling(Math.Max(seconds, MinimumSeconds) * PacketsPerSecond);

        // Clamped this way round rather than with Math.Clamp, which throws when a deployment has
        // set a ceiling below the floor rather than quietly giving it the smaller of the two. The
        // rates are finite by construction, so the cast cannot see a NaN.
        return Math.Min(ceiling, Math.Max(MinimumPackets, (int)Math.Min(packets, int.MaxValue)));
    }

    /// <summary>
    /// What one stream sends a second, in packets, from what the sender presented about it.
    ///
    /// Each kind of stream says it differently. A picture says it as a frame rate, and the average
    /// rather than the base rate because a telecined or variable source sends the average. Audio
    /// says it as a sample rate over the samples in one frame, which is how forty-eight kilohertz
    /// AAC comes out at forty-seven packets a second rather than forty-eight thousand. Anything
    /// else - KLV metadata, a subtitle track - declares no rate at all and is taken to send
    /// <see cref="AssumedPacketsPerSecond"/>, which for the metadata case is the closer of the two
    /// available guesses.
    ///
    /// Every answer goes through <see cref="Believable"/>, because all of it is the sender's
    /// arithmetic: a rational with a zero denominator is an infinity, and a frame rate of ten
    /// thousand is a claim.
    /// </summary>
    private static double RateOf(AVStream* stream)
    {
        var parameters = stream->codecpar;

        if (parameters->codec_type == AVMediaType.AVMEDIA_TYPE_AUDIO
            && parameters->sample_rate > 0
            && parameters->frame_size > 0)
        {
            return Believable((double)parameters->sample_rate / parameters->frame_size);
        }

        var average = ffmpeg.av_q2d(stream->avg_frame_rate);

        if (average > 0)
        {
            return Believable(average);
        }

        var declared = ffmpeg.av_q2d(stream->r_frame_rate);

        return declared > 0 ? Believable(declared) : AssumedPacketsPerSecond;
    }

    /// <summary>
    /// One stream's declared rate, or the assumption where what it declared cannot be true.
    ///
    /// Discarded rather than clamped to the maximum, which is the difference that matters: a claim of
    /// ten thousand frames a second clamped to a thousand is still four times anything real, and a
    /// sender could use it to pin its own viewers to the packet ceiling and so to a fraction of a
    /// second of queue. Treated as a stream that declared nothing, it gets the same queue a stream
    /// libav could not measure gets. Nothing downstream is allowed to see an infinity or a NaN
    /// either, because this figure is multiplied by a number of seconds and cast to a queue depth.
    /// </summary>
    private static double Believable(double rate)
        => double.IsFinite(rate) && rate is > 0 and <= MaxPacketsPerSecond ? rate : AssumedPacketsPerSecond;

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

            // Compared first because the codec parameters cannot tell these apart: a synthetic
            // ST 0601 track and a KLV track the sender declared are both SMPTE KLV data streams
            // with no width, no format and no extradata. A camera that starts reporting its own
            // telemetry would otherwise match the layout that was synthesising for it, and the
            // injector would carry on publishing onto the index the sender is now using.
            if (_roles[index] != other._roles[index])
            {
                return false;
            }

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
