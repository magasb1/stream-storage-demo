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
    /// Held to the same ceiling a sender's claim is, and thrown rather than replaced.
    ///
    /// The ceiling is the part that is not obvious. Left unchecked, a declared 999,999 adds
    /// straight into <see cref="StreamLayout.PacketsPerSecond"/> - past a million for a 25 fps
    /// stream - and pins <see cref="StreamLayout.QueueDepth"/> to the packet ceiling, which is a
    /// fraction of a second of queue for every viewer: precisely the failure
    /// <see cref="StreamLayout"/>'s own <c>Believable</c> exists to prevent, arriving by the one
    /// door that did not have the check on it. Unreachable from configuration today, since the
    /// only caller passes a constant, and that is a reason to make it impossible rather than a
    /// reason to leave it.
    ///
    /// Thrown rather than replaced with the assumption, which is where this parts company with
    /// <c>Believable</c>. A sender's impossible claim has a sensible fallback - treat it as a
    /// stream that declared nothing - because the sender is not ours to fix. This rate is this
    /// service's own arithmetic, and there is no figure that would be right for a track whose
    /// publisher we also wrote; a wrong one silently resizes every viewer's queue on the stream.
    /// </summary>
    public double PacketsPerSecond { get; } =
        double.IsFinite(PacketsPerSecond) && PacketsPerSecond is > 0 and <= StreamLayout.MaxPacketsPerSecond
            ? PacketsPerSecond
            : throw new ArgumentOutOfRangeException(
                nameof(PacketsPerSecond),
                PacketsPerSecond,
                $"A synthetic track declares a real rate, above 0 and at most {StreamLayout.MaxPacketsPerSecond} a second.");
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
    internal const double MaxPacketsPerSecond = 1_000;

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
        double syntheticPacketsPerSecond,
        int videoIndex,
        int klvIndex)
    {
        _parameters = parameters;
        _timeBases = timeBases;
        _roles = roles;
        PacketsPerSecond = packetsPerSecond;
        SyntheticPacketsPerSecond = syntheticPacketsPerSecond;
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
    ///
    /// Still read once and never corrected after issue #26, which is the point of
    /// <see cref="EffectivePacketsPerSecond"/> being a function rather than a setter: a
    /// declaration that turns out to be far too low is answered by giving the viewer path a
    /// better figure to ask for, not by editing this one. This layout outlives the connection and
    /// is shared with the recorder, the forwarder and the snapshot muxer, none of which is on the
    /// path the misdeclaration hurts.
    /// </summary>
    public double PacketsPerSecond { get; }

    /// <summary>
    /// The part of <see cref="PacketsPerSecond"/> that this service produces rather than receives:
    /// the sum of the appended <see cref="SyntheticTrack"/> rates, and zero for a layout with none.
    ///
    /// It exists because an observed rate and a declared one are not otherwise commensurable.
    /// <see cref="StreamHub.PublishAtLiveEdge"/> deliberately leaves <c>StreamHub.Packets</c> alone
    /// - that split is what stops a packet this replica produced moving <c>LastPacketAt</c> and
    /// keeping a dead camera alive for ever - so any rate measured from those counters is arrivals
    /// only and systematically omits every synthetic track. Adding this back is what makes the two
    /// halves of <c>LiveStreamCoordinator</c>'s effective rate comparable, and it is what keeps a
    /// 25 fps stream with a 1 Hz synthetic track reading 26 rather than 25: a rate this service
    /// itself declared and knows to be true must not be talked down by measuring around it.
    /// </summary>
    public double SyntheticPacketsPerSecond { get; }

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
        var syntheticPacketsPerSecond = 0d;
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

            // Kept as well as added, rather than recovered later by walking the roles: a role says
            // which track is synthetic, not what rate it was declared at, and the track records
            // here are gone once this loop ends. See SyntheticPacketsPerSecond for what reads it.
            syntheticPacketsPerSecond += track.PacketsPerSecond;

            if (klvIndex < 0 && track.Role == SyntheticTrackRole.PlatformMetadata)
            {
                klvIndex = index;
            }
        }

        return new StreamLayout(
            parameters,
            timeBases,
            roles,
            packetsPerSecond,
            syntheticPacketsPerSecond,
            videoIndex,
            klvIndex);
    }

    /// <summary>
    /// The rate a viewer's queue should actually be sized from: this layout's declaration, or what
    /// the stream is observed to be sending where that is more.
    ///
    /// Issue #26. <see cref="Believable"/> can reject a declared rate that cannot be true - an
    /// infinity, ten thousand frames a second - but it cannot reject one that is merely too low,
    /// because from outside a 1 fps time-lapse and a 50 fps camera misdeclaring itself are the
    /// same thing. A sender declaring 1 while sending 50 gets a viewer queue of four packets
    /// against the two hundred its traffic needs, so every viewer of it skips to live almost
    /// continuously while the stream reports itself healthy. An H.264 encoder's VUI timing is
    /// wrong in the wild by accident, so this needs no attacker.
    ///
    /// <see cref="SyntheticPacketsPerSecond"/> is added to the observation and not to the
    /// declaration, and getting that backwards is the trap in this one expression.
    /// <see cref="StreamHub.PublishAtLiveEdge"/> does not increment <c>StreamHub.Packets</c> - the
    /// split that stops a locally produced packet moving <c>LastPacketAt</c> and keeping a dead
    /// camera alive - so an observation taken from those counters is arrivals only, while
    /// <see cref="PacketsPerSecond"/> already counts every track. Without the addition a 25 fps
    /// stream carrying a 1 Hz synthetic track would observe 25 against a declared 26 and have
    /// every viewer's queue sized 3.8% short of a figure this service computed itself and knows to
    /// be right - invisibly, on exactly the streams whose rate is least in doubt. With it the two
    /// are commensurable, the larger is the declared 26, and nothing moves.
    ///
    /// The larger of the two rather than the observation alone, for two reasons. A stream that has
    /// just started, is interrupted, or has not yet completed a sample window reports no arrivals
    /// at all, and falls back to the declaration without any caller having to ask which of the
    /// three it is in. And a sender that <i>over</i>states keeps exactly today's behaviour - a
    /// queue shallower in seconds than asked for, bounded by the packet ceiling - rather than
    /// gaining a new one out of a change aimed at the opposite case.
    ///
    /// Not held to <see cref="MaxPacketsPerSecond"/>, which a declaration is, and at the shipped
    /// options that is not a judgement call but arithmetic: the cap is unreachable. A queue is
    /// clamped to <see cref="LiveOptions.ViewerQueuePackets"/> = 2000, and
    /// <see cref="LiveOptions.ViewerQueueSeconds"/> = 4 reaches it at any rate from 500 a second
    /// up, which is below the cap of 1000 - so from 500 upward the ceiling already binds and the
    /// queue is 2000 packets whether this reads 500 or a million. On the other consumer the rate
    /// is a divisor and the room is already zero at 500. Capping would change no production
    /// number, and the largest queue a flood can provoke is exactly the one an honest viewer
    /// asking for a full rollback is already given.
    ///
    /// That rests on an inequality worth stating, because it is a deployment's to break:
    /// <c>ViewerQueueSeconds * MaxPacketsPerSecond &gt;= ViewerQueuePackets</c>, which is
    /// 4000 >= 2000 today. ViewerQueueSeconds ranges down to 0.25, and below 2 the cap would start
    /// to bind before the packet ceiling does - at which point an uncapped observation would be
    /// doing something the cap was written to prevent, and this decision would need taking again.
    ///
    /// What <see cref="LiveOptions.ViewerQueueSeconds"/> promises changes shape here, and it is
    /// worth saying plainly: before this it was a number of seconds of the rate the sender
    /// declared, and it is now a number of seconds of the rate measured at the moment the viewer
    /// attached. A depth is fixed at <see cref="StreamHub.Subscribe"/> and never revised, so a
    /// stream whose rate genuinely moves during a long session is held to whatever it was doing
    /// when that viewer arrived. <c>LiveStreamEntry.ObservedPacketsPerSecond</c> is damped against
    /// a burst for exactly this reason.
    ///
    /// Nothing here is stored. The declaration is read once from what the sender presented and
    /// never corrected - see <see cref="PacketsPerSecond"/> - because this layout outlives the
    /// connection and is shared with the recorder, the forwarder and the snapshot muxer, none of
    /// which wants a rate that moves underneath it.
    /// </summary>
    /// <param name="observedArrivals">
    /// Packets a second arriving from the sender, excluding synthetic tracks, or zero where the
    /// stream has not been measured. <c>LiveStreamEntry.ObservedPacketsPerSecond</c> is the one
    /// producer of it.
    /// </param>
    public double EffectivePacketsPerSecond(double observedArrivals)
    {
        // Guarded rather than trusted: this is multiplied by a number of seconds and cast to a
        // queue depth, and the arithmetic upstream is a difference of packet totals over a
        // measured interval. Believable is not reused, because its fallback is the assumption of
        // sixty - right for a rate a sender declared, wrong here, where a figure that cannot be
        // read is a reason to keep the declaration and nothing more.
        var observed = double.IsFinite(observedArrivals) && observedArrivals > 0
            ? observedArrivals + SyntheticPacketsPerSecond
            : 0;

        return Math.Max(PacketsPerSecond, observed);
    }

    /// <summary>
    /// How many packets a subscriber has to hold to keep this many seconds of a stream running at
    /// <paramref name="packetsPerSecond"/>, never fewer than a burst's worth and never more than
    /// <paramref name="ceiling"/>.
    ///
    /// The ceiling is the reason a queue can be asked for in seconds at all. Seconds are what the
    /// intent is written in - a viewer must never accumulate more delay than it can be asked to
    /// tolerate - but the memory the queue costs is packets, and a sender is free to claim a
    /// thousand frames a second. Whichever of the two binds first is the answer.
    ///
    /// The rate is passed in rather than read from <see cref="PacketsPerSecond"/>, which is issue
    /// #26: <c>Believable</c> can reject a declared rate that is too high but not one that is too
    /// low, because a 1 fps time-lapse and a 50 fps camera misdeclaring itself present the same
    /// thing. The one production caller is <c>LiveStreamCoordinator.Serve</c>, which passes the
    /// larger of the declaration and what the traffic is actually doing; taking the rate as an
    /// argument is what puts that rule in one place there rather than hiding a second rate in
    /// here. Everything else attaches with a flat packet count and never reaches this at all.
    /// </summary>
    public int QueueDepth(double packetsPerSecond, double seconds, int ceiling)
    {
        // The floor is in seconds, so that a slow stream gets a short queue rather than a deep one:
        // a floor in packets is how a queue meant to hold a moment came to hold eighty seconds.
        var packets = Math.Ceiling(Math.Max(seconds, MinimumSeconds) * packetsPerSecond);

        // Clamped this way round rather than with Math.Clamp, which throws when a deployment has
        // set a ceiling below the floor rather than quietly giving it the smaller of the two. The
        // rates are finite by construction - a declaration through Believable, an observation
        // through EffectivePacketsPerSecond, which is the only thing that produces one - so the
        // cast cannot see a NaN.
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
