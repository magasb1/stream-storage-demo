using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// Writes packets from a hub into a container, on a destination of the caller's choosing.
///
/// Every consumer that writes bytes owns one of these. A recording started at ten past and a
/// viewer who joined at twelve past have different timelines and different first packets, so a
/// shared muxer would force both to inherit whichever attached first. Each therefore gets its own
/// timestamp base, established here from its own first packet.
///
/// The destination is an ordinary .NET stream reached through a custom libav IO context, which is
/// what lets one class serve both a recording writing to a file and a viewer writing to an HTTP
/// response.
/// </summary>
public sealed unsafe class PacketMuxer : IDisposable
{
    /// <summary>libav's buffer between the muxer and the write callback.</summary>
    private const int IoBufferSize = 64 * 1024;

    /// <summary>
    /// A resumed feed whose encoder restarted counts from zero again. Anything landing this far
    /// behind what has already been written is that, rather than ordinary packet reordering.
    /// </summary>
    private static readonly TimeSpan ClockReset = TimeSpan.FromSeconds(1);

    private readonly Stream _destination;
    private readonly StreamLayout _layout;
    private readonly AVRational _reference;
    private readonly int[] _mapping;

    /// <summary>Held so the garbage collector cannot free the thunk libav writes through.</summary>
    private readonly avio_alloc_context_write_packet _write;

    private AVFormatContext* _format;
    private AVIOContext* _io;
    private AVPacket* _packet;

    private readonly long _origin;

    /// <summary>The last timestamp written on each output stream, so none of them goes backwards.</summary>
    private readonly long[] _written;

    private bool _started;
    private bool _headerWritten;
    private long _shift;
    private long _lastOut;

    /// <param name="continueFromSeconds">
    /// Where this muxer's timeline starts. Zero for a fresh consumer. A viewer whose stream has
    /// moved to another replica hands over where the last one left off, so the picture carries on
    /// rather than jumping backwards to zero in the middle of the same connection.
    /// </param>
    public PacketMuxer(
        Stream destination,
        StreamLayout layout,
        string containerFormat = "mpegts",
        double continueFromSeconds = 0)
    {
        _destination = destination;
        _layout = layout;
        _reference = layout.ReferenceTimeBase;
        _write = OnWrite;
        // One tick past where the last one ended, not exactly on it: two packets sharing a
        // timestamp across the join is what a strict muxer refuses and a player stutters on.
        _origin = continueFromSeconds > 0
            ? (long)(continueFromSeconds / ffmpeg.av_q2d(layout.ReferenceTimeBase)) + 1
            : 0;

        _lastOut = _origin;

        AVFormatContext* format = null;

        if (ffmpeg.avformat_alloc_output_context2(&format, null, containerFormat, null) < 0 || format is null)
        {
            throw new InvalidOperationException($"libav cannot write a '{containerFormat}' container.");
        }

        _format = format;

        try
        {
            _mapping = layout.ApplyTo(_format);
            _written = new long[_format->nb_streams];
            Array.Fill(_written, ffmpeg.AV_NOPTS_VALUE);

            var buffer = (byte*)ffmpeg.av_malloc(IoBufferSize);

            _io = ffmpeg.avio_alloc_context(
                buffer,
                IoBufferSize,
                write_flag: 1,
                opaque: null,
                read_packet: null,
                write_packet: _write,
                seek: null);

            if (_io is null)
            {
                throw new InvalidOperationException("Could not allocate an IO context for the muxer.");
            }

            // Not seekable. An HTTP response cannot be rewound, and a live container must not
            // need to be: MPEG-TS is chosen precisely because a receiver can join mid-stream.
            _io->seekable = 0;

            _format->pb = _io;
            _format->flags |= ffmpeg.AVFMT_FLAG_CUSTOM_IO;

            // Write through rather than buffering. A recording that only reaches disk when the
            // muxer closes cannot be uploaded until then either, and a viewer would see nothing
            // for as long as the buffer took to fill.
            _format->flush_packets = 1;

            _packet = ffmpeg.av_packet_alloc();

            if (_packet is null)
            {
                throw new InvalidOperationException("Could not allocate a packet.");
            }
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>Bytes handed to the destination, which is the size of what is being written.</summary>
    public long Written { get; private set; }

    /// <summary>How far this muxer's timeline has reached, for whatever continues it.</summary>
    public double TimelineSeconds => _lastOut * ffmpeg.av_q2d(_reference);

    /// <summary>Set when the destination refused the bytes, which ends whatever is writing.</summary>
    public Exception? Fault { get; private set; }

    public void Write(MediaPacket packet)
    {
        if (Fault is not null)
        {
            return;
        }

        // Bounds-checked as well as mapped, because a subscriber's stream indexes and this
        // muxer's layout can disagree. Every consumer that writes bytes - Serve, the recorder,
        // every forward, the snapshot muxer - subscribes to every index and none of them re-reads
        // the layout afterwards, so a packet on an index this muxer's layout never had is a stale
        // consumer rather than a corrupt stream. Dropping the track is what the line below already
        // does for a track the container refused; without the check it is an
        // IndexOutOfRangeException on the hub's publishing thread instead, which ends the stream
        // for everybody rather than the one track for one consumer.
        if (packet.StreamIndex < 0 || packet.StreamIndex >= _mapping.Length)
        {
            return;
        }

        var output = _mapping[packet.StreamIndex];

        if (output < 0)
        {
            return;
        }

        var source = _layout.TimeBase(packet.StreamIndex);
        var shift = Rebase(packet, source);

        if (ffmpeg.av_new_packet(_packet, packet.Data.Length) < 0)
        {
            return;
        }

        try
        {
            Marshal.Copy(packet.Data, 0, (IntPtr)_packet->data, packet.Data.Length);

            _packet->stream_index = output;
            _packet->pts = Shifted(packet.Pts, shift, source);
            _packet->dts = Shifted(packet.Dts, shift, source);
            _packet->duration = packet.Duration;
            _packet->flags = packet.IsKeyframe ? ffmpeg.AV_PKT_FLAG_KEY : 0;
            _packet->pos = -1;

            // Timestamps mean nothing outside the time base they were measured in. Rescaling them
            // into the output's is the whole job of a remultiplexer.
            ffmpeg.av_packet_rescale_ts(_packet, source, _format->streams[output]->time_base);

            KeepMonotonic(output);

            EnsureHeader();

            ffmpeg.av_interleaved_write_frame(_format, _packet);
        }
        finally
        {
            ffmpeg.av_packet_unref(_packet);
        }
    }

    /// <summary>
    /// Gives up on finishing the container.
    ///
    /// For a viewer whose stream has moved to another replica: the connection is being handed on,
    /// and a trailer would tell the player the stream had ended when it has not.
    /// </summary>
    public void Abandon() => _headerWritten = false;

    /// <summary>
    /// Finishes the container. Safe to call twice, because the consumer that owns this and the
    /// disposal on the way out both want to be sure it happened.
    /// </summary>
    public void Close()
    {
        if (_format is null || !_headerWritten)
        {
            return;
        }

        _headerWritten = false;

        ffmpeg.av_write_trailer(_format);
        ffmpeg.avio_flush(_io);
    }

    /// <summary>
    /// Establishes and maintains this muxer's own timeline.
    ///
    /// The first packet it sees becomes its zero. A feed that goes away and comes back with its
    /// clock reset to zero would otherwise write timestamps behind what is already in the file,
    /// which no muxer accepts, so the shift is moved forward to continue after what was written.
    /// The interruption then reads as a gap in presentation time, which is what it was.
    /// </summary>
    private long Rebase(MediaPacket packet, AVRational source)
    {
        var stamp = packet.Dts != ffmpeg.AV_NOPTS_VALUE ? packet.Dts : packet.Pts;

        if (stamp == ffmpeg.AV_NOPTS_VALUE)
        {
            return _shift;
        }

        var here = ffmpeg.av_rescale_q(stamp, source, _reference);

        if (!_started)
        {
            _started = true;
            _shift = _origin - here;
        }
        else if (here + _shift < _lastOut - (long)(ClockReset.TotalSeconds / ffmpeg.av_q2d(_reference)))
        {
            _shift = _lastOut - here;
        }

        _lastOut = Math.Max(_lastOut, here + _shift);

        return _shift;
    }

    /// <summary>
    /// Keeps each stream's timestamps moving forward.
    ///
    /// One shift is computed for the whole part from its first packet, and the streams inside a
    /// transport are interleaved rather than aligned: whichever one that first packet belongs to,
    /// another can be a few milliseconds behind it and would land before where the previous part
    /// finished. Nudging it forward costs that much drift once per part, and the alternative is a
    /// recording whose timestamps go backwards at every join.
    /// </summary>
    private void KeepMonotonic(int output)
    {
        if (_packet->dts == ffmpeg.AV_NOPTS_VALUE)
        {
            return;
        }

        if (_written[output] != ffmpeg.AV_NOPTS_VALUE && _packet->dts <= _written[output])
        {
            var nudge = _written[output] - _packet->dts + 1;

            _packet->dts += nudge;

            if (_packet->pts != ffmpeg.AV_NOPTS_VALUE)
            {
                // Moved with it, so a frame never claims to be presented before it was decoded.
                _packet->pts += nudge;
            }
        }

        _written[output] = _packet->dts;
    }

    /// <summary>The shift is measured on the reference clock; a packet carries its own.</summary>
    private long Shifted(long timestamp, long shift, AVRational source)
        => timestamp == ffmpeg.AV_NOPTS_VALUE
            ? timestamp
            : timestamp + ffmpeg.av_rescale_q(shift, _reference, source);

    /// <summary>
    /// Deferred until the first packet, so nothing is written to the destination for a consumer
    /// that attaches and leaves before anything arrives.
    /// </summary>
    private void EnsureHeader()
    {
        if (_headerWritten)
        {
            return;
        }

        _headerWritten = true;

        if (ffmpeg.avformat_write_header(_format, null) < 0)
        {
            _headerWritten = false;
            Fault ??= new InvalidOperationException("libav could not write the container header.");
        }
    }

    private int OnWrite(void* opaque, byte* buffer, int size)
    {
        if (size <= 0)
        {
            return 0;
        }

        try
        {
            _destination.Write(new ReadOnlySpan<byte>(buffer, size));
            Written += size;

            return size;
        }
        catch (Exception ex)
        {
            // A viewer closing the player is the common case and is not an error, but the muxer
            // has to be told to stop either way.
            Fault ??= ex;

            // EIO. The bindings expose AVERROR but not errno, so the number is spelled out.
            return ffmpeg.AVERROR(5);
        }
    }

    public void Dispose()
    {
        Close();

        if (_packet is not null)
        {
            var packet = _packet;
            ffmpeg.av_packet_free(&packet);
            _packet = null;
        }

        if (_format is not null)
        {
            _format->pb = null;
            ffmpeg.avformat_free_context(_format);
            _format = null;
        }

        if (_io is not null)
        {
            // The IO buffer can have been reallocated by libav, so the context's own pointer is
            // the one to free rather than the one handed in.
            ffmpeg.av_free(_io->buffer);

            var io = _io;
            ffmpeg.avio_context_free(&io);
            _io = null;
        }
    }
}
