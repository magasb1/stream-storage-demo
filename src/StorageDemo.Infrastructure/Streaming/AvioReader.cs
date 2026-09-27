using FFmpeg.AutoGen.Abstractions;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>An ordinary .NET stream, as a libav transport.</summary>
public sealed unsafe class AvioReader : IDisposable
{
    /// <summary>
    /// libav's buffer between the read callback and the demuxer, the same size the muxer uses.
    /// </summary>
    private const int IoBufferSize = 64 * 1024;

    private readonly Stream _source;

    /// <summary>Held so the garbage collector cannot free the thunk libav reads through.</summary>
    private readonly avio_alloc_context_read_packet _read;

    private AVIOContext* _io;

    public AvioReader(Stream source)
    {
        _source = source;
        _read = OnRead;

        var buffer = (byte*)ffmpeg.av_malloc(IoBufferSize);

        _io = ffmpeg.avio_alloc_context(
            buffer,
            IoBufferSize,
            write_flag: 0,
            // Null, and the stream is reached through the captured delegate instead.
            opaque: null,
            read_packet: _read,
            write_packet: null,
            seek: null);

        if (_io is null)
        {
            ffmpeg.av_free(buffer);

            throw new InvalidOperationException("Could not allocate an IO context for the demuxer.");
        }

        // Not seekable.
        _io->seekable = 0;
    }

    /// <summary>
    /// The transport to hand to <see cref="StreamDemuxer"/>, which takes ownership of it and closes
    /// it; disposing this reader closes the underlying <see cref="Stream"/> and nothing else.
    /// </summary>
    public AVIOContext* Context => _io;

    private int OnRead(void* opaque, byte* buffer, int size)
    {
        try
        {
            var read = _source.Read(new Span<byte>(buffer, size));

            // A stream that has ended reads zero forever, and libav reads zero as "nothing yet, ask
            // again", so the ending has to be spelled out or av_read_frame spins on it.
            return read > 0 ? read : ffmpeg.AVERROR_EOF;
        }
        catch (Exception)
        {
            // A sender that vanished mid-read is the common case and is not an error here; the
            // demuxer has to be told to stop either way, and it reports the feed as ended.
            return ffmpeg.AVERROR(5);
        }
    }

    public void Dispose()
    {
        // The context and its buffer are the demuxer's to free, by way of avio_closep.
        _io = null;

        _source.Dispose();
    }
}
