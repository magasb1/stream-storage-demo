using FFmpeg.AutoGen.Abstractions;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>A libav transport opened from a URL, as an ordinary .NET stream.</summary>
public sealed unsafe class AvioWriter : Stream, IWireWriter
{
    private readonly string _url;

    private AVIOContext* _io;

    public AvioWriter(string url)
    {
        _url = url;

        AVIOContext* io = null;

        var opened = ffmpeg.avio_open2(&io, url, ffmpeg.AVIO_FLAG_WRITE, null, null);

        if (opened < 0 || io is null)
        {
            // Closed rather than abandoned: libav can have allocated the context and then failed
            // inside the protocol's own open, and on that path it hands the half-built context
            // back.
            if (io is not null)
            {
                ffmpeg.avio_closep(&io);
            }

            throw new InvalidOperationException(
                $"Could not open '{url}' to write: {FfmpegLibrary.Describe(opened)}.");
        }

        _io = io;
    }

    /// <summary>Bytes handed to libav, which is what the forward has actually pushed.</summary>
    public long Written { get; private set; }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
        => Write(new ReadOnlySpan<byte>(buffer, offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (_io is null)
        {
            throw new ObjectDisposedException(nameof(AvioWriter));
        }

        fixed (byte* bytes = buffer)
        {
            ffmpeg.avio_write(_io, bytes, buffer.Length);
        }

        // avio_write reports nothing, so a far end that has gone away is invisible until the
        // context is asked.
        if (_io->error < 0)
        {
            throw new IOException($"Writing to '{_url}' failed: {FfmpegLibrary.Describe(_io->error)}.");
        }

        Written += buffer.Length;
    }

    public override void Flush()
    {
        if (_io is not null)
        {
            ffmpeg.avio_flush(_io);
        }
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (_io is not null)
        {
            var io = _io;

            // Nulled first: avio_closep flushes, which can fault on a peer that is already gone,
            // and a second Dispose finding the old pointer would close freed memory.
            _io = null;

            ffmpeg.avio_closep(&io);
        }

        base.Dispose(disposing);
    }
}
