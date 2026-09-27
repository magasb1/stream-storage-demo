using System.Buffers;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// Collects what a muxer writes, so whoever owns it can hand the bytes on asynchronously.
///
/// libav writes through a synchronous callback and has no asynchronous form of it, so a consumer
/// that muxes straight into its destination writes to that destination from inside the callback.
/// For anything on a dedicated thread - a recording writing to a file, a forward writing to a
/// socket - that is exactly right. For a viewer it was the whole of a measured failure: a consumer
/// that will not take the bytes does not merely fall behind, it holds the thread that is writing to
/// it, and with a thread-pool worker per slow viewer and nothing bounding how many, viewers per
/// replica were bounded by threads rather than by bandwidth.
///
/// Writing into memory cannot block on anybody. The caller then awaits its destination between
/// packets, where a viewer that is not reading costs a pending continuation rather than a thread.
///
/// It is not a queue and it absorbs nothing: what absorbs a slow viewer is the subscription's own
/// bounded queue, which skips to live when it fills. This holds what one packet muxed to, because
/// the muxer flushes per packet and the caller drains it per packet, and it exists only so that the
/// muxing and the writing need not happen on the same thread.
///
/// Written and drained by one task in turn, never concurrently, which is what lets the buffer be
/// reused rather than copied out.
/// </summary>
internal sealed class MuxerBuffer : Stream
{
    /// <summary>
    /// What one packet is expected to mux to. Larger than most and smaller than a keyframe at
    /// contribution bitrates, which is what the growth below is for; the rented array is usually
    /// bigger than this anyway.
    /// </summary>
    private const int InitialCapacity = 16 * 1024;

    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(InitialCapacity);

    private int _pending;

    public override bool CanRead => false;

    public override bool CanWrite => true;

    public override bool CanSeek => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
        => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty)
        {
            return;
        }

        if (_pending + buffer.Length > _buffer.Length)
        {
            var grown = ArrayPool<byte>.Shared.Rent(Math.Max(_pending + buffer.Length, _buffer.Length * 2));

            _buffer.AsSpan(0, _pending).CopyTo(grown);
            ArrayPool<byte>.Shared.Return(_buffer);

            _buffer = grown;
        }

        buffer.CopyTo(_buffer.AsSpan(_pending));
        _pending += buffer.Length;
    }

    /// <summary>
    /// Hands everything muxed since the last drain to the destination, and waits for it to be
    /// taken. This is the wait a slow consumer causes, and the whole point of it is that it is
    /// awaited rather than blocked on.
    /// </summary>
    public async ValueTask DrainToAsync(Stream destination, CancellationToken cancellationToken)
    {
        if (_pending == 0)
        {
            return;
        }

        var writing = _pending;

        await destination.WriteAsync(_buffer.AsMemory(0, writing), cancellationToken);

        // After the write rather than before it: the buffer is only free to be filled again once
        // the destination has taken what is in it, and nothing else writes here meanwhile.
        _pending = 0;
    }

    /// <summary>Nothing to do: the bytes are in memory, and the drain above is what moves them.</summary>
    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (_buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = [];
        }

        base.Dispose(disposing);
    }
}
