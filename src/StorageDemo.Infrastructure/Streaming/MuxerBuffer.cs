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
/// the muxer flushes per packet and the caller drains it per packet.
///
/// Written and drained by one task in turn, never concurrently, which is what lets the array be
/// reused rather than copied out. The array is rented, and a write after disposal is refused rather
/// than allowed to reach an array something else has since been given - which is also why a caller
/// declares this before the muxer that writes through it, so that disposal happens the other way
/// round.
///
/// Public only because the test project has no access to internals, and the lifetime of a borrowed
/// array is worth a test of its own; nothing outside this assembly has a reason to use it.
/// </summary>
public sealed class MuxerBuffer : Stream
{
    /// <summary>
    /// What one packet is expected to mux to, which is libav's own IO buffer size: the callback
    /// hands over at most that much at a time, so the first full flush never has to grow. It grows
    /// for a keyframe at contribution bitrates and then stays grown, because the high-water mark of
    /// one connection is held for the life of that connection - a few tens of kilobytes per viewer,
    /// against the same figure libav already spends on the buffer it writes from.
    /// </summary>
    private const int InitialCapacity = 64 * 1024;

    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(InitialCapacity);

    private int _pending;

    private bool _disposed;

    public override bool CanRead => false;

    public override bool CanWrite => !_disposed;

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
        ObjectDisposedException.ThrowIf(_disposed, this);

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

    /// <summary>How much is waiting to be drained, which is what one packet muxed to.</summary>
    public int Pending => _pending;

    /// <summary>
    /// Hands everything muxed since the last drain to the destination, and waits for it to be
    /// taken. This is the wait a slow consumer causes, and the whole point of it is that it is
    /// awaited rather than blocked on.
    /// </summary>
    public async ValueTask DrainToAsync(Stream destination, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

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
        if (!_disposed)
        {
            _disposed = true;
            _pending = 0;

            ArrayPool<byte>.Shared.Return(_buffer);

            // Not a rented array any more, so a second disposal has nothing to give back and a
            // write has somewhere harmless to fail.
            _buffer = [];
        }

        base.Dispose(disposing);
    }
}
