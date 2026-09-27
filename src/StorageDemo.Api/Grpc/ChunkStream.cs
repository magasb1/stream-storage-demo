using Grpc.Core;
using StorageDemo.Grpc;

namespace StorageDemo.Api.Grpc;

/// <summary>
/// Presents an inbound client-streaming call as a read-only <see cref="Stream"/>, so uploads flow
/// straight into the file store without the whole file ever being buffered.
/// </summary>
public sealed class ChunkStream(
    IAsyncStreamReader<UploadRequest> requestStream,
    CancellationToken cancellationToken,
    ReadOnlyMemory<byte> prefix = default) : Stream
{
    private ReadOnlyMemory<byte> _remainder = prefix;
    private bool _finished;

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, token);

        while (_remainder.IsEmpty && !_finished)
        {
            if (!await requestStream.MoveNext(linked.Token))
            {
                _finished = true;
                break;
            }

            if (requestStream.Current.PayloadCase == UploadRequest.PayloadOneofCase.Chunk)
            {
                _remainder = requestStream.Current.Chunk.Memory;
            }
        }

        if (_remainder.IsEmpty)
        {
            return 0;
        }

        var count = Math.Min(buffer.Length, _remainder.Length);
        _remainder[..count].CopyTo(buffer);
        _remainder = _remainder[count..];

        return count;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token)
        => await ReadAsync(buffer.AsMemory(offset, count), token);

    public override int Read(byte[] buffer, int offset, int count)
        => ReadAsync(buffer.AsMemory(offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
