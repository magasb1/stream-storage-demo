using StorageDemo.Core.Storage;

namespace StorageDemo.Core.Documents;

/// <summary>The pieces of a segmented document, read as one file.</summary>
public sealed class PartedStream(
    IFileStorage storage,
    IReadOnlyList<DocumentPart> parts,
    CancellationToken cancellationToken = default) : Stream
{
    /// <summary>Where each part begins in the whole, so a position resolves by binary search.</summary>
    private readonly long[] _starts = Offsets(parts);

    private Stream? _open;
    private int _openPart = -1;
    private long _position;

    public override bool CanRead => true;

    public override bool CanSeek => true;

    public override bool CanWrite => false;

    public override long Length { get; } = parts.Sum(part => part.Size);

    public override long Position
    {
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    public override int Read(byte[] buffer, int offset, int count)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask().GetAwaiter().GetResult();

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken token = default)
    {
        if (buffer.IsEmpty || _position >= Length)
        {
            return 0;
        }

        var part = PartAt(_position);

        if (_openPart != part)
        {
            await OpenAsync(part, _position - _starts[part], token);
        }

        var read = await _open!.ReadAsync(buffer, token);

        if (read > 0)
        {
            _position += read;

            return read;
        }

        // The part ended where its recorded size said it would not.
        if (part + 1 >= parts.Count)
        {
            return 0;
        }

        _position = _starts[part + 1];
        await OpenAsync(part + 1, 0, token);

        return await ReadAsync(buffer, token);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token)
        => ReadAsync(buffer.AsMemory(offset, count), token).AsTask();

    /// <summary>
    /// Records the new position and lets the next read act on it, so nothing has to be opened here.
    /// </summary>
    public override long Seek(long offset, SeekOrigin origin)
    {
        var target = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => Length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin)),
        };

        ArgumentOutOfRangeException.ThrowIfNegative(target, nameof(offset));

        if (target != _position)
        {
            _position = target;
            Close(_open);
            _open = null;
            _openPart = -1;
        }

        return _position;
    }

    public override void Flush()
    {
    }

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    private async ValueTask OpenAsync(int part, long within, CancellationToken token)
    {
        Close(_open);

        _open = await storage.OpenReadAsync(parts[part].Key, within, token)
            ?? throw new StorageException($"Part {parts[part].Key} of this document is missing.");

        _openPart = part;
    }

    /// <summary>The index of the part holding this position.</summary>
    private int PartAt(long position)
    {
        var found = Array.BinarySearch(_starts, position);

        // An exact hit is the first byte of that part; otherwise the search says where it would go,
        // and the part before that is the one containing it.
        return found >= 0 ? found : ~found - 1;
    }

    private static long[] Offsets(IReadOnlyList<DocumentPart> parts)
    {
        var starts = new long[parts.Count];
        var running = 0L;

        for (var index = 0; index < parts.Count; index++)
        {
            starts[index] = running;
            running += parts[index].Size;
        }

        return starts;
    }

    private static void Close(Stream? stream) => stream?.Dispose();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close(_open);
            _open = null;
            _openPart = -1;
        }

        base.Dispose(disposing);
    }
}
