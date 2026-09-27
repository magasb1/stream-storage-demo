using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>One SRT socket, as an ordinary .NET stream.</summary>
public sealed unsafe class SrtSocketStream : Stream, IWireWriter
{
    /// <summary>Hands a chunk to libsrt, returning what <c>srt_sendmsg</c> returned.</summary>
    public delegate int Send(ReadOnlySpan<byte> chunk);

    /// <summary>Fills a buffer from libsrt, returning what <c>srt_recvmsg</c> returned.</summary>
    public delegate int Receive(Span<byte> buffer);

    private readonly int _socket;

    private readonly bool _writable;

    private readonly int _payloadSize;

    private readonly Send _send;

    private readonly Receive _receive;

    private bool _closed;

    public SrtSocketStream(int socket, bool writable)
        : this(socket, writable, send: null, receive: null, payloadSize: PayloadSizeOf(socket))
    {
    }

    /// <summary>The test seam.</summary>
    public SrtSocketStream(
        int socket,
        bool writable,
        Send? send,
        Receive? receive,
        int payloadSize = Srt.LiveDefaultPayloadSize)
    {
        _socket = socket;
        _writable = writable;
        _payloadSize = payloadSize > 0 ? payloadSize : Srt.LiveDefaultPayloadSize;

        _send = send ?? SendToSocket;
        _receive = receive ?? ReceiveFromSocket;
    }

    public override bool CanRead => !_writable && !_closed;

    public override bool CanWrite => _writable && !_closed;

    public override bool CanSeek => false;

    /// <summary>True once a send has been refused, which for a viewer means it has gone.</summary>
    public bool Faulted { get; private set; }

    /// <summary>What a write is cut into and the smallest buffer a read may be given.</summary>
    public int PayloadSize => _payloadSize;

    /// <summary>
    /// Bytes actually handed to libsrt, which for a forward is what "bytes sent" is meant to answer
    /// - not bytes muxed, which can differ from what leaves the socket by whatever the container's
    /// own overhead is.
    /// </summary>
    public long Written { get; private set; }

    /// <summary>
    /// What this connection lost and dropped since the last time it was asked, and what libsrt
    /// itself says about the link over the same interval - or null when the socket has gone and
    /// there is nothing to ask.
    /// </summary>
    public (int Lost, int Dropped, SrtLinkStats Link)? Health()
    {
        if (_closed || _socket == Srt.SRT_INVALID_SOCK)
        {
            return null;
        }

        if (!Srt.Stats(_socket, out var stats, clear: true))
        {
            return null;
        }

        return (
            stats.pktRcvLoss,
            stats.pktRcvDrop,
            new SrtLinkStats(
                stats.mbpsBandwidth,
                stats.mbpsRecvRate,
                stats.msRTT,
                stats.pktRcvRetrans,
                stats.msRcvTsbPdDelay,
                stats.pktRcvUndecryptTotal));
    }

    /// <summary>
    /// The sending twin of <see cref="Health"/>: what a forward's own connection reports about
    /// itself, over the same kind of interval and for the same reason <c>clear</c> exists there - a
    /// second read in the same beat would see the remainder of this one's window rather than a
    /// fresh sample.
    /// </summary>
    public (int Lost, int Dropped, SrtForwardLinkStats Link)? SendHealth()
    {
        if (_closed || _socket == Srt.SRT_INVALID_SOCK)
        {
            return null;
        }

        if (!Srt.Stats(_socket, out var stats, clear: true))
        {
            return null;
        }

        return (
            stats.pktSndLoss,
            stats.pktSndDrop,
            new SrtForwardLinkStats(
                stats.mbpsBandwidth,
                stats.mbpsSendRate,
                stats.msRTT,
                stats.pktRetrans,
                stats.msSndTsbPdDelay));
    }

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
        => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        if (_closed || buffer.IsEmpty)
        {
            return 0;
        }

        if (buffer.Length < _payloadSize)
        {
            // libsrt would answer SRT_EINVALMSGAPI, which reads as a coding error rather than as
            // the too-small buffer it is.
            throw new ArgumentException(
                $"An SRT read needs at least the payload size, {_payloadSize} bytes, "
                + $"and was given {buffer.Length}.",
                nameof(buffer));
        }

        while (!_closed)
        {
            var read = _receive(buffer);

            if (read > 0)
            {
                return read;
            }

            if (read == 0)
            {
                // The peer closed in an orderly way.
                return 0;
            }

            if (Srt.LastErrorCode() == Srt.SRT_ETIMEOUT)
            {
                // SRTO_RCVTIMEO expired, which is not end of stream.
                continue;
            }

            // A broken link, or a socket closed under us.
            Faulted = true;

            return 0;
        }

        return 0;
    }

    public override void Write(byte[] buffer, int offset, int count)
        => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (_closed || buffer.IsEmpty)
        {
            return;
        }

        while (!buffer.IsEmpty)
        {
            var chunk = buffer[..Math.Min(_payloadSize, buffer.Length)];

            if (_send(chunk) < 0)
            {
                // The socket's own error, which libsrt reports by return value.
                Faulted = true;

                throw new IOException($"The socket refused {chunk.Length} bytes: {Srt.LastError()}");
            }

            Written += chunk.Length;

            buffer = buffer[chunk.Length..];
        }
    }

    /// <summary>Nothing to do: libsrt puts every message on the wire as it is sent.</summary>
    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (!_closed)
        {
            _closed = true;

            if (_socket != Srt.SRT_INVALID_SOCK)
            {
                Srt.srt_close(_socket);
            }
        }

        base.Dispose(disposing);
    }

    private int SendToSocket(ReadOnlySpan<byte> chunk)
    {
        fixed (byte* data = chunk)
        {
            // ttl -1 and inorder 0 are the live defaults; the transport-stream timeline is the
            // muxer's business, not the socket's.
            return Srt.srt_sendmsg(_socket, data, chunk.Length, -1, 0);
        }
    }

    private int ReceiveFromSocket(Span<byte> buffer)
    {
        fixed (byte* data = buffer)
        {
            return Srt.srt_recvmsg(_socket, data, buffer.Length);
        }
    }

    /// <summary>
    /// The socket's configured payload, falling back to the live default when it cannot be read,
    /// which is what a socket that has already gone answers.
    /// </summary>
    private static int PayloadSizeOf(int socket)
    {
        if (socket == Srt.SRT_INVALID_SOCK)
        {
            return Srt.LiveDefaultPayloadSize;
        }

        var size = Srt.GetInt32(socket, SRT_SOCKOPT.SRTO_PAYLOADSIZE);

        return size is > 0 and <= Srt.LiveMaxPayloadSize ? size : Srt.LiveDefaultPayloadSize;
    }
}
