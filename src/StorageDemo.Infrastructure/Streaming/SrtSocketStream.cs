using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// One SRT socket, as an ordinary .NET stream.
///
/// Everything downstream of the hub keeps writing to a
/// <see cref="Stream"/> and never learns what is on the other end, so the recorder writes to a file
/// and a viewer writes to a socket through the same muxer and the same code.
///
/// Two of libsrt's rules leak through the <see cref="Stream"/> contract and cannot be hidden:
/// a read buffer smaller than the socket's payload size is refused with SRT_EINVALMSGAPI, and a
/// write larger than it is refused as a message, so writes are cut into payload-sized sends.
///
/// Reads and writes block, so this is only ever used from a thread already dedicated to one
/// connection.
/// </summary>
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

    /// <summary>
    /// The test seam. Substituting the two calls is what lets the chunking be proved on a machine
    /// with no libsrt, which is every machine until someone runs scripts/fetch-libsrt.sh.
    ///
    /// Public rather than internal only because the test project has no access to internals and
    /// giving it some would mean editing a csproj for one constructor.
    /// </summary>
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

    /// <summary>
    /// True once a send has been refused, which for a viewer means it has gone.
    ///
    /// Worth exposing, because the muxer swallows the exception a refused write throws: it treats
    /// that as "stop writing" rather than as a failure. Without a flag the caller cannot tell a
    /// stream that ended from a viewer that left, and would re-attach to a socket nobody is
    /// listening to, forever.
    /// </summary>
    public bool Faulted { get; private set; }

    /// <summary>What a write is cut into and the smallest buffer a read may be given.</summary>
    public int PayloadSize => _payloadSize;

    /// <summary>
    /// Bytes actually handed to libsrt, which for a forward is what "bytes sent" is meant to
    /// answer - not bytes muxed, which can differ from what leaves the socket by whatever the
    /// container's own overhead is.
    /// </summary>
    public long Written { get; private set; }

    /// <summary>
    /// What this connection lost and dropped since the last time it was asked, and what libsrt
    /// itself says about the link over the same interval - or null when the socket has gone and
    /// there is nothing to ask.
    ///
    /// Lost and dropped are the interval, not the running total, and the <c>clear</c> argument is
    /// what makes it one. An operator looking at a list of a thousand streams is asking which of
    /// them is broken now: a total answers "this one lost forty packets at some point today", which
    /// is true of a healthy stream that had one bad minute and says nothing about the last two
    /// seconds. Only the heartbeat calls this, once per beat per stream, so the window is that beat
    /// and the figure reads as "per two seconds" without anything having to record when it was last
    /// cleared.
    ///
    /// Lost and dropped are different failures. Lost is what never arrived and could not be
    /// retransmitted in time, which is the network or a saturated receive path; dropped is what
    /// arrived too late for the latency window, which is usually the latency window being too small
    /// for the link.
    ///
    /// One call to libsrt, not two. <c>clear</c> resets the interval counters it reads, so asking
    /// twice in the same beat for what should be one sample would make the second call read the
    /// remainder of the first's window rather than the same one - <see cref="Link"/> is built from
    /// this same read for exactly that reason.
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
    /// itself, over the same kind of interval and for the same reason <c>clear</c> exists there -
    /// a second read in the same beat would see the remainder of this one's window rather than a
    /// fresh sample.
    ///
    /// Genuinely different fields, not the same ones renamed. SRT_TRACEBSTATS keeps a separate
    /// counter for almost everything depending on which direction is asking, because a sender and
    /// a receiver are different questions about the same connection even when it is this replica
    /// asking both of them a beat apart on two different sockets. There is no sending analogue of
    /// a decrypt failure - decrypting is what a receiver does - so <see cref="SrtForwardLinkStats"/>
    /// simply carries no field for it, rather than one that would always read zero for a fact
    /// nothing here ever asked.
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
            // the too-small buffer it is. Say so here instead.
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
                // The peer closed in an orderly way. End of stream, and nothing faulted.
                return 0;
            }

            if (Srt.LastErrorCode() == Srt.SRT_ETIMEOUT)
            {
                // SRTO_RCVTIMEO expired, which is not end of stream. It is the chance to notice
                // that the socket has been disposed underneath us and read again if it has not,
                // so a shutdown never waits on a silent sender.
                //
                // ponytail: a read can still sit one receive timeout past a Dispose, because the
                // loop only looks between reads. Pass a CancellationToken in if that second ever
                // shows up in a shutdown measurement.
                continue;
            }

            // A broken link, or a socket closed under us. Both mean nothing more is coming, and
            // both are a fault rather than an ending.
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
                // The socket's own error, which libsrt reports by return value. Without this a
                // viewer who walked away would be written to forever.
                Faulted = true;

                throw new IOException($"The socket refused {chunk.Length} bytes: {Srt.LastError()}");
            }

            Written += chunk.Length;

            buffer = buffer[chunk.Length..];
        }
    }

    /// <summary>
    /// The synchronous send, inline, and returning a task that is already finished.
    ///
    /// libsrt has no asynchronous send: a send blocks until the socket has room, and making that
    /// non-blocking would mean libsrt's own epoll rather than anything a .NET task can express. A
    /// write to an SRT peer therefore occupies a thread whatever this method does, and the only
    /// thing an override changes is how many. The base class hands the blocking send to a
    /// thread-pool worker and waits for it, which costs one thread more than doing it here on the
    /// thread already serving this one connection - and a viewer or a forward is written to once per
    /// packet.
    /// </summary>
    public override ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }

        Write(buffer.Span);

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc cref="WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/>
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <summary>Nothing to do: libsrt puts every message on the wire as it is sent.</summary>
    public override void Flush()
    {
    }

    /// <inheritdoc cref="Flush"/>
    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

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
