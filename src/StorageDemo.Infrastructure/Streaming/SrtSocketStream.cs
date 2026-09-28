using System.Diagnostics;
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

    /// <summary>
    /// How long a send may make no progress at all before the peer is dropped, or zero for the
    /// behaviour every socket here had before <see cref="LiveOptions.ViewerSendStallSeconds"/>
    /// existed: no retry, and the first refused send faults the stream.
    /// </summary>
    private readonly long _stallBudgetTicks;

    private bool _closed;

    /// <summary>
    /// When the chunk currently being offered to libsrt was first offered, as a
    /// <see cref="Stopwatch"/> timestamp, or zero when no send is in progress or the budget is off.
    /// </summary>
    private long _offeredAt;

    public SrtSocketStream(int socket, bool writable, TimeSpan sendStallBudget = default)
        : this(
            socket,
            writable,
            send: null,
            receive: null,
            payloadSize: PayloadSizeOf(socket),
            sendStallBudget: sendStallBudget)
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
        int payloadSize = Srt.LiveDefaultPayloadSize,
        TimeSpan sendStallBudget = default)
    {
        _socket = socket;
        _writable = writable;
        _payloadSize = payloadSize > 0 ? payloadSize : Srt.LiveDefaultPayloadSize;

        _send = send ?? SendToSocket;
        _receive = receive ?? ReceiveFromSocket;

        _stallBudgetTicks = sendStallBudget > TimeSpan.Zero
            ? (long)(sendStallBudget.TotalSeconds * Stopwatch.Frequency)
            : 0;
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

    /// <summary>
    /// True when <see cref="Faulted"/> was set by the no-progress budget rather than by a socket
    /// that refused a send outright - that is, when this peer stopped taking bytes for longer than
    /// <see cref="LiveOptions.ViewerSendStallSeconds"/> while the connection itself stayed up.
    ///
    /// Separate from <see cref="Faulted"/> because the two are different events to an operator and
    /// only one of them is worth counting. A viewer closing its player faults every socket on the
    /// way out and is the ordinary end of a session; a viewer still acknowledging packets and
    /// accepting none of them is a peer that cleared its too-late-packet drop flag, which
    /// <c>Admit</c> cannot screen for because <c>SRTO_TLPKTDROP</c> on an accepted socket reads back
    /// our own setting rather than the peer's. This flag is the only way that condition is visible
    /// at all, which is why <c>LiveMetrics.ViewerDropped</c> exists and reads it.
    /// </summary>
    public bool SendStalled { get; private set; }

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

    /// <summary>
    /// Cuts the buffer into payload-sized sends, and bounds how long a peer that has stopped taking
    /// them may hold this thread.
    ///
    /// The bound is wall-clock time in which libsrt accepted nothing, never a count of timeouts,
    /// and the measurements in <c>SrtSendPressureTests</c> are why. <c>SRTO_SNDTIMEO</c> is a polling
    /// interval rather than a policy: a peer that advertised too-late-packet drop stalls for about
    /// a second and recovers whatever the option is set to, because that second is libsrt's own drop
    /// threshold - max(latency + SRTO_SNDDROPDELAY, 1000) + 20 ms, so 1020 ms at the 120 ms this
    /// service negotiates - showing through, while a peer that cleared the flag never recovers at any
    /// setting: probing libsrt 1.5.3 directly for #20 gave sixteen seconds of unbroken timeouts in a
    /// sixteen-second window, ended only by closing the socket. Two orders of magnitude apart, and the
    /// gap does not move with the timeout value, so the timeout value cannot be what the decision is
    /// made on.
    ///
    /// Retrying the timed-out chunk is lossless, which is the assumption the whole design rests on:
    /// <c>srt_sendmsg</c> returning SRT_ETIMEOUT has queued nothing, so the same bytes sent again are
    /// sent once. Measured rather than assumed, in
    /// <c>SrtSendPressureTests.A_send_that_timed_out_and_was_retried_loses_nothing_and_reorders_nothing</c>:
    /// forty thousand numbered messages through a peer with the drop flag cleared reading in bursts,
    /// twenty-three of them timed out and were retried, forty thousand received, none missing, none
    /// duplicated, none out of order.
    ///
    /// Isolated timeouts must not accumulate, which is why the clock resets on every accepted chunk.
    /// A viewer asking for a rollback is seeded with up to <c>ViewerQueuePackets</c> packets - about
    /// twenty-three seconds of media and thirty-six megabytes, well past the twelve-megabyte send
    /// buffer - before a live packet arrives, and that shows up as a couple of isolated timeouts with
    /// sends succeeding either side of them: two of them seeding thirty-six megabytes, never two in a
    /// row, the longest stretch with nothing accepted being one poll. A rule that counted timeouts, or
    /// dropped on the first, would drop every viewer that asked to start from the past.
    ///
    /// With a zero budget this loop is exactly what it was before the budget existed, and a socket
    /// with no <c>SRTO_SNDTIMEO</c> never returns SRT_ETIMEOUT in the first place. That is what keeps
    /// the forward's dialled socket and the ingest socket out of it without either being special-cased
    /// here.
    /// </summary>
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (_closed || buffer.IsEmpty)
        {
            return;
        }

        while (!buffer.IsEmpty)
        {
            var chunk = buffer[..Math.Min(_payloadSize, buffer.Length)];

            // Taken before the send rather than when one first times out, so the budget covers the
            // whole window in which nothing was accepted - including the first SRTO_SNDTIMEO
            // interval, which is otherwise a second of stall the budget cannot see. One timestamp
            // read per packet, on a path that is already a syscall per packet.
            if (_stallBudgetTicks > 0 && _offeredAt == 0)
            {
                _offeredAt = Stopwatch.GetTimestamp();
            }

            if (_send(chunk) < 0)
            {
                if (_stallBudgetTicks > 0 && Srt.LastErrorCode() == Srt.SRT_ETIMEOUT)
                {
                    if (Stopwatch.GetTimestamp() - _offeredAt < _stallBudgetTicks)
                    {
                        // The send buffer is full and nothing has aged out of it yet. Offer the same
                        // chunk again: nothing was queued, so nothing is duplicated by doing so.
                        continue;
                    }

                    SendStalled = true;
                    Faulted = true;

                    throw new IOException(
                        $"The socket accepted nothing for {_stallBudgetTicks / (double)Stopwatch.Frequency:0.#} s, "
                        + "which is the no-progress budget for a peer that is still connected.");
                }

                // The socket's own error, which libsrt reports by return value. Without this a
                // viewer who walked away would be written to forever.
                Faulted = true;

                throw new IOException($"The socket refused {chunk.Length} bytes: {Srt.LastError()}");
            }

            _offeredAt = 0;

            Written += chunk.Length;

            buffer = buffer[chunk.Length..];
        }
    }

    /// <summary>
    /// The synchronous send, inline, and returning a task that is already finished.
    ///
    /// libsrt has no asynchronous send: a send blocks until the socket has room, and making that
    /// non-blocking would mean libsrt's own epoll rather than anything a .NET task can express. A
    /// write to an SRT peer therefore occupies a thread whatever this method does.
    ///
    /// How long it occupies one used to be the peer's business entirely, and it is measured in
    /// <c>SrtSendPressureTests</c>: a peer advertising too-late-packet drop lets libsrt discard from
    /// this buffer, so the write never waits; a peer that cleared that flag fills the buffer and the
    /// write waited with no bound at all until <see cref="Write(ReadOnlySpan{byte})"/> grew one. It is
    /// now the peer's business up to the no-progress budget and this class's after it, which is the
    /// reason the consumption port's comment about this method is as long as it is. What the base
    /// class does is not wrong so much as pointless here: it moves the blocking send to a
    /// thread-pool worker and releases the caller while it runs, which for a consumer that has
    /// nothing else to do until the write finishes buys a hop and a <see cref="Task"/> and no
    /// concurrency at all.
    ///
    /// The load-bearing part is what that costs the viewer path. Serving a viewer drains its muxed
    /// bytes with an await per packet, and against a destination like this one that await completes
    /// synchronously and the loop simply carries on. Left to the base class, every packet for every
    /// direct viewer would instead suspend the loop and queue a work item, which is a per-packet
    /// pool dispatch on the busiest path this service has.
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
