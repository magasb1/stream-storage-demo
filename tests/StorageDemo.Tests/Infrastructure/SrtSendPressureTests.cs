using System.Diagnostics;
using System.Net;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Integration;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// Whether a send to a peer that has stopped reading waits, measured on the sending socket.
///
/// This is the mechanism underneath both halves of the slow-viewer question, and until this existed
/// it was only ever inferred. <c>LiveSlowPlayerTests</c> counts what the replica holds while slow
/// players are served, which answers "does it cost a thread" and cannot answer "why not": every
/// figure it collects is receive-side, on the reader's own socket, and a sender that was never put
/// under back-pressure looks exactly like one that was and shrugged. So this asks the sending socket
/// directly - libsrt's own <c>pktSndDrop</c>, <c>msSndBuf</c> and <c>byteAvailSndBuf</c> - with a peer
/// that reads nothing at all.
///
/// Two sockets over loopback and no service at all, because the subject is the transport. What
/// stands in for a viewer is a <see cref="SrtSocketStream"/> over an accepted socket configured the
/// way <c>SrtListener</c> configures one, which is the object whose <c>Write</c> the consumption port
/// blocks in.
///
/// The condition is the whole point, and it is not ours to set. libsrt drops from its own send buffer
/// only when the peer advertised <c>SRT_OPT_TLPKTDROP</c> in the handshake - <c>sndDropTooLate</c>
/// returns immediately when that flag is clear - and this service sets nothing about it on either
/// side. It is the player's default, and a player can clear it with one query parameter on its URL.
/// So there is a test each way, and the pair is the evidence for what the consumption port may and
/// may not assume about a viewer it did not configure.
/// </summary>
public sealed unsafe class SrtSendPressureTests(ITestOutputHelper output)
{
    /// <summary>
    /// What the sender pushes, and what the peer refuses to read: the rate
    /// <c>LiveSlowPlayerTests</c> measures at, so the two can be read together.
    /// </summary>
    private const int BitsPerSecond = 12_288_000;

    /// <summary>
    /// How long each case is watched. The receive buffer fills in about seven seconds at this rate
    /// and the send buffer in about seven more, so twenty-five leaves both cases past the point where
    /// they differ without waiting on anything twice.
    /// </summary>
    private static readonly TimeSpan Watched = TimeSpan.FromSeconds(25);

    private static readonly TimeSpan Sample = TimeSpan.FromSeconds(2);

    [Fact]
    public void A_peer_that_stops_reading_is_dropped_from_the_send_buffer_rather_than_waited_for()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);

        var pressed = Pressure(peerDropsLatePackets: true);

        // The mechanism, measured rather than named: libsrt threw the peer's backlog out of its own
        // send buffer, so the write always had somewhere to put the next packet. Measured here at 461
        // packets a second discarded out of about 1170 written, with the buffer holding steady at
        // 1020 ms - which is the threshold itself, max(negotiated latency + SRTO_SNDDROPDELAY, 1000)
        // + 20 ms - and at 1581 KB, an eighth of the twelve megabytes libsrt says it could hold. The
        // rest of the stream went onto the wire and was discarded by the receiver instead, which is
        // the other half of the picture and is measured in LiveSlowPlayerTests.
        Assert.True(
            pressed.Drops > 0,
            $"the sender discarded nothing for a peer that read nothing, having written "
            + $"{pressed.Written / 1024} KB, so what keeps a slow viewer from holding this thread is "
            + "not sender-side drop and every claim that rests on it is wrong");

        // No send waited at all: 0.00 s, every sample, in every run. The bound is generous because
        // what it excludes is a send that waits, not a send that is slow.
        Assert.True(
            pressed.Stalled < TimeSpan.FromSeconds(3),
            $"a send was outstanding for {pressed.Stalled.TotalSeconds:0.0} s against a peer "
            + "advertising packet drop, which is longer than the drop threshold can explain");
    }

    [Fact]
    public void A_peer_that_cleared_its_drop_flag_blocks_the_send_without_bound()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);

        var pressed = Pressure(peerDropsLatePackets: false);

        // The other half, and the reason #20 stays open rather than closing with the measurement
        // above. One query parameter on a player's URL clears this flag, the consumption port admits
        // every caller by design, and libsrt then has nothing to free its send buffer with: it fills
        // to its twelve megabytes, the next send waits on a condition variable with no timeout
        // because SRTO_SNDTIMEO is -1 unless somebody sets it, and a peer that keeps acknowledging
        // never trips the connection-lost exit either. Measured: the buffer filled in fourteen
        // seconds and one send was still inside libsrt twelve seconds later, when closing the socket
        // was the only thing that brought it back. On the consumption port that is a thread-pool
        // worker, and it is a stranger's to take.
        //
        // The day a send timeout lands, this is the test to change: it is the measurement of what the
        // timeout is for, and the figure it asserts becomes the bound rather than the absence of one.
        Assert.True(
            pressed.Stalled > TimeSpan.FromSeconds(5),
            $"no send was outstanding for more than {pressed.Stalled.TotalSeconds:0.0} s against a "
            + $"peer that reads nothing and drops nothing, having taken {pressed.Written / 1024} KB "
            + $"and discarded {pressed.Drops}");

        // And it waited because nothing freed the buffer, which is what distinguishes this case from
        // the one above rather than a second symptom of it.
        Assert.Equal(0, pressed.Drops);
    }

    /// <summary>
    /// Writes to a peer that never reads, and reports what the sending socket says about it: how much
    /// it discarded, the longest a single <see cref="SrtSocketStream.Write"/> took, and how much was
    /// accepted before the window ran out.
    /// </summary>
    private Pressed Pressure(bool peerDropsLatePackets)
    {
        var port = SrtSenders.FreePort();

        Srt.EnsureStarted();

        var listener = Listening(port);

        try
        {
            using var peer = Calling(port, peerDropsLatePackets);

            var accepted = Srt.srt_accept(listener, null, null);

            Assert.True(
                accepted != Srt.SRT_INVALID_SOCK,
                $"nothing was accepted on {port}: {Srt.LastError()}");

            // Exactly what an accepted viewer gets, because the object under measurement is the one
            // the consumption port writes viewers through.
            using var sender = new SrtSocketStream(accepted, writable: true);

            Srt.Stats(accepted, out var settled, clear: false);

            output.WriteLine(
                $"peer advertises packet drop: {peerDropsLatePackets}; payload {sender.PayloadSize} B, "
                + $"send buffer {settled.byteAvailSndBuf / 1024} KB available, "
                + $"{settled.msSndTsbPdDelay} ms of negotiated latency");

            var pushing = new Pushing(sender, BitsPerSecond);

            try
            {
                output.WriteLine($"{"at",-6}{"written",-10}{"sndbuf",-10}{"held",-8}{"dropped",-9}{"in flight",-11}avail");

                var started = Stopwatch.StartNew();
                var stalled = TimeSpan.Zero;

                while (started.Elapsed < Watched)
                {
                    Thread.Sleep(Sample);

                    Srt.Stats(accepted, out var stats, clear: false);

                    // The age of the send that has not returned, sampled rather than recorded on
                    // completion: the send this test exists to catch never completes at all, so a
                    // figure taken when a write returns would read zero for the whole window.
                    stalled = pushing.InFlight > stalled ? pushing.InFlight : stalled;

                    output.WriteLine(
                        $"{started.Elapsed.TotalSeconds,-6:0}{pushing.Written / 1024,-10}"
                        + $"{stats.byteSndBuf / 1024,-10}{stats.msSndBuf,-8}{stats.pktSndDrop,-9}"
                        + $"{pushing.InFlight.TotalSeconds,-11:0.00}{stats.byteAvailSndBuf / 1024}");
                }

                Srt.Stats(accepted, out var last, clear: false);

                output.WriteLine(
                    $"wrote {pushing.Written / 1024} KB in {Watched.TotalSeconds:0} s, longest send that "
                    + $"returned {pushing.Longest.TotalSeconds:0.00} s, longest still outstanding "
                    + $"{stalled.TotalSeconds:0.00} s, {last.pktSndDrop} packets dropped by the sender, "
                    + $"{last.pktSndLoss} reported lost, {last.pktRetrans} resent, buffer holding "
                    + $"{last.byteSndBuf / 1024} KB at {last.msSndBuf} ms with {last.byteAvailSndBuf / 1024} KB free");

                return new Pressed(last.pktSndDrop, stalled, pushing.Written);
            }
            finally
            {
                // The socket goes first: closing it is what unblocks a send already inside libsrt,
                // which in the second case is where the writing thread is and where it would stay.
                sender.Dispose();

                pushing.Dispose();
            }
        }
        finally
        {
            Srt.srt_close(listener);
        }
    }

    /// <param name="Drops">Packets libsrt threw out of the sending socket's own buffer.</param>
    /// <param name="Stalled">The longest a single send was seen outstanding, which is the question.</param>
    /// <param name="Written">What the peer's socket accepted before the window ran out.</param>
    private readonly record struct Pressed(int Drops, TimeSpan Stalled, long Written);

    private static int Listening(int port)
    {
        var listener = Srt.srt_create_socket();

        Assert.True(listener != Srt.SRT_INVALID_SOCK, $"no listening socket: {Srt.LastError()}");

        Srt.SetBool(listener, SRT_SOCKOPT.SRTO_REUSEADDR, true);

        // The figure the service configures, because the drop threshold is derived from the latency
        // the two ends negotiate and a test at another latency would be measuring another threshold.
        Srt.SetInt32(listener, SRT_SOCKOPT.SRTO_LATENCY, 120);

        var address = new IPEndPoint(IPAddress.Loopback, port).Serialize();

        fixed (byte* raw = address.Buffer.Span)
        {
            Assert.True(Srt.srt_bind(listener, raw, address.Size) == 0, $"bind: {Srt.LastError()}");
        }

        Assert.True(Srt.srt_listen(listener, 1) == 0, $"listen: {Srt.LastError()}");

        return listener;
    }

    /// <summary>
    /// The peer, connected and then left alone: it never reads a byte, which is the case both tests
    /// are about. Disposed by closing, since there is no stream over it to close instead.
    /// </summary>
    private static Peer Calling(int port, bool dropsLatePackets)
    {
        var caller = Srt.srt_create_socket();

        Assert.True(caller != Srt.SRT_INVALID_SOCK, $"no calling socket: {Srt.LastError()}");

        if (!dropsLatePackets)
        {
            // Before the handshake, because it is the handshake that carries the flag, and it is the
            // flag rather than this socket's own behaviour that decides what the far end may discard.
            Assert.True(
                Srt.SetBool(caller, SRT_SOCKOPT.SRTO_TLPKTDROP, false),
                $"could not clear the peer's packet-drop flag: {Srt.LastError()}");
        }

        var address = new IPEndPoint(IPAddress.Loopback, port).Serialize();

        fixed (byte* raw = address.Buffer.Span)
        {
            Assert.True(Srt.srt_connect(caller, raw, address.Size) == 0, $"connect: {Srt.LastError()}");
        }

        return new Peer(caller);
    }

    private sealed class Peer(int socket) : IDisposable
    {
        public void Dispose() => Srt.srt_close(socket);
    }

    /// <summary>
    /// Writes to the sender at a fixed rate on a thread of its own, keeping what only the caller of a
    /// blocking write can know: how long the longest one took, and how long the one in flight has
    /// been in flight. A thread rather than a task, because a blocked send is exactly what is being
    /// measured and it must not be measured on the pool this process also uses for everything else.
    /// </summary>
    private sealed class Pushing : IDisposable
    {
        private readonly Thread _thread;

        private long _written;

        private long _longestTicks;

        private long _startedTicks;

        public Pushing(SrtSocketStream sender, int bitsPerSecond)
        {
            _thread = new Thread(() => Push(sender, bitsPerSecond))
            {
                IsBackground = true,
                Name = "send pressure",
            };

            _thread.Start();
        }

        public long Written => Interlocked.Read(ref _written);

        public TimeSpan Longest => TimeSpan.FromTicks(Interlocked.Read(ref _longestTicks));

        /// <summary>How long the write that has not returned yet has been waiting, or zero between writes.</summary>
        public TimeSpan InFlight => Interlocked.Read(ref _startedTicks) is var started and not 0
            ? TimeSpan.FromTicks(Math.Max(Stopwatch.GetTimestamp() - started, 0) * TimeSpan.TicksPerSecond / Stopwatch.Frequency)
            : TimeSpan.Zero;

        public void Dispose() => _thread.Join(TimeSpan.FromSeconds(10));

        private void Push(SrtSocketStream sender, int bitsPerSecond)
        {
            var buffer = new byte[sender.PayloadSize];

            Random.Shared.NextBytes(buffer);

            var bytesASecond = bitsPerSecond / 8d;
            var started = Stopwatch.StartNew();

            try
            {
                while (true)
                {
                    Interlocked.Exchange(ref _startedTicks, Stopwatch.GetTimestamp());

                    var at = started.Elapsed;

                    sender.Write(buffer);

                    var took = started.Elapsed - at;

                    Interlocked.Exchange(ref _startedTicks, 0);

                    if (took.Ticks > Interlocked.Read(ref _longestTicks))
                    {
                        Interlocked.Exchange(ref _longestTicks, took.Ticks);
                    }

                    var total = Interlocked.Add(ref _written, buffer.Length);

                    var due = TimeSpan.FromSeconds(total / bytesASecond) - started.Elapsed;

                    if (due > TimeSpan.Zero)
                    {
                        Thread.Sleep(due);
                    }
                }
            }
            catch (Exception)
            {
                // The socket was closed under the write, which is how both cases end: the second one
                // is still inside libsrt when the window runs out, and closing is the only thing that
                // brings it back.
            }
        }
    }
}
