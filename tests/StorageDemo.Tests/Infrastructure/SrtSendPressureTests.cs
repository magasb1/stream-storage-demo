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
///
/// Since #20 the pair is also the boundary of a policy rather than a description of two behaviours.
/// The accepted socket now carries <c>SRTO_SNDTIMEO</c> and the stream over it a no-progress budget,
/// so the first case must still be untouched and the second must end inside the budget. Two further
/// cases hold up the shape of that policy: that isolated timeouts do not accumulate into it, which is
/// what a viewer asking for a rollback produces, and that retrying a timed-out send loses nothing,
/// which is the assumption the retry rests on.
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

    /// <summary>
    /// What <c>SrtListener</c> sets on an accepted viewer socket, repeated rather than shared because
    /// it is private there and the point of repeating it is that a test which drifts from the service
    /// is measuring something else.
    /// </summary>
    private const int SendTimeoutMilliseconds = 1000;

    /// <summary>
    /// <c>LiveOptions.ViewerSendStallSeconds</c> at its default, for the same reason.
    /// </summary>
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// What seeding a viewer's rollback hands the socket in one go: <c>ViewerQueuePackets</c> of 2000
    /// demultiplexed frames, which at the frame sizes this bitrate produces is about this much, and
    /// three times the twelve megabytes libsrt's send buffer holds.
    /// </summary>
    private const long SeedBytes = 36L * 1024 * 1024;

    /// <summary>
    /// How much of the seed goes in per call. A viewer is written frame by frame, not in one
    /// thirty-six-megabyte call, and a call large enough to be the whole seed would hide how the
    /// timeouts are spread through it.
    /// </summary>
    private const int BurstBytes = 256 * 1024;

    /// <summary>The poll for the retry case, short enough to make retries happen inside a test.</summary>
    private const int RetryTimeoutMilliseconds = 300;

    /// <summary>
    /// How long the retry case gives the peer to take the whole sequence. It reads a thousand at a
    /// time and then pauses half a second, so about two thousand messages a second and a transfer of
    /// roughly twenty seconds; this is generous rather than tight.
    /// </summary>
    private static readonly TimeSpan Drained = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How many numbered messages the retry case sends.
    ///
    /// Sized against the buffers rather than picked: the sender's twelve megabytes and the peer's
    /// twelve megabytes hold about eighteen thousand messages of this size between them, and until a
    /// sequence is comfortably past that the sender is never made to wait at all - twenty thousand
    /// through a peer reading five hundred at a time produced zero timeouts and proved nothing. This
    /// leaves some twenty-two thousand that have to be metered through the reader's pauses, which is
    /// where the retries come from.
    /// </summary>
    private const int Messages = 40_000;

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

        // The ordinary viewer, and the reason this assertion is here rather than in the test below:
        // this peer is on the worst network there is - it reads nothing at all - and it must survive
        // the send budget untouched, because what it loses is picture and #11 settled that losing
        // picture is the right answer for it. Measured with SRTO_SNDTIMEO set: zero timeouts across
        // the whole window, the full 12.29 Mbit/s onto the socket, 1.7 MB resident. If tuning
        // ViewerSendStallSeconds down ever starts dropping real viewers, this is the assertion that
        // says so, and it should be read before the figure is changed rather than after.
        Assert.False(
            pressed.Faulted,
            $"a peer advertising packet drop was faulted by the {Budget.TotalSeconds:0} s send budget "
            + $"after {pressed.Written / 1024} KB, which means the budget can reach an ordinary viewer "
            + "on a bad network and not only one that cleared its drop flag");
    }

    /// <summary>
    /// Was <c>A_peer_that_cleared_its_drop_flag_blocks_the_send_without_bound</c>, which said in its
    /// own comment that the day a send timeout landed this was the test to change and that the figure
    /// it asserts becomes the bound rather than the absence of one. It has, so it is, and the name
    /// had to go with it: the old one now claims the opposite of what the test proves.
    /// </summary>
    [Fact]
    public void A_peer_that_cleared_its_drop_flag_is_dropped_when_the_send_budget_runs_out()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);

        var pressed = Pressure(peerDropsLatePackets: false);

        // The other half, and the reason #20 stays open rather than closing with the measurement
        // above. One query parameter on a player's URL clears this flag, the consumption port admits
        // every caller by design, and libsrt then has nothing to free its send buffer with: it fills
        // to its twelve megabytes and the next send waits. Before the budget it waited on a condition
        // variable with no timeout at all, because SRTO_SNDTIMEO was -1 and nobody set it, and a peer
        // that keeps acknowledging never trips the connection-lost exit either: measured then at the
        // buffer filling in fourteen seconds and one send still inside libsrt twelve seconds later,
        // when closing the socket was the only thing that brought it back. On the consumption port
        // that is a thread-pool worker, and it is a stranger's to take.
        //
        // Now it is taken for the budget and no longer. The timeout itself does not end the wait:
        // probing libsrt 1.5.3 directly for #20 gave sixteen unbroken seconds of timeouts against this
        // peer in a sixteen-second window, whatever SRTO_SNDTIMEO was set to, so its value decides
        // only how often the budget is consulted. What ends the wait is the budget, and the figure
        // below is what a stranger can now cost this pod: one thread and one send buffer for that
        // long, rather than until somebody notices. Measured here at 5.00 s for the send that gave
        // up, with the buffer at 10.9 MB and nothing free.
        Assert.True(
            pressed.Longest > TimeSpan.Zero && pressed.Longest < Budget + TimeSpan.FromSeconds(2),
            $"the send that gave up took {pressed.Longest.TotalSeconds:0.00} s against a peer that "
            + $"reads nothing and drops nothing, which is not the {Budget.TotalSeconds:0} s budget "
            + $"plus at most one poll; it had taken {pressed.Written / 1024} KB first and discarded "
            + $"{pressed.Drops}");

        // And it gave up because of the budget rather than because the socket broke under it, which
        // is the difference the log line and the dropped-viewer counter are built on.
        Assert.True(pressed.Faulted, "the stream was not faulted, so the viewer would not be dropped");

        Assert.True(
            pressed.SendStalled,
            "the stream faulted for some reason other than the no-progress budget, so this measured "
            + "something other than what it claims to");

        // And it waited because nothing freed the buffer, which is what distinguishes this case from
        // the one above rather than a second symptom of it.
        Assert.Equal(0, pressed.Drops);
    }

    /// <summary>
    /// The rollback seed, which is the case a naive rule gets wrong.
    ///
    /// Serving a viewer that asked to start from the past muxes up to <c>ViewerQueuePackets</c>
    /// packets into its socket before a single live packet arrives - two thousand demultiplexed
    /// frames, about twenty-three seconds of media and some thirty-six megabytes, well past the
    /// twelve-megabyte send buffer. So the buffer fills, and it fills against a perfectly ordinary
    /// peer: this one advertises too-late-packet drop, which is libsrt's default and what every real
    /// player does.
    ///
    /// What that produces is isolated timeouts. libsrt's <c>sndDropTooLate</c> runs on entry to
    /// <c>srt_sendmsg</c>, so a send that finds the buffer full waits out the poll, and the next
    /// entry frees whatever has since aged past the drop threshold and takes the bytes. A rule that
    /// counted timeouts, or dropped on the first one, would therefore drop every viewer that asked
    /// for a rollback - which is why the budget is wall-clock time with nothing accepted and why it
    /// resets on every accepted chunk. This is the test that fails if that ever changes.
    /// </summary>
    [Fact]
    public void Isolated_timeouts_seeding_a_rollback_never_add_up_to_the_budget()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);

        var port = SrtSenders.FreePort();

        Srt.EnsureStarted();

        var listener = Listening(port);

        try
        {
            using var peer = Calling(port, dropsLatePackets: true);

            var accepted = Accept(listener, port, SendTimeoutMilliseconds);
            var counting = new Counting(accepted);

            using var sender = Sending(accepted, counting);

            var burst = new byte[BurstBytes];

            Random.Shared.NextBytes(burst);

            var started = Stopwatch.StartNew();
            var longest = TimeSpan.Zero;

            for (var written = 0L; written < SeedBytes; written += burst.Length)
            {
                var at = started.Elapsed;

                // As fast as the muxer would: seeding a viewer's socket is not paced by anything,
                // which is the whole reason it reaches the buffer's ceiling at all.
                sender.Write(burst);

                var took = started.Elapsed - at;

                longest = took > longest ? took : longest;
            }

            Srt.Stats(accepted, out var last, clear: false);

            output.WriteLine(
                $"seeded {sender.Written / (1024 * 1024)} MB in {started.Elapsed.TotalSeconds:0.0} s, "
                + $"longest write {longest.TotalSeconds:0.00} s, {counting.Timeouts} sends timed out, "
                + $"longest run {counting.LongestRun}, longest stretch with nothing accepted "
                + $"{counting.LongestStall.TotalSeconds:0.00} s, {last.pktSndDrop} packets dropped by "
                + $"the sender, buffer holding {last.byteSndBuf / 1024} KB at {last.msSndBuf} ms");

            // The whole seed went in. Nothing here is about throughput: if the budget had fired, the
            // write would have thrown and this line would never be reached.
            Assert.Equal(SeedBytes, sender.Written);

            Assert.False(
                sender.Faulted,
                $"seeding {SeedBytes / (1024 * 1024)} MB into a peer that advertises packet drop "
                + $"faulted the stream after {counting.Timeouts} timeouts, so a viewer asking for a "
                + "rollback would be dropped for asking");

            // The measurement behind the assertion above, and the one that would move first if the
            // budget were shortened or the reset removed: no run of timeouts ever came close to it.
            //
            // This is deliberately not an assertion about how long a Write took. One Write of this
            // size is two hundred sends, and two of them timing out a second apart makes a two-second
            // call out of two one-second stalls with a hundred successful sends between them. The
            // clock is per chunk accepted rather than per call precisely so that reads as what it is.
            Assert.True(
                counting.LongestStall < Budget,
                $"a stretch of {counting.LongestStall.TotalSeconds:0.00} s passed with nothing "
                + $"accepted while seeding an ordinary peer, against a {Budget.TotalSeconds:0} s "
                + "budget, so the margin this rests on is gone");
        }
        finally
        {
            Srt.srt_close(listener);
        }
    }

    /// <summary>
    /// That retrying a timed-out send is lossless, which is the assumption the budget rests on and
    /// the one thing in it that is not a policy choice.
    ///
    /// <c>srt_sendmsg</c> returning SRT_ETIMEOUT has queued nothing - the message never entered the
    /// send buffer - so offering the same bytes again offers them once. If that were wrong, every
    /// stalled viewer that recovered would see a duplicated or reordered message, and the loop in
    /// <c>SrtSocketStream.Write</c> would be a corruption rather than a bound. So it is counted here
    /// rather than reasoned about: a numbered sequence through a peer that cleared its drop flag - so
    /// neither end may discard anything, and what arrives is exactly what was sent - reading in
    /// bursts, so the sender stalls, times out and retries repeatedly on the way through.
    ///
    /// The poll is shorter here than the service's second, because the subject is the retry rather
    /// than the bound: a shorter poll makes more retries happen inside a test that has to finish.
    /// </summary>
    [Fact]
    public void A_send_that_timed_out_and_was_retried_loses_nothing_and_reorders_nothing()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);

        var port = SrtSenders.FreePort();

        Srt.EnsureStarted();

        var listener = Listening(port);

        try
        {
            using var peer = Calling(port, dropsLatePackets: false);

            var accepted = Accept(listener, port, RetryTimeoutMilliseconds);
            var counting = new Counting(accepted);

            using var sender = Sending(accepted, counting);

            using var draining = new Draining(peer.Socket, sender.PayloadSize, Messages);

            var message = new byte[sender.PayloadSize];

            Random.Shared.NextBytes(message);

            var started = Stopwatch.StartNew();

            for (var number = 0; number < Messages; number++)
            {
                // One Write of exactly the payload size is one libsrt message, so the number in the
                // first eight bytes is what the far end reads back as that message's identity.
                BitConverter.TryWriteBytes(message.AsSpan(), (long)number);

                sender.Write(message);
            }

            var drained = draining.Wait(Drained);

            output.WriteLine(
                $"sent {Messages} messages of {sender.PayloadSize} B in {started.Elapsed.TotalSeconds:0.0} s "
                + $"with a {RetryTimeoutMilliseconds} ms poll; {counting.Timeouts} sends timed out and "
                + $"were retried, longest run {counting.LongestRun}, longest stretch with nothing "
                + $"accepted {counting.LongestStall.TotalSeconds:0.00} s; received {draining.Received}, "
                + $"{draining.Missing} missing, {draining.Duplicated} duplicated, "
                + $"{draining.OutOfOrder} out of order");

            Assert.True(
                drained,
                $"the peer received {draining.Received} of {Messages} within {Drained.TotalSeconds:0} s");

            // The test proves nothing at all unless sends actually timed out on the way through, so
            // that is asserted rather than assumed. Measured at 23 of 40,000 with this poll, every
            // one of them an isolated timeout: longest run 1, longest stretch with nothing accepted
            // 0.30 s, which is the poll.
            Assert.True(
                counting.Timeouts > 0,
                "no send timed out, so nothing was retried and this measured an ordinary transfer");

            Assert.Equal(Messages, draining.Received);
            Assert.Equal(0, draining.Missing);
            Assert.Equal(0, draining.Duplicated);
            Assert.Equal(0, draining.OutOfOrder);

            Assert.False(sender.Faulted, "the stream faulted, so the sequence above is incomplete");
        }
        finally
        {
            Srt.srt_close(listener);
        }
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

            // Exactly what an accepted viewer gets, because the object under measurement is the one
            // the consumption port writes viewers through - SrtListener's send timeout on the socket,
            // LiveOptions' budget on the stream.
            var accepted = Accept(listener, port, SendTimeoutMilliseconds);

            using var sender = new SrtSocketStream(accepted, writable: true, sendStallBudget: Budget);

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

                // Ends early once the budget has dropped the peer, because everything after that is
                // a closed stream being sampled: the figures are taken at the moment it gave up.
                while (started.Elapsed < Watched && !pushing.GaveUp)
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

                output.WriteLine(
                    sender.Faulted
                        ? $"the stream faulted, {(sender.SendStalled ? "on the no-progress budget" : "on a refused send")}"
                        : "the stream never faulted");

                // Read before the finally below closes the socket under the writer, which would
                // fault the stream itself and make every case look like the blocked one.
                return new Pressed(
                    last.pktSndDrop,
                    stalled,
                    pushing.Written,
                    pushing.Longest,
                    sender.Faulted,
                    sender.SendStalled);
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
    /// <param name="Longest">
    /// The longest a single send took to come back, whether it came back with the bytes taken or with
    /// the budget spent. Sampling answers how long a send has been waiting; only this answers how
    /// long one waited in the end, which is what a bound is a statement about.
    /// </param>
    /// <param name="Faulted">Whether the stream ended the window unusable.</param>
    /// <param name="SendStalled">Whether the budget was what made it unusable.</param>
    private readonly record struct Pressed(
        int Drops,
        TimeSpan Stalled,
        long Written,
        TimeSpan Longest,
        bool Faulted,
        bool SendStalled);

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
        /// <summary>The raw socket, for the one case that reads from the peer rather than ignoring it.</summary>
        public int Socket => socket;

        public void Dispose() => Srt.srt_close(socket);
    }

    /// <summary>
    /// Accepts the caller and configures the socket the way <c>SrtListener.Handle</c> configures an
    /// accepted viewer, with the send timeout as the one thing a test may vary.
    /// </summary>
    private static int Accept(int listener, int port, int sendTimeoutMilliseconds)
    {
        var accepted = Srt.srt_accept(listener, null, null);

        Assert.True(
            accepted != Srt.SRT_INVALID_SOCK,
            $"nothing was accepted on {port}: {Srt.LastError()}");

        Assert.True(
            Srt.SetInt32(accepted, SRT_SOCKOPT.SRTO_SNDTIMEO, sendTimeoutMilliseconds),
            $"could not set the send timeout the listener sets: {Srt.LastError()}");

        return accepted;
    }

    /// <summary>
    /// The viewer's stream, with the budget the consumption port gives one and every send counted on
    /// the way past. The counting goes through <see cref="SrtSocketStream"/>'s own test seam rather
    /// than around it, so what is counted is exactly what the retry loop saw.
    /// </summary>
    private static SrtSocketStream Sending(int accepted, Counting counting)
        => new(
            accepted,
            writable: true,
            counting.Send,
            receive: null,
            payloadSize: Srt.LiveDefaultPayloadSize,
            sendStallBudget: Budget);

    /// <summary>
    /// <c>srt_sendmsg</c>, with a record of what timed out. Called only from the thread doing the
    /// writing, and read only once that thread has finished, so nothing here is synchronised.
    /// </summary>
    private sealed unsafe class Counting(int socket)
    {
        private long _stalledAt;

        private int _run;

        /// <summary>Sends that came back SRT_ETIMEOUT and were offered again.</summary>
        public int Timeouts { get; private set; }

        /// <summary>The most consecutive timeouts without a chunk being accepted in between.</summary>
        public int LongestRun { get; private set; }

        /// <summary>
        /// The longest stretch of wall clock in which libsrt accepted nothing, which is the figure
        /// the budget is a bound on - the run length is not, since it is the timeout value in
        /// disguise.
        /// </summary>
        public TimeSpan LongestStall { get; private set; }

        public int Send(ReadOnlySpan<byte> chunk)
        {
            var offeredAt = _run == 0 ? Stopwatch.GetTimestamp() : _stalledAt;

            fixed (byte* data = chunk)
            {
                var sent = Srt.srt_sendmsg(socket, data, chunk.Length, -1, 0);

                if (sent >= 0)
                {
                    _run = 0;

                    return sent;
                }

                if (Srt.LastErrorCode() != Srt.SRT_ETIMEOUT)
                {
                    return sent;
                }

                _stalledAt = offeredAt;
                _run++;
                Timeouts++;

                LongestRun = Math.Max(LongestRun, _run);

                var stalled = Stopwatch.GetElapsedTime(_stalledAt);

                LongestStall = stalled > LongestStall ? stalled : LongestStall;

                return sent;
            }
        }
    }

    /// <summary>
    /// The peer for the retry case: it reads, but in bursts with a pause between them, which is what
    /// fills the sender's buffer and makes its sends time out. Every message is checked back against
    /// the number written into it, so what this reports is loss, duplication and reordering across
    /// exactly those retries.
    /// </summary>
    private sealed unsafe class Draining : IDisposable
    {
        /// <summary>
        /// How many messages are taken before pausing, and for how long. The pause is longer than the
        /// sender's poll on purpose, and that is the whole of what makes this case what it claims to
        /// be: with a pause shorter than the poll the sender is still blocked for most of the
        /// transfer - measured at sixteen seconds for forty thousand messages - but every blocked
        /// send is woken by the next burst before its timeout, so nothing times out and nothing is
        /// retried. Back-pressure is not the same event as a timeout, and only the timeout is on
        /// trial here.
        /// </summary>
        private const int Burst = 1000;

        private static readonly TimeSpan Between = TimeSpan.FromMilliseconds(500);

        private readonly Thread _thread;

        private readonly bool[] _seen;

        private readonly ManualResetEventSlim _drained = new(false);

        public Draining(int socket, int payloadSize, int expected)
        {
            _seen = new bool[expected];

            // So a peer that stops hearing anything can leave rather than sit in srt_recvmsg for the
            // rest of the run.
            Srt.SetInt32(socket, SRT_SOCKOPT.SRTO_RCVTIMEO, 1000);

            _thread = new Thread(() => Drain(socket, payloadSize))
            {
                IsBackground = true,
                Name = "burst reader",
            };

            _thread.Start();
        }

        public int Received { get; private set; }

        public int Duplicated { get; private set; }

        public int OutOfOrder { get; private set; }

        public int Missing => _seen.Count(seen => !seen);

        /// <summary>True when every message arrived inside the deadline.</summary>
        public bool Wait(TimeSpan within) => _drained.Wait(within);

        public void Dispose()
        {
            _thread.Join(TimeSpan.FromSeconds(5));

            _drained.Dispose();
        }

        private void Drain(int socket, int payloadSize)
        {
            var buffer = new byte[payloadSize];
            var highest = -1L;

            try
            {
                while (Received < _seen.Length)
                {
                    for (var taken = 0; taken < Burst && Received < _seen.Length; taken++)
                    {
                        int read;

                        fixed (byte* data = buffer)
                        {
                            read = Srt.srt_recvmsg(socket, data, buffer.Length);
                        }

                        if (read < 0)
                        {
                            if (Srt.LastErrorCode() == Srt.SRT_ETIMEOUT)
                            {
                                continue;
                            }

                            // The socket went. Whatever has arrived is what the assertions see.
                            return;
                        }

                        if (read == 0)
                        {
                            return;
                        }

                        var number = BitConverter.ToInt64(buffer);

                        if (number < 0 || number >= _seen.Length)
                        {
                            continue;
                        }

                        if (_seen[number])
                        {
                            Duplicated++;
                        }
                        else
                        {
                            _seen[number] = true;
                            Received++;
                        }

                        if (number <= highest)
                        {
                            OutOfOrder++;
                        }

                        highest = Math.Max(highest, number);
                    }

                    // The pause is the point: it is what lets the sender's buffer fill behind a peer
                    // that is reading, which an evenly-paced reader never does.
                    Thread.Sleep(Between);
                }

                _drained.Set();
            }
            catch (Exception)
            {
                // The socket was closed under the read at the end of the case.
            }
        }
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

        private volatile bool _gaveUp;

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

        /// <summary>
        /// Whether a write threw. Against a peer that reads nothing that is the budget dropping it,
        /// and it is also how the loop ends when the socket is closed under it at the end of the
        /// window - which is why the caller reads it only while the window is still open.
        /// </summary>
        public bool GaveUp => _gaveUp;

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
                // Either the budget gave up on the peer, or the socket was closed under the write at
                // the end of the window. The send that threw is timed like any other: for the first
                // case it is the whole measurement, and a figure taken only from sends that returned
                // successfully would miss it entirely.
                var took = InFlight;

                if (took.Ticks > Interlocked.Read(ref _longestTicks))
                {
                    Interlocked.Exchange(ref _longestTicks, took.Ticks);
                }

                _gaveUp = true;
            }
        }
    }
}
