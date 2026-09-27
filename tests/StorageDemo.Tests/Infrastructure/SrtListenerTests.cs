using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>The listener this service owns, against real callers.</summary>
public sealed class SrtListenerTests(ITestOutputHelper output) : IDisposable
{
    private const string NoLibsrt = "libsrt is not installed. Run scripts/fetch-libsrt.sh.";

    private readonly List<Process> _callers = [];

    private readonly List<int> _held = [];

    public void Dispose()
    {
        foreach (var caller in _callers)
        {
            SrtSenders.Kill(caller);
        }

        foreach (var socket in _held)
        {
            Srt.srt_close(socket);
        }
    }

    /// <summary>The proof the whole phase exists for.</summary>
    [Fact]
    public async Task Twenty_senders_started_at_once_are_all_accepted_within_five_seconds()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var names = Enumerable.Range(1, 20).Select(index => $"cam-{index:00}").ToArray();
        var accepted = new ConcurrentDictionary<string, bool>();

        using var metrics = new LiveMetrics();

        // Before anything is accepted: a counter is an event, so a listener started afterwards sees
        // none of them.
        using var meters = new Meters(metrics);

        var listenPort = SrtSenders.FreePort();

        await ListeningAsync(
            Listener(socket => accepted[socket.Name] = true, metrics: metrics),
            listenPort,
            async port =>
            {
                // Nothing between them, which is the point.
                foreach (var name in names)
                {
                    Sender(port, name);
                }

                await SrtSenders.WaitUntilAsync(
                    () => accepted.Count == names.Length,
                    TimeSpan.FromSeconds(5),
                    () => $"only {accepted.Count} of {names.Length} senders were accepted on port {port} "
                        + $"({string.Join(", ", accepted.Keys.Order())}): {SrtSenders.Complaints(_callers)}");
            });

        Assert.Equal(names.Order(), accepted.Keys.Order());

        var counted = meters
            .Read()
            .Where(measurement => measurement.Instrument == "live.accepts")
            .ToArray();

        Assert.Equal(names.Length, counted.Sum(measurement => measurement.Value));
        Assert.All(
            counted,
            measurement => Assert.Equal(
                [new KeyValuePair<string, object?>("port", listenPort)],
                measurement.Tags));
    }

    /// <summary>
    /// The name comes off the socket now, not out of a log line, and nothing is installed to
    /// intercept it.
    /// </summary>
    [Fact]
    public async Task The_stream_identifier_is_read_off_the_accepted_socket_byte_for_byte()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var accepted = new TaskCompletionSource<(string Name, string StreamId)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await ListeningAsync(
            Listener(socket => accepted.TrySetResult((socket.Name, socket.StreamId))),
            SrtSenders.FreePort(),
            async port =>
            {
                Sender(port, "#!::r=live/cam-1,m=publish");

                await SrtSenders.WaitUntilAsync(
                    () => accepted.Task.IsCompleted,
                    TimeSpan.FromSeconds(15),
                    () => $"the sender was never accepted on port {port}: {SrtSenders.Complaints(_callers)}");
            });

        var (name, streamId) = await accepted.Task;

        Assert.Equal("live/cam-1", name);
        Assert.EndsWith("r=live/cam-1,m=publish", streamId, StringComparison.Ordinal);
    }

    /// <summary>
    /// The property Phases 1b, 4 and 5 are all built on: an answer of no that arrives before a
    /// connection exists.
    /// </summary>
    [Fact]
    public async Task A_sender_with_an_unparseable_name_is_rejected_during_the_handshake_and_not_after()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var accepted = new ConcurrentBag<string>();

        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);

        var listenPort = SrtSenders.FreePort();

        await ListeningAsync(
            Listener(socket => accepted.Add(socket.Name), metrics: metrics),
            listenPort,
            async port =>
            {
                var sender = Sender(port, "../../etc/passwd");

                var refused = await SrtSenders.WasRefused(sender);

                Assert.Empty(accepted);

                Assert.True(
                    refused,
                    $"the sender was not turned away quickly: {SrtSenders.Complaints(_callers)}");
            });

        // The other half of "an operator can find out why an encoder cannot connect".
        var reject = Assert.Single(
            meters.Read(),
            measurement => measurement.Instrument == "live.rejects");

        Assert.Equal(
            [
                new KeyValuePair<string, object?>("port", listenPort),
                new KeyValuePair<string, object?>("reason", "bad-request"),
            ],
            reject.Tags);
    }

    /// <summary>
    /// The one thing that cannot be reasoned about: that the fields this service reads out of
    /// <c>srt_bstats</c> are the fields libsrt wrote.
    /// </summary>
    [Fact]
    public async Task The_statistics_libsrt_writes_land_in_the_fields_this_service_reads()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var connected = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stats = default(SRT_TRACEBSTATS);

        await ListeningAsync(
            Listener(
                socket =>
                {
                    var connection = socket.Release();
                    _held.Add(connection);

                    connected.TrySetResult(connection);
                },
                latencyMs: 60),
            SrtSenders.FreePort(),
            async port =>
            {
                Sender(port, "live/cam-1");

                await SrtSenders.WaitUntilAsync(
                    () => connected.Task.IsCompleted,
                    TimeSpan.FromSeconds(15),
                    () => $"the sender was never accepted on port {port}: {SrtSenders.Complaints(_callers)}");

                var socket = await connected.Task;

                // Sampled once media has actually moved, so the receive counters mean something.
                await SrtSenders.WaitUntilAsync(
                    () => Srt.Stats(socket, out stats, clear: false) && stats.pktRecvTotal > 0,
                    TimeSpan.FromSeconds(15),
                    () => $"libsrt never reported a received packet: {SrtSenders.Complaints(_callers)}");
            });

        Assert.True(stats.msTimeStamp > 0, "the connection reported no elapsed time");
        Assert.Equal(1500, stats.byteMSS);

        Assert.Equal(
            Srt.GetInt32(await connected.Task, SRT_SOCKOPT.SRTO_RCVLATENCY),
            stats.msRcvTsbPdDelay);
    }

    /// <summary>
    /// A stream that is fine says so, which is the half of the health signal that has to be quiet
    /// or none of it means anything.
    /// </summary>
    [Fact]
    public async Task A_healthy_stream_reports_no_loss_and_no_drops()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var connected = new TaskCompletionSource<SrtSocketStream>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        (int Lost, int Dropped, SrtLinkStats Link)? health = null;

        await ListeningAsync(
            Listener(socket => connected.TrySetResult(new SrtSocketStream(socket.Release(), writable: false))),
            SrtSenders.FreePort(),
            async port =>
            {
                Sender(port, "live/cam-1");

                await SrtSenders.WaitUntilAsync(
                    () => connected.Task.IsCompleted,
                    TimeSpan.FromSeconds(15),
                    () => $"the sender was never accepted on port {port}: {SrtSenders.Complaints(_callers)}");

                using var transport = await connected.Task;
                using var draining = new CancellationTokenSource();

                var drain = Task.Factory.StartNew(
                    () => Drain(transport, draining.Token),
                    TaskCreationOptions.LongRunning);

                await Task.Delay(TimeSpan.FromSeconds(2));

                Assert.NotNull(transport.Health());

                await Task.Delay(TimeSpan.FromSeconds(2));

                health = transport.Health();

                await draining.CancelAsync();
                await drain;
            });

        Assert.Equal(0, health?.Lost);
        Assert.Equal(0, health?.Dropped);

        // The link stats are new alongside the loss/drop counts this test has always checked, and
        // the cheapest thing to pin about them here is that they arrived at all - libsrt's own
        // numbers (bandwidth, RTT, and the rest) vary with the machine running the test, so nothing
        // beyond presence belongs in an assertion.
        Assert.NotNull(health?.Link);
    }

    /// <summary>The same mechanism in both directions.</summary>
    [Fact]
    public async Task A_publisher_on_the_consumption_port_and_a_subscriber_on_the_ingest_port_are_both_rejected()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var accepted = new ConcurrentBag<string>();
        var ingestPort = SrtSenders.FreePort();

        await ListeningAsync(
            Listener(socket => accepted.Add(socket.Name)),
            ingestPort,
            async _ => await ListeningAsync(
                Listener(socket => accepted.Add(socket.Name), StreamIntent.Subscribe),
                SrtSenders.FreePort(),
                async consumptionPort =>
                {
                    var offering = Sender(consumptionPort, "#!::r=cam-1,m=publish");
                    var asking = Player(ingestPort, "#!::r=cam-1,m=request");

                    var refused = await SrtSenders.WasRefused(offering) && await SrtSenders.WasRefused(asking);

                    Assert.Empty(accepted);

                    Assert.True(
                        refused,
                        $"a caller going the wrong way was not turned away: {SrtSenders.Complaints(_callers)}");
                }));
    }

    /// <summary>
    /// The bug the old carousel opened the transport on a separate thread to avoid: one caller that
    /// says nothing holding up everybody behind it.
    /// </summary>
    [Fact]
    public async Task A_caller_that_never_sends_a_byte_does_not_block_the_next_accept()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var accepted = new ConcurrentDictionary<string, bool>();

        await ListeningAsync(
            Listener(socket =>
            {
                accepted[socket.Name] = true;

                // Held open and unread until the test is over.
                _held.Add(socket.Release());
            }),
            SrtSenders.FreePort(),
            async port =>
            {
                Player(port, "silent");

                await SrtSenders.WaitUntilAsync(
                    () => accepted.ContainsKey("silent"),
                    TimeSpan.FromSeconds(15),
                    () => $"the silent caller was never accepted: {SrtSenders.Complaints(_callers)}");

                Sender(port, "after-the-silent-one");

                await SrtSenders.WaitUntilAsync(
                    () => accepted.ContainsKey("after-the-silent-one"),
                    TimeSpan.FromSeconds(3),
                    () => "the silent caller blocked the next accept: " + SrtSenders.Complaints(_callers));
            });
    }

    /// <summary>What a shutdown depends on.</summary>
    [Fact]
    public async Task Closing_the_socket_from_another_thread_ends_a_blocked_read()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var accepted = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        await ListeningAsync(
            Listener(socket => accepted.TrySetResult(socket.Release()), StreamIntent.Subscribe),
            SrtSenders.FreePort(),
            async port =>
            {
                // A viewer sends nothing, so the read below has nothing to return and blocks.
                Player(port, "viewer-1");

                await SrtSenders.WaitUntilAsync(
                    () => accepted.Task.IsCompleted,
                    TimeSpan.FromSeconds(15),
                    () => $"the viewer was never accepted on port {port}: {SrtSenders.Complaints(_callers)}");

                using var stream = new SrtSocketStream(await accepted.Task, writable: false);

                var reading = Task.Run(() => stream.Read(new byte[4096], 0, 4096));

                // Long enough to be inside srt_recvmsg, and past the first receive timeout, so what
                // ends the read is the close and not a coincidence of timing.
                await Task.Delay(TimeSpan.FromMilliseconds(1500));

                Assert.False(reading.IsCompleted, "the read returned before anything closed the socket");

                stream.Dispose();

                var finished = await Task.WhenAny(reading, Task.Delay(TimeSpan.FromSeconds(2)));

                Assert.True(finished == reading, "a blocked read outlived the close by over two seconds");
                Assert.Equal(0, await reading);

                output.WriteLine(stream.Faulted
                    ? "srt_close unblocked the read: it came back as a socket error"
                    : "the receive timeout ended the read, not the close");
            });
    }

    /// <summary>
    /// What the listener asks for is what an accepted socket ends up with, as long as nobody at the
    /// far end asks for more.
    /// </summary>
    [Fact]
    public async Task The_configured_latency_is_what_an_accepted_socket_negotiates()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var negotiated = await NegotiatedAsync(
            StreamIntent.Publish,
            latencyMs: 60,
            SRT_SOCKOPT.SRTO_RCVLATENCY,
            port => Sender(port, "cam-1"));

        Assert.Equal(60, negotiated);
    }

    /// <summary>The direction of the negotiation, pinned so nobody "fixes" it later.</summary>
    [Fact]
    public async Task The_encoders_higher_latency_wins()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var negotiated = await NegotiatedAsync(
            StreamIntent.Publish,
            latencyMs: 60,
            SRT_SOCKOPT.SRTO_RCVLATENCY,
            port => Sender(port, "cam-1", "latency=200000"));

        Assert.Equal(200, negotiated);
    }

    /// <summary>The half a plain SRTO_RCVLATENCY would have missed.</summary>
    [Fact]
    public async Task The_consumption_side_inherits_the_latency_too()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var negotiated = await NegotiatedAsync(
            StreamIntent.Subscribe,
            latencyMs: 60,
            SRT_SOCKOPT.SRTO_PEERLATENCY,
            port => Player(port, "cam-1", "latency=20000"));

        Assert.Equal(60, negotiated);
    }

    /// <summary>
    /// Runs one caller against a listener at <paramref name="latencyMs"/> and answers with the
    /// option read off the accepted socket.
    /// </summary>
    private async Task<int> NegotiatedAsync(
        StreamIntent intent,
        int latencyMs,
        SRT_SOCKOPT option,
        Func<int, Process> caller)
    {
        var negotiated = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        await ListeningAsync(
            Listener(
                socket =>
                {
                    // Taken off the handler rather than read through it: an option is only worth
                    // anything on a live connection, and the handler closes what it is given.
                    var connection = socket.Release();
                    _held.Add(connection);

                    negotiated.TrySetResult(Srt.GetInt32(connection, option));
                },
                intent,
                latencyMs),
            SrtSenders.FreePort(),
            async port =>
            {
                caller(port);

                await SrtSenders.WaitUntilAsync(
                    () => negotiated.Task.IsCompleted,
                    TimeSpan.FromSeconds(15),
                    () => $"the caller was never accepted on port {port}: {SrtSenders.Complaints(_callers)}");
            });

        return await negotiated.Task;
    }

    private SrtListener Listener(
        Action<AcceptedSocket> onAccepted,
        StreamIntent intent = StreamIntent.Publish,
        int latencyMs = 120,
        LiveMetrics? metrics = null)
        => new(
            intent,
            new LiveOptions { SrtLatencyMs = latencyMs },
            _ => null,
            onAccepted,
            NullLogger<SrtListener>.Instance,
            listeners: null,
            metrics);

    /// <summary>
    /// Runs a listener on its own thread for as long as the body takes, and stops it afterwards
    /// whatever the body did.
    /// </summary>
    private static async Task ListeningAsync(SrtListener listener, int port, Func<int, Task> body)
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        var listening = Task.Factory.StartNew(
            () => listener.Run(port, stop.Token),
            TaskCreationOptions.LongRunning);

        try
        {
            await body(port);
        }
        finally
        {
            await stop.CancelAsync();
            await listening;
        }
    }

    /// <summary>Reads until cancelled, so the receiver is never why a packet went stale.</summary>
    private static void Drain(SrtSocketStream transport, CancellationToken cancellationToken)
    {
        var buffer = new byte[transport.PayloadSize];

        while (!cancellationToken.IsCancellationRequested && transport.Read(buffer) > 0)
        {
        }
    }

    private Process Sender(int port, string? streamId, string? callerOptions = null)
    {
        var caller = SrtSenders.StartSender(port, streamId, callerOptions);
        _callers.Add(caller);

        return caller;
    }

    private Process Player(int port, string streamId, string? callerOptions = null)
    {
        var caller = SrtSenders.StartPlayer(port, streamId, callerOptions);
        _callers.Add(caller);

        return caller;
    }
}
