using System.Net;
using System.Net.Sockets;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Coordination;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Application;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>Forwarding, in the two halves it actually has.</summary>
public sealed class StreamForwarderTests
{
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(5);

    private const string NoLibsrt = "libsrt is not installed. Run scripts/fetch-libsrt.sh.";

    private static readonly DateTimeOffset Now = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    /// <summary>The smallest layout a muxer will accept, built without a real transport.</summary>
    private static unsafe StreamLayout OneVideoStream()
    {
        var format = ffmpeg.avformat_alloc_context();

        try
        {
            var stream = ffmpeg.avformat_new_stream(format, null);

            stream->time_base = new AVRational { num = 1, den = 90000 };
            stream->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_VIDEO;
            stream->codecpar->codec_id = AVCodecID.AV_CODEC_ID_MPEG2VIDEO;
            stream->codecpar->width = 320;
            stream->codecpar->height = 240;

            return StreamLayout.From(format);
        }
        finally
        {
            ffmpeg.avformat_free_context(format);
        }
    }

    /// <summary>
    /// The whole path, with real bytes: packets into a hub, a forward pointed at a socket this test
    /// holds, and a transport stream coming out of it.
    /// </summary>
    [Fact]
    public async Task A_forward_pushes_a_transport_stream_to_the_far_end()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();

        using var hub = new StreamHub("forwarded-camera", options, NullLogger.Instance);
        using var layout = OneVideoStream();

        hub.Adopt(layout);

        var port = SrtSenders.FreePort();

        using var far = new UdpClient(new IPEndPoint(IPAddress.Loopback, port));

        using var forwarder = new StreamForwarder(
            "forward-1",
            $"udp://127.0.0.1:{port}",
            hub,
            options,
            NullLogger.Instance);

        forwarder.Start(CancellationToken.None);

        // Published in a loop rather than all at once, because the forward subscribes at the live
        // edge: anything sent before its thread has attached is gone, and the thread has a socket
        // to open first.
        var received = far.ReceiveAsync(CancellationToken.None).AsTask();
        var pts = 0L;

        for (var burst = 0; burst < 100 && !received.IsCompleted; burst++)
        {
            for (var step = 0; step < 10; step++)
            {
                hub.Publish(
                    new MediaPacket(0, new byte[1400], pts, pts, Duration: 3600, IsKeyframe: step == 0),
                    pts);

                pts += 3600;
            }

            await Task.Delay(50);
        }

        Assert.True(
            received.IsCompleted,
            $"nothing reached the far end in five seconds; the forward said: {forwarder.Error}");

        var datagram = (await received).Buffer;

        Assert.NotEmpty(datagram);
        Assert.Equal(0x47, datagram[0]);

        Assert.True(forwarder.Connected, $"the forward reported itself disconnected: {forwarder.Error}");
        Assert.True(forwarder.Bytes > 0, "the forward counted no bytes");
        Assert.NotNull(forwarder.ConnectedAt);

        var status = forwarder.Status;

        Assert.Equal("forward-1", status.Id);
        Assert.True(status.Connected);
        Assert.True(status.Bytes > 0);
    }

    /// <summary>
    /// An SRT forward, dialling out as a caller, through the real direct-libsrt path rather than
    /// libav's: real bytes reach a real far end, exactly as the UDP test above proves.
    /// </summary>
    [Fact]
    public async Task An_srt_forward_dials_out_and_reports_libsrts_own_link_stats()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();

        using var hub = new StreamHub("forwarded-camera-srt-caller", options, NullLogger.Instance);
        using var layout = OneVideoStream();

        hub.Adopt(layout);

        var port = SrtSenders.FreePort();

        using var listener = new RawSrtListener(port);

        using var forwarder = new StreamForwarder(
            "forward-srt-caller",
            $"srt://127.0.0.1:{port}?streamid=forwarded-camera-srt-caller&latency=120",
            hub,
            options,
            NullLogger.Instance);

        forwarder.Start(CancellationToken.None);

        // Kept running rather than sent once, so there is still a live connection by the time the
        // assertions below ask libsrt for a sample - a caller dialling a listener that has not
        // finished accepting is the ordinary race this loop already exists to lose gracefully.
        var pts = 0L;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);

        while (DateTime.UtcNow < deadline && listener.Received == 0)
        {
            for (var step = 0; step < 10; step++)
            {
                hub.Publish(
                    new MediaPacket(0, new byte[1400], pts, pts, Duration: 3600, IsKeyframe: step == 0),
                    pts);

                pts += 3600;
            }

            await Task.Delay(50);
        }

        Assert.True(
            listener.Received > 0,
            $"nothing reached the far end in twenty seconds; the forward said: {forwarder.Error}, "
                + $"the listener said: {listener.Error}");

        Assert.Equal(0x47, listener.FirstByte);
        Assert.True(forwarder.Connected, $"the forward reported itself disconnected: {forwarder.Error}");

        // One more beat so a real srt_bstats sample has something in its window: the very first
        // read after a handshake is legitimately still mostly zero, and asking before the transport
        // has been up for one would make this test flaky rather than prove anything.
        await Task.Delay(TimeSpan.FromSeconds(2));

        for (var step = 0; step < 10; step++)
        {
            hub.Publish(new MediaPacket(0, new byte[1400], pts, pts, Duration: 3600, IsKeyframe: step == 0), pts);
            pts += 3600;
        }

        await Task.Delay(TimeSpan.FromSeconds(1));

        var status = forwarder.Status;

        Assert.Equal(0, status.PacketsLost);
        Assert.Equal(0, status.PacketsDropped);
        Assert.NotNull(status.Link);

        Assert.True(
            status.Link!.BandwidthMbps > 0,
            $"bandwidth read as {status.Link.BandwidthMbps}, which is not a real libsrt estimate");

        Assert.True(status.Link.NegotiatedLatencyMs >= 120, "the negotiated latency was below what this forward asked for");
    }

    /// <summary>
    /// The other shape a forward's SRT URL can ask for: this replica waits, and the far end pulls.
    /// </summary>
    [Fact]
    public async Task An_srt_forward_can_wait_for_the_far_end_to_pull_it()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();

        using var hub = new StreamHub("forwarded-camera-srt-listener", options, NullLogger.Instance);
        using var layout = OneVideoStream();

        hub.Adopt(layout);

        var port = SrtSenders.FreePort();

        using var forwarder = new StreamForwarder(
            "forward-srt-listener",
            $"srt://0.0.0.0:{port}?mode=listener",
            hub,
            options,
            NullLogger.Instance);

        forwarder.Start(CancellationToken.None);

        // Given a moment to reach srt_listen before anything tries to connect - the accept below
        // this comment has no retry of its own, unlike a caller's handshake.
        await Task.Delay(TimeSpan.FromSeconds(1));

        using var puller = new RawSrtCaller(port);

        var pts = 0L;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);

        while (DateTime.UtcNow < deadline && puller.Received == 0)
        {
            for (var step = 0; step < 10; step++)
            {
                hub.Publish(
                    new MediaPacket(0, new byte[1400], pts, pts, Duration: 3600, IsKeyframe: step == 0),
                    pts);

                pts += 3600;
            }

            await Task.Delay(50);
        }

        Assert.True(
            puller.Received > 0,
            $"nothing reached the far end in twenty seconds; the forward said: {forwarder.Error}, "
                + $"the caller said: {puller.Error}");

        Assert.Equal(0x47, puller.FirstByte);

        // Stop() has to unblock a listener parked in srt_accept with nobody having pulled it yet in
        // the ordinary case; here somebody already has, so this instead proves the running thread
        // actually exits once the accepted connection is asked to close, rather than hanging on a
        // send to a peer this test is about to walk away from.
        forwarder.Stop();

        try
        {
            await forwarder.Running!.WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException)
        {
            Assert.Fail("the forward's thread never exited after Stop()");
        }
    }

    /// <summary>
    /// The far end for a listening forward's test: a raw libsrt caller, the mirror of <see
    /// cref="RawSrtListener"/> for the other direction a forward's URL can ask for.
    /// </summary>
    private sealed unsafe class RawSrtCaller : IDisposable
    {
        private readonly int _socket;
        private readonly Task _read;

        public int Received { get; private set; }

        public byte FirstByte { get; private set; }

        public string? Error { get; private set; }

        public RawSrtCaller(int port)
        {
            Srt.EnsureStarted();

            _socket = Srt.srt_create_socket();

            var address = new IPEndPoint(IPAddress.Loopback, port).Serialize();

            fixed (byte* raw = address.Buffer.Span)
            {
                if (Srt.srt_connect(_socket, raw, address.Size) != 0)
                {
                    Error = Srt.LastError();
                    _read = Task.CompletedTask;

                    return;
                }
            }

            _read = Task.Factory.StartNew(Drain, TaskCreationOptions.LongRunning);
        }

        private void Drain()
        {
            using var transport = new SrtSocketStream(_socket, writable: false);
            var buffer = new byte[transport.PayloadSize];

            while (true)
            {
                int read;

                try
                {
                    read = transport.Read(buffer);
                }
                catch (Exception ex)
                {
                    Error ??= ex.Message;

                    return;
                }

                if (read <= 0)
                {
                    return;
                }

                if (Received == 0)
                {
                    FirstByte = buffer[0];
                }

                Received += read;
            }
        }

        public void Dispose() => _read.Wait(TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// The far end for an SRT caller test, standing in for a real gateway with the same primitives
    /// <see cref="SrtEgress"/> uses to open one: bind, listen, accept, all blocking on a background
    /// thread since that is the one thing every libsrt call in this service assumes about its
    /// caller.
    /// </summary>
    private sealed unsafe class RawSrtListener : IDisposable
    {
        private readonly int _socket;
        private readonly Task _accept;

        public int Received { get; private set; }

        public byte FirstByte { get; private set; }

        public string? Error { get; private set; }

        public RawSrtListener(int port)
        {
            Srt.EnsureStarted();

            _socket = Srt.srt_create_socket();
            Srt.SetBool(_socket, SRT_SOCKOPT.SRTO_REUSEADDR, true);

            var address = new IPEndPoint(IPAddress.Loopback, port).Serialize();

            fixed (byte* raw = address.Buffer.Span)
            {
                if (Srt.srt_bind(_socket, raw, address.Size) != 0)
                {
                    throw new InvalidOperationException($"Could not bind the test listener: {Srt.LastError()}");
                }
            }

            if (Srt.srt_listen(_socket, 1) != 0)
            {
                throw new InvalidOperationException($"Could not listen on the test listener: {Srt.LastError()}");
            }

            _accept = Task.Factory.StartNew(Accept, TaskCreationOptions.LongRunning);
        }

        private void Accept()
        {
            var accepted = Srt.srt_accept(_socket, null, null);

            if (accepted == Srt.SRT_INVALID_SOCK)
            {
                Error = Srt.LastError();

                return;
            }

            // Drained for as long as the sender keeps the connection, not read once and closed: a
            // far end that hangs up after one message would fault the forward's own socket well
            // before the test gets to ask it for a link sample, which is the whole second half of
            // what this test proves.
            using var transport = new SrtSocketStream(accepted, writable: false);
            var buffer = new byte[transport.PayloadSize];

            while (true)
            {
                var read = transport.Read(buffer);

                if (read <= 0)
                {
                    return;
                }

                if (Received == 0)
                {
                    FirstByte = buffer[0];
                }

                Received += read;
            }
        }

        public void Dispose()
        {
            Srt.srt_close(_socket);
            _accept.Wait(TimeSpan.FromSeconds(5));
        }
    }

    /// <summary>
    /// A forward whose far end cannot be opened stops and says why, rather than retrying on its
    /// own.
    /// </summary>
    [Fact]
    public async Task A_forward_that_cannot_open_stops_and_leaves_the_reason()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();

        using var hub = new StreamHub("unopenable", options, NullLogger.Instance);
        using var layout = OneVideoStream();

        hub.Adopt(layout);

        // A scheme libav has no output protocol for, so the open fails rather than hanging on a
        // handshake this test would then have to wait out.
        using var forwarder = new StreamForwarder(
            "forward-1",
            "nonsense://127.0.0.1:1/x",
            hub,
            options,
            NullLogger.Instance);

        forwarder.Start(CancellationToken.None);

        await SrtSenders.WaitUntilAsync(
            () => forwarder.Finished,
            TimeSpan.FromSeconds(10),
            () => "the forward never gave up on a URL it cannot open");

        Assert.False(forwarder.Connected);
        Assert.NotNull(forwarder.Error);
        Assert.Contains("nonsense", forwarder.Error);
    }

    [Fact]
    public void A_configured_forward_that_is_not_running_is_started()
    {
        var (start, stop) = ForwardPlan.Decide(
            [new ForwardTarget("a", "udp://far:5000")],
            new Dictionary<string, RunningForward>(),
            Now,
            Retry);

        Assert.Equal("a", Assert.Single(start).Id);
        Assert.Empty(stop);
    }

    [Fact]
    public void A_forward_that_was_removed_from_the_source_is_stopped()
    {
        var (start, stop) = ForwardPlan.Decide(
            [],
            Running(("a", "udp://far:5000", false)),
            Now,
            Retry);

        Assert.Empty(start);
        Assert.Equal("a", Assert.Single(stop));
    }

    /// <summary>Disabling is the same decision as removing, from here.</summary>
    [Fact]
    public void A_forward_that_was_disabled_is_stopped()
    {
        var source = new LiveSource(
            "camera",
            null,
            Enabled: true,
            [new ForwardTarget("a", "udp://far:5000", Enabled: false)],
            Now);

        var (start, stop) = ForwardPlan.Decide(
            [.. source.Forwards.Where(target => target.Enabled)],
            Running(("a", "udp://far:5000", false)),
            Now,
            Retry);

        Assert.Empty(start);
        Assert.Equal("a", Assert.Single(stop));
    }

    /// <summary>
    /// An edited URL is a redial on the same forward, not a forward disappearing and another
    /// appearing, which is the whole reason the id is stable across edits.
    /// </summary>
    [Fact]
    public void A_forward_whose_url_changed_is_stopped_and_started_again()
    {
        var (start, stop) = ForwardPlan.Decide(
            [new ForwardTarget("a", "udp://elsewhere:5000")],
            Running(("a", "udp://far:5000", false)),
            Now,
            Retry);

        Assert.Equal("udp://elsewhere:5000", Assert.Single(start).Url);
        Assert.Equal("a", Assert.Single(stop));
    }

    [Fact]
    public void A_running_forward_is_left_alone()
    {
        var (start, stop) = ForwardPlan.Decide(
            [new ForwardTarget("a", "udp://far:5000")],
            Running(("a", "udp://far:5000", false)),
            Now,
            Retry);

        Assert.Empty(start);
        Assert.Empty(stop);
    }

    /// <summary>The back-off.</summary>
    [Fact]
    public void A_failed_forward_is_left_alone_until_its_retry_is_due()
    {
        var running = new Dictionary<string, RunningForward>(StringComparer.Ordinal)
        {
            ["a"] = new("udp://far:5000", Finished: true, LastAttemptAt: Now - TimeSpan.FromSeconds(2)),
        };

        var (start, stop) = ForwardPlan.Decide([new ForwardTarget("a", "udp://far:5000")], running, Now, Retry);

        Assert.Empty(start);
        Assert.Empty(stop);
    }

    [Fact]
    public void A_failed_forward_is_started_again_once_its_retry_is_due()
    {
        var running = new Dictionary<string, RunningForward>(StringComparer.Ordinal)
        {
            ["a"] = new("udp://far:5000", Finished: true, LastAttemptAt: Now - TimeSpan.FromSeconds(30)),
        };

        var (start, stop) = ForwardPlan.Decide([new ForwardTarget("a", "udp://far:5000")], running, Now, Retry);

        Assert.Equal("a", Assert.Single(start).Id);
        Assert.Equal("a", Assert.Single(stop));
    }

    /// <summary>
    /// A forward that failed and has been disabled since is stopped and not retried, which is the
    /// case that would go wrong if the forwarder retried itself: it has no idea the operator has
    /// changed their mind.
    /// </summary>
    [Fact]
    public void A_failed_forward_that_was_disabled_is_not_retried()
    {
        var running = new Dictionary<string, RunningForward>(StringComparer.Ordinal)
        {
            ["a"] = new("udp://far:5000", Finished: true, LastAttemptAt: Now - TimeSpan.FromSeconds(30)),
        };

        var (start, stop) = ForwardPlan.Decide([], running, Now, Retry);

        Assert.Empty(start);
        Assert.Equal("a", Assert.Single(stop));
    }

    private static Dictionary<string, RunningForward> Running(params (string Id, string Url, bool Finished)[] forwards)
        => forwards.ToDictionary(
            forward => forward.Id,
            forward => new RunningForward(forward.Url, forward.Finished, Now),
            StringComparer.Ordinal);
}

/// <summary>The allowlist, on the output side.</summary>
public sealed class ForwardTargetAllowlistTests : IAsyncDisposable
{
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    private LiveStreamCoordinator? _coordinator;

    [Fact]
    public async Task A_file_forward_target_is_refused_and_the_reason_is_on_the_stream()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        const string name = "allowlisted-camera";

        var (coordinator, sources) = Coordinator();

        await coordinator.CreateManualAsync(name, $"udp://127.0.0.1:{SrtSenders.FreePort()}");

        sources.Save(new LiveSource(
            name,
            null,
            Enabled: true,
            [new ForwardTarget("leak", "file:///tmp/somewhere-it-should-not-go.ts")],
            DateTimeOffset.UtcNow));

        await coordinator.TickAsync(CancellationToken.None);

        var stream = await coordinator.GetAsync(name);

        Assert.NotNull(stream);
        Assert.NotNull(stream.Forwards);

        var forward = Assert.Single(stream.Forwards);

        Assert.False(forward.Connected);
        Assert.Equal(0, forward.Bytes);
        Assert.NotNull(forward.Error);
        Assert.Contains("file", forward.Error);
    }

    /// <summary>Parking a source stops this service dialling out.</summary>
    [Fact]
    public async Task A_pulled_stream_stops_when_its_source_is_switched_off()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        const string name = "parked-camera";

        var (coordinator, sources) = Coordinator();

        await coordinator.CreateManualAsync(name, $"udp://127.0.0.1:{SrtSenders.FreePort()}");

        sources.Save(new LiveSource(name, "udp://127.0.0.1:1", Enabled: true, [], DateTimeOffset.UtcNow));
        await coordinator.TickAsync(CancellationToken.None);

        Assert.True(coordinator.Owns(name));

        sources.Save(new LiveSource(name, "udp://127.0.0.1:1", Enabled: false, [], DateTimeOffset.UtcNow));
        await coordinator.TickAsync(CancellationToken.None);

        Assert.False(coordinator.Owns(name));
        Assert.Null(await coordinator.GetAsync(name));
    }

    /// <summary>Deliberately narrow, in the direction that matters: an absent row sweeps nothing.</summary>
    [Fact]
    public async Task A_pulled_stream_with_no_configured_row_is_left_alone()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        const string name = "unconfigured-camera";

        var (coordinator, _) = Coordinator();

        await coordinator.CreateManualAsync(name, $"udp://127.0.0.1:{SrtSenders.FreePort()}");

        await coordinator.TickAsync(CancellationToken.None);

        Assert.True(coordinator.Owns(name));
    }

    /// <summary>A real coordinator over fakes, kept for disposal.</summary>
    private (LiveStreamCoordinator Coordinator, FakeLiveSourceStore Sources) Coordinator()
    {
        var sources = new FakeLiveSourceStore();

        var options = Options.Create(new LiveOptions
        {
            NodeName = "pod-a",
            // A second rather than thirty, so the pulled input a stream needs in order to exist
            // gives up promptly and the test is not held open by a port nothing is sending to.
            ManualInputOptions = new Dictionary<string, string> { ["timeout"] = "1000000" },
        });

        _coordinator = new LiveStreamCoordinator(
            new StreamDemuxer(options, NullLogger<StreamDemuxer>.Instance),
            new InMemoryLiveStreamRegistry(),
            sources,
            new InMemoryLock(),
            new FakeMediaAnalyzer(),
            _services.GetRequiredService<IServiceScopeFactory>(),
            options,
            Options.Create(new MediaOptions()),
            new LiveMetrics(),
            NullLogger<LiveStreamCoordinator>.Instance);

        return (_coordinator, sources);
    }

    public async ValueTask DisposeAsync()
    {
        if (_coordinator is not null)
        {
            await _coordinator.DisposeAsync();
        }

        await _services.DisposeAsync();
    }
}

/// <summary>The configuration half, with no store behind it.</summary>
internal sealed class FakeLiveSourceStore : ILiveSourceStore
{
    private readonly Dictionary<string, LiveSource> _sources = new(StringComparer.Ordinal);

    public void Save(LiveSource source) => _sources[source.Name] = source;

    public Task<IReadOnlyList<LiveSource>> ListAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<LiveSource>>([.. _sources.Values]);

    public Task<LiveSource?> GetAsync(string name, CancellationToken cancellationToken = default)
        => Task.FromResult(_sources.GetValueOrDefault(name));

    public Task SaveAsync(LiveSource source, CancellationToken cancellationToken = default)
    {
        Save(source);

        return Task.CompletedTask;
    }

    public Task RemoveAsync(string name, CancellationToken cancellationToken = default)
    {
        _sources.Remove(name);

        return Task.CompletedTask;
    }
}
