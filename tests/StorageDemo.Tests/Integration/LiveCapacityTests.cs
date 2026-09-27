using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>What a full replica does, through the real application with real encoders.</summary>
public sealed class LiveCapacityTests : IAsyncLifetime
{
    private readonly LiveReplicas _replicas = new();

    private int _ingest;

    public ValueTask InitializeAsync()
    {
        _ingest = SrtSenders.FreePort();

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => _replicas.DisposeAsync();

    /// <summary>The refusal, and that it is the overload refusal rather than any other.</summary>
    [Fact]
    public async Task A_full_pod_refuses_a_new_name_at_the_handshake()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        var host = _replicas.Start("pod-a", _ingest, maxStreams: 1);

        // Before the refusal it is measuring: a counter is an event, and a listener started
        // afterwards sees nothing of it.
        using var meters = new Meters(host.Services.GetRequiredService<LiveMetrics>());

        _replicas.Send(_ingest, "first-camera");

        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync("first-camera") is { State: LiveStreamState.Live },
            TimeSpan.FromSeconds(40),
            "the first camera never went live");

        var second = _replicas.Send(_ingest, "second-camera");

        Assert.True(
            await SrtSenders.WasRefused(second, TimeSpan.FromSeconds(10)),
            $"a full pod admitted a second name: {SrtSenders.Complaints([second])}");

        // Refused at the handshake means nothing was ever claimed.
        Assert.Null(await _replicas.Registry.GetAsync("second-camera"));

        var reject = Assert.Single(meters.Read(), measurement => measurement.Instrument == "live.rejects");

        Assert.Equal(
            [
                new KeyValuePair<string, object?>("port", _ingest),
                new KeyValuePair<string, object?>("reason", "overload"),
            ],
            reject.Tags);
    }

    /// <summary>The exception that matters more than the rule.</summary>
    [Fact]
    public async Task A_full_pod_accepts_a_reconnect_of_a_name_it_already_owns()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "blinking-camera";

        // The production grace period, so the interrupted entry is still here when the encoder
        // comes back.
        _replicas.Start("pod-a", _ingest, graceSeconds: 30, maxStreams: 1);

        var first = _replicas.Send(_ingest, name);

        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync(name) is { State: LiveStreamState.Live, Packets: > 0 },
            TimeSpan.FromSeconds(40),
            "the camera never went live");

        var began = (await _replicas.Registry.GetAsync(name))!.StartedAt;

        SrtSenders.Kill(first);

        // The feed timeout declares it interrupted, and the same heartbeat pass refreshes the copy
        // of the registry the handshake reads, so this is also when the name lock lets go.
        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync(name) is { State: LiveStreamState.Interrupted },
            TimeSpan.FromSeconds(40),
            "the feed never went interrupted");

        var again = _replicas.Send(_ingest, name);

        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync(name) is { State: LiveStreamState.Live, Packets: > 0 },
            TimeSpan.FromSeconds(40),
            $"a full pod refused its own encoder reconnecting: {SrtSenders.Complaints([again])}");

        Assert.Equal(began, (await _replicas.Registry.GetAsync(name))!.StartedAt);
    }

    /// <summary>
    /// The number an autoscaler would be trusting, against the dictionary it is meant to describe.
    /// </summary>
    [Fact]
    public async Task The_owned_gauge_reports_what_the_coordinator_holds()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        var host = _replicas.Start("pod-a", _ingest);

        using var meters = new Meters(host.Services.GetRequiredService<LiveMetrics>());

        var sender = _replicas.Send(_ingest, "counted-camera");

        await LiveReplicas.Until(
            () => Task.FromResult(meters.Value("live.streams.owned") == 1),
            TimeSpan.FromSeconds(40),
            "the gauge never counted the stream the pod had taken");

        SrtSenders.Kill(sender);

        // Interrupted after the feed timeout, then dropped when the grace period expires.
        await LiveReplicas.Until(
            () => Task.FromResult(meters.Value("live.streams.owned") == 0),
            TimeSpan.FromSeconds(40),
            "the gauge still counted a stream the pod had let go of");
    }
}
