using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>What this pod says about itself, as whatever is reading the meter would see it.</summary>
public sealed class LiveMetricsTests
{
    [Fact]
    public void The_owned_gauge_reports_what_the_heartbeat_last_told_it()
    {
        using var metrics = new LiveMetrics { StreamsOwned = 7 };
        using var meters = new Meters(metrics);

        Assert.Equal(7, meters.Value("live.streams.owned"));

        metrics.StreamsOwned = 0;

        Assert.Equal(0, meters.Value("live.streams.owned"));
    }

    /// <summary>
    /// The six figures a beat publishes are one picture of one moment, which is the reason they
    /// travel as a record: a scrape landing between six separate writes could report a replica
    /// holding ten streams with eleven of them interrupted.
    /// </summary>
    [Fact]
    public void A_census_publishes_every_figure_the_beat_found()
    {
        using var metrics = new LiveMetrics
        {
            Census = new LiveCensus(
                Streams: 12,
                Interrupted: 3,
                Unstartable: 2,
                Viewers: 40,
                Recordings: 5,
                Forwards: 1),
        };

        using var meters = new Meters(metrics);

        Assert.Equal(12, meters.Value("live.streams.owned"));
        Assert.Equal(3, meters.Value("live.streams.interrupted"));
        Assert.Equal(2, meters.Value("live.streams.unstartable"));
        Assert.Equal(40, meters.Value("live.viewers"));
        Assert.Equal(5, meters.Value("live.recordings.active"));
        Assert.Equal(1, meters.Value("live.forwards.active"));

        // The shortcut the heartbeat uses before it has walked anything must not silently reset the
        // rest of the picture.
        metrics.StreamsOwned = 11;

        Assert.Equal(11, meters.Value("live.streams.owned"));
        Assert.Equal(3, meters.Value("live.streams.interrupted"));
    }

    /// <summary>Both counters carry the port, and a reject carries why.</summary>
    [Fact]
    public void Accepts_and_rejects_are_tagged_by_port_and_by_reason_and_by_nothing_else()
    {
        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);

        metrics.Accepted(9000);
        metrics.Rejected(9000, "conflict");

        var recorded = meters.Read();

        var accept = Assert.Single(recorded, measurement => measurement.Instrument == "live.accepts");
        var reject = Assert.Single(recorded, measurement => measurement.Instrument == "live.rejects");

        Assert.Equal(1, accept.Value);
        Assert.Equal([new KeyValuePair<string, object?>("port", 9000)], accept.Tags);

        Assert.Equal(1, reject.Value);
        Assert.Equal(
            [
                new KeyValuePair<string, object?>("port", 9000),
                new KeyValuePair<string, object?>("reason", "conflict"),
            ],
            reject.Tags);
    }

    /// <summary>
    /// A fed interval reaches all six counters untagged, so a thousand streams are one time series
    /// each rather than a thousand.
    /// </summary>
    [Fact]
    public void A_fed_interval_is_counted_once_and_carries_no_tags()
    {
        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);

        metrics.Fed(new StreamFeed(
            Packets: 900,
            Bytes: 1_200_000,
            KlvPackets: 20,
            KlvRejected: 1,
            PacketsLost: 4,
            PacketsDropped: 2));

        Assert.Equal(900, meters.Total("live.packets"));
        Assert.Equal(1_200_000, meters.Total("live.bytes"));
        Assert.Equal(20, meters.Total("live.klv.packets"));
        Assert.Equal(1, meters.Total("live.klv.rejected"));
        Assert.Equal(4, meters.Total("live.packets.lost"));
        Assert.Equal(2, meters.Total("live.packets.dropped"));

        Assert.Empty(meters.Tags("live.packets"));
        Assert.Empty(meters.Tags("live.bytes"));
    }

    /// <summary>
    /// A stream carrying no KLV and losing nothing - which is nine out of ten of them - costs the
    /// meter nothing per beat rather than four measurements of zero.
    /// </summary>
    [Fact]
    public void A_quiet_stream_reports_nothing_rather_than_zeros()
    {
        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);

        metrics.Fed(new StreamFeed(Packets: 10, Bytes: 500, 0, 0, 0, 0));

        Assert.Equal(10, meters.Total("live.packets"));
        Assert.Empty(meters.Of("live.klv.packets"));
        Assert.Empty(meters.Of("live.packets.lost"));
        Assert.Empty(meters.Of("live.packets.dropped"));
    }

    /// <summary>Every outcome tag is a word this class chose.</summary>
    [Fact]
    public void Outcomes_are_tagged_from_a_fixed_vocabulary()
    {
        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);

        metrics.Claimed("resumed");
        metrics.Ended("displaced");
        metrics.Viewing("relayed");
        metrics.Snapshotted("preview");
        metrics.Recorded("truncated");
        metrics.Overflowed(OverflowPolicy.Fail);
        metrics.Overflowed(OverflowPolicy.SkipToLive);
        metrics.BeatFailed("registry");

        Assert.Equal([new KeyValuePair<string, object?>("outcome", "resumed")], meters.Tags("live.claims"));
        Assert.Equal([new KeyValuePair<string, object?>("reason", "displaced")], meters.Tags("live.streams.ended"));
        Assert.Equal([new KeyValuePair<string, object?>("route", "relayed")], meters.Tags("live.viewer.sessions"));
        Assert.Equal([new KeyValuePair<string, object?>("outcome", "preview")], meters.Tags("live.snapshots"));
        Assert.Equal([new KeyValuePair<string, object?>("outcome", "truncated")], meters.Tags("live.recordings"));
        Assert.Equal([new KeyValuePair<string, object?>("stage", "registry")], meters.Tags("live.heartbeat.failures"));

        Assert.Equal(
            ["fail", "skip-to-live"],
            meters.Of("live.overflows").Select(recording => recording.Tags.Single().Value).ToArray());
    }

    /// <summary>The beat is two seconds, so its histogram has to be able to say so in seconds.</summary>
    [Fact]
    public void The_heartbeat_reports_in_seconds()
    {
        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);

        metrics.Beat(TimeSpan.FromMilliseconds(250));

        Assert.Equal(0.25, meters.Value("live.heartbeat.duration"));
    }

    /// <summary>
    /// The one thing on the fan-out path counted where it happens rather than sampled by the beat.
    /// </summary>
    [Fact]
    public void A_subscriber_that_falls_behind_is_counted_through_the_hub()
    {
        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);
        using var hub = new StreamHub("overflowing", new LiveOptions(), NullLogger.Instance, metrics);

        using var subscription = hub.Subscribe(capacity: 1, OverflowPolicy.SkipToLive, streamIndexes: []);

        // Two past its capacity of one, with nothing reading: the second is what overflows, and the
        // third arrives while it is waiting to resynchronise and is simply not delivered.
        for (var i = 0; i < 3; i++)
        {
            hub.Publish(new MediaPacket(0, [1, 2, 3], i, i, 1, IsKeyframe: false), i);
        }

        Assert.Equal(1, subscription.Overflows);
        Assert.Equal(1, meters.Total("live.overflows"));
        Assert.Equal(
            [new KeyValuePair<string, object?>("policy", "skip-to-live")],
            meters.Tags("live.overflows"));
    }

    /// <summary>
    /// A hub with nowhere to report to has to work exactly as well, because two of the three things
    /// that build one - the detection worker and the tests - publish no meter at all.
    /// </summary>
    [Fact]
    public void A_hub_with_no_meter_still_fans_out()
    {
        using var hub = new StreamHub("unmeasured", new LiveOptions(), NullLogger.Instance);
        using var subscription = hub.Subscribe(capacity: 1, OverflowPolicy.Fail, streamIndexes: []);

        hub.Publish(new MediaPacket(0, [1], 0, 0, 1, IsKeyframe: true), 0);
        hub.Publish(new MediaPacket(0, [1], 1, 1, 1, IsKeyframe: true), 1);

        Assert.Equal(1, subscription.Overflows);
        Assert.True(subscription.Faulted);
    }

    /// <summary>
    /// The counter that catches what never reached libsrt, on a machine that has no such counter.
    /// </summary>
    [Fact]
    public void The_kernel_udp_counter_is_absent_rather_than_fatal_where_proc_is_not()
    {
        var errors = LiveMetrics.KernelUdpReceiveErrors();

        if (OperatingSystem.IsLinux())
        {
            Assert.NotNull(errors);
            Assert.True(errors >= 0);
        }
        else
        {
            Assert.Null(errors);
        }
    }

    /// <summary>
    /// The same absence, seen from the meter: an instrument with nothing to report publishes no
    /// measurement rather than a zero.
    /// </summary>
    [Fact]
    public void The_kernel_udp_counter_publishes_a_measurement_only_where_there_is_one()
    {
        using var metrics = new LiveMetrics();
        using var meters = new Meters(metrics);

        Assert.Equal(OperatingSystem.IsLinux(), meters.Value("live.udp.receive.errors") is not null);
    }
}
