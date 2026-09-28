using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// Publishes a fixed camera's configured position and pointing onto its own stream, as ST 0601,
/// for as long as the stream is running.
///
/// This is the second half of what makes a static camera indistinguishable from one that reports
/// its own telemetry. <see cref="StreamLayout"/> gives the stream a metadata track it never
/// declared; this puts packets on it. They go in through <see cref="StreamHub"/>, which is the
/// only way every consumer sees them through one path - viewers, forwards, recordings, the
/// snapshot muxer and this service's own <see cref="KlvExtractor"/> - rather than the metadata
/// being injected once per route and missing from whichever route was added last.
///
/// One per stream, owned by <see cref="LiveStreamEntry"/> beside the decoder and the extractor,
/// and it holds no state of its own: the sensor is fixed, so every packet is the same set with a
/// later timestamp on it.
/// </summary>
public sealed class StaticSensorPublisher(StreamHub hub, StaticSensor sensor, ILogger logger)
{
    /// <summary>
    /// How often a set goes out.
    ///
    /// Once a second, which is conventional for a fixed sensor and is the answer to two questions
    /// at once. Static data at the frame rate is waste - the content is identical and only the
    /// timestamp moves - while a consumer that joins mid-stream waits at most this long for the
    /// first set, and anything slower makes a viewer stare at an unplaced camera.
    ///
    /// It is also the rate declared on the track, and those two must not drift apart: a track
    /// whose declared rate is wrong resizes every viewer's queue on the stream. Hence one constant
    /// read by both <see cref="Interval"/> and whoever builds the <see cref="SyntheticTrack"/>.
    /// </summary>
    public const double PacketsPerSecond = 1;

    public static readonly TimeSpan Interval = TimeSpan.FromSeconds(1 / PacketsPerSecond);

    /// <summary>
    /// ST 0601 tag 10, Platform Designation, on every set this publishes: the in-band half of
    /// saying where the numbers came from.
    ///
    /// In the packet as well as on <see cref="KlvSample.Synthesised"/>, not instead of it. The flag
    /// is what a client reads to say "configured position" rather than "reported position" without
    /// parsing a string; the tag is what an exploitation workstation that has never heard of this
    /// service reads, because it decodes tag 10 already. Provenance that only this service's own
    /// clients can see is provenance that is lost at the first hop.
    /// </summary>
    public const string Designation = "SYNTHESISED STATIC SENSOR";

    /// <summary>
    /// Runs until the token is cancelled or the hub closes. Every tick re-reads the layout, because
    /// a reconnect adopts a new one and the track's index is not promised to be where it was.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var timer = new PeriodicTimer(Interval);

            while (await timer.WaitForNextTickAsync(cancellationToken) && !hub.Closed)
            {
                if (hub.Layout?.SyntheticIndexOf(SyntheticTrackRole.PlatformMetadata) is not { } index
                    || index < 0)
                {
                    // Nothing demultiplexed yet, or a reconnect brought a layout with no synthetic
                    // track - a camera that has started reporting its own telemetry, which wins.
                    continue;
                }

                // Stamped by the hub at the live edge rather than here. The rules that stamp has to
                // satisfy are unforgiving in both directions and are stated once, on
                // StreamHub.PublishAtLiveEdge, where the live edge is actually readable without a
                // race. False here only means nothing has arrived yet.
                hub.PublishAtLiveEdge(index, Misb0601.Encode(sensor, DateTimeOffset.UtcNow, Designation));
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            // The stream carries on without its metadata rather than ending. A configuration that
            // Encode refuses is the one way this throws, and every range it checks is one
            // LiveSourceRules.Refuse already checked at the trust boundary - so reaching here means
            // the two disagree, which is worth a log line rather than a dead stream.
            logger.LogWarning(ex, "Synthesised metadata for '{Name}' stopped", hub.Name);
        }
    }
}
