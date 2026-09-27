using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// Runs the ingest port for the life of the process: the libsrt listener, and beside it the
/// heartbeat that keeps the registry honest.
/// </summary>
public sealed class LiveIngestService(
    LiveStreamCoordinator coordinator,
    LiveListeners listeners,
    LiveMetrics metrics,
    IOptions<LiveOptions> options,
    ILogger<LiveIngestService> logger) : BackgroundService
{
    private readonly LiveOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            logger.LogInformation("Live streaming is switched off, so no ingest port is opened");
            return;
        }

        listeners.Enabled = true;

        FfmpegLibrary.EnsureLoaded();

        if (!Srt.IsAvailable)
        {
            // Both listening ports are libsrt's, so without it this replica can accept nothing and
            // takes itself out of the Service rather than swallowing encoders it cannot serve.
            listeners.Fault = "libsrt is not loaded";

            logger.LogError(
                "libsrt is not loaded, or is older than 1.5, so no media port can be opened. Run "
                + "scripts/fetch-libsrt.sh, or install libsrt1.5 on the host.");

            return;
        }

        if (!FfmpegLibrary.InputProtocols().Contains("srt"))
        {
            logger.LogWarning(
                "The loaded FFmpeg has no SRT. Encoders can still push here, but pulled streams "
                + "and relaying a viewer to another replica both dial with libav and will fail. "
                + "Point Media:LibraryPath at a build compiled with libsrt.");
        }

        var listener = new SrtListener(
            StreamIntent.Publish,
            _options,
            coordinator.AdmitPublisher,
            coordinator.OnAccepted,
            logger,
            listeners,
            metrics);

        // One thread per port and one libsrt receive worker behind each, which is the only way this
        // replica gets more than one: the worker belongs to the multiplexer, and there is one of
        // those per bound UDP port per process.
        var ports = Enumerable.Range(_options.IngestPort, _options.IngestPortCount).ToArray();

        listeners.Expect(StreamIntent.Publish, ports.Length);

        // Materialised, not deferred: a lazy Select here would start nothing until the heartbeat
        // ended, which is to say at shutdown.
        var listening = ports
            .Select(port => Task.Factory.StartNew(
                () => listener.Run(port, stoppingToken),
                TaskCreationOptions.LongRunning))
            .ToArray();

        await HeartbeatAsync(stoppingToken);
        await Task.WhenAll(listening);
    }

    /// <summary>
    /// The same beat the coordinator counts heartbeats in, and the same pass refreshes the copy of
    /// the registry the handshake reads, so a name is locked here within one beat of being claimed
    /// anywhere.
    /// </summary>
    private async Task HeartbeatAsync(CancellationToken stoppingToken)
    {
        using var beats = new PeriodicTimer(LiveStreamCoordinator.Beat);

        while (await Safe(() => beats.WaitForNextTickAsync(stoppingToken).AsTask()))
        {
            try
            {
                await coordinator.TickAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                // A registry that is briefly unreachable must not take the ingest port down.
                logger.LogWarning(ex, "A heartbeat pass failed");
            }
        }
    }

    private static async Task<bool> Safe(Func<Task<bool>> wait)
    {
        try
        {
            return await wait();
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
