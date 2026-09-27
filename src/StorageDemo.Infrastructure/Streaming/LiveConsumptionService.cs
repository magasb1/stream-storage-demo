using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>The consumption port: SRT out, live only.</summary>
public sealed class LiveConsumptionService(
    LiveStreamCoordinator coordinator,
    LiveListeners listeners,
    LiveMetrics metrics,
    IHttpClientFactory clients,
    IOptions<LiveOptions> options,
    ILogger<LiveConsumptionService> logger) : BackgroundService
{
    private readonly LiveOptions _options = options.Value;

    private CancellationToken _stopping;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stopping = stoppingToken;

        // The same gate ingest sets its fault on, asked directly rather than read off that service,
        // so neither port depends on which of the two started first.
        if (!_options.Enabled || !Srt.IsAvailable)
        {
            return Task.CompletedTask;
        }

        var listener = new SrtListener(
            StreamIntent.Subscribe,
            _options,
            Admit,
            OnAccepted,
            logger,
            listeners,
            metrics);

        return Task.Factory.StartNew(
            () => listener.Run(_options.ConsumptionPort, stoppingToken),
            TaskCreationOptions.LongRunning);
    }

    /// <summary>Everything the listener could parse is admitted.</summary>
    private static int? Admit(Admission admission) => null;

    /// <summary>Takes an accepted viewer off the accept thread.</summary>
    private void OnAccepted(AcceptedSocket socket)
    {
        var name = socket.Name;

        // The position rides in the identifier's user_from key, so returning to live is a new
        // connection rather than a control message and the connection stays one-way.
        var from = StreamName.Position(socket.StreamId) ?? 0;
        var viewer = new SrtSocketStream(socket.Release(), writable: true);

        _ = Task.Factory.StartNew(
            () => ServeAsync(name, from, viewer, _stopping),
            TaskCreationOptions.LongRunning);
    }

    /// <summary>
    /// Serves one viewer for as long as it stays connected, whatever happens to the stream behind
    /// it.
    /// </summary>
    private async Task ServeAsync(
        string name,
        double from,
        SrtSocketStream viewer,
        CancellationToken stopping)
    {
        var timeline = 0d;
        var connected = DateTimeOffset.UtcNow;
        var waitingSince = connected;

        // Wall clock stands in for the timeline across a relayed hop, because the owner's answer
        // carries no exact figure back.
        double Carried() => Math.Max(timeline, (DateTimeOffset.UtcNow - connected).TotalSeconds);

        try
        {
            while (!stopping.IsCancellationRequested)
            {
                var stream = await coordinator.GetAsync(name, stopping);

                if (stream is null)
                {
                    if (DateTimeOffset.UtcNow - waitingSince > TimeSpan.FromSeconds(_options.GracePeriodSeconds))
                    {
                        logger.LogInformation("Giving up on '{Name}' for a viewer; it is not on air", name);
                        return;
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(250), stopping);
                    continue;
                }

                if (coordinator.Owns(name))
                {
                    // Asking for twenty seconds and receiving twenty-six is normal, since a stream
                    // can only be joined where a decoder can start.
                    logger.LogInformation(
                        "Serving '{Name}' to a viewer from {Given:0.#}s back (asked for {Asked:0.#}s)",
                        name,
                        coordinator.ResolvePreroll(name, from),
                        from);

                    metrics.Viewing("direct");

                    timeline = await coordinator.WriteToViewerAsync(
                        new ViewerRequest(name, from),
                        viewer,
                        timeline,
                        stopping);
                }
                else
                {
                    metrics.Viewing("relayed");

                    await RelayAsync(stream, from, Carried(), viewer, stopping);

                    timeline = Carried();
                }

                if (viewer.Faulted)
                {
                    logger.LogInformation("A viewer of '{Name}' went away", name);

                    return;
                }

                // Whatever was feeding this viewer stopped, and only now does the wait begin.
                waitingSince = DateTimeOffset.UtcNow;

                // Only a rollback is a chosen position; resuming after a gap wants the live edge
                // rather than the same twenty seconds again.
                from = 0;

                await Task.Delay(TimeSpan.FromMilliseconds(250), stopping);
            }
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Serving '{Name}' to a viewer failed", name);
        }
        finally
        {
            await viewer.DisposeAsync();
        }
    }

    /// <summary>
    /// Fetches the stream from the replica that owns it and pumps its bytes into this viewer.
    /// </summary>
    private async Task RelayAsync(
        LiveStream stream,
        double from,
        double timeline,
        Stream viewer,
        CancellationToken stopping)
    {
        if (stream.OwnerAddress is not { Length: > 0 } address)
        {
            logger.LogWarning(
                "'{Name}' is owned by {Owner}, which recorded no address, so a viewer here cannot "
                + "be served. Set Live:PeerBaseUrl on every replica.",
                stream.Name,
                stream.Owner);

            await Task.Delay(TimeSpan.FromSeconds(1), stopping);

            return;
        }

        // The name is the trailing catch-all of that route and may contain slashes, so it goes in
        // unescaped exactly as every other forwarded call writes it.
        var url = $"{address.TrimEnd('/')}/api/live/peer/view/{stream.Name}"
            + $"?from={Figure(from)}&continue={Figure(timeline)}";

        using var request = new HttpRequestMessage(HttpMethod.Get, url);

        if (_options.Token is { Length: > 0 } token)
        {
            request.Headers.Add("X-Storage-Token", token);
        }

        try
        {
            using var response = await clients.CreateClient(LiveOptions.PeerClient)
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, stopping);

            if (!response.IsSuccessStatusCode)
            {
                logger.LogWarning(
                    "{Url} answered {Status} for a relayed viewer", url, (int)response.StatusCode);
            }
            else
            {
                logger.LogInformation(
                    "Relaying '{Name}' from its owner {Owner}, from {Given}s back (asked for {Asked})",
                    stream.Name,
                    stream.Owner,
                    response.Headers.TryGetValues("X-Live-Preroll", out var given)
                        ? given.FirstOrDefault()
                        : "?",
                    Figure(from));

                await using var upstream = await response.Content.ReadAsStreamAsync(stopping);

                await upstream.CopyToAsync(viewer, stopping);

                return;
            }
        }
        catch (Exception ex)
            when (ex is HttpRequestException or TaskCanceledException && !stopping.IsCancellationRequested)
        {
            // The address may belong to a replica that has already gone: a force-killed pod leaves
            // its registry entry behind until its heartbeat goes stale.
            logger.LogWarning(ex, "Could not reach {Url} to relay a viewer", url);
        }

        // Straight round the loop: the registry is re-read every pass, so the moment the encoder
        // reconnects somewhere this viewer follows it.
        await Task.Delay(TimeSpan.FromMilliseconds(250), stopping);
    }

    private static string Figure(double seconds)
        => seconds.ToString("0.###", CultureInfo.InvariantCulture);
}
