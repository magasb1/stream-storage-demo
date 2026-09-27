using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// The consumption port: SRT out, live only.
///
/// Symmetric with ingest, down to running the same kind of listener. A player calls this port and
/// names the stream it wants in its stream identifier, exactly as an encoder names the stream it is
/// sending. One address, any replica behind it, nothing to configure on the player beyond a URL.
///
/// Its own port rather than a route on the API, so a deployment can expose one network to viewers
/// and keep the other private. Reaching this port lets you watch live streams and nothing else,
/// which is the whole reason the ports are split. Finished recordings and snapshots are documents
/// and stay on the API.
///
/// One cost comes with the symmetry and is worth stating plainly: a viewer reaching a replica that
/// does not own the stream cannot be redirected, since SRT has no such thing, so that replica
/// fetches the bytes from the owner over HTTP and pumps them into the viewer's socket. SRT stops at
/// the pod the player reached; the hop behind it is the peer proxy every other forwarded call uses.
/// </summary>
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

    /// <summary>
    /// Everything the listener could parse is admitted.
    ///
    /// The name lock is deliberately not applied here: it stops a second publisher taking a live
    /// name, and a viewer takes nothing. Refusing viewers of a live stream would be exactly backwards.
    /// Phase 4's capacity limit will not apply here either, for the same reason. A viewer is cheap.
    /// </summary>
    private static int? Admit(Admission admission) => null;

    /// <summary>
    /// Takes an accepted viewer off the accept thread. As on ingest, everything real happens
    /// elsewhere: time spent here is time the consumption port is not listening.
    /// </summary>
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
    ///
    /// This is a loop rather than a single attach, because that is where a viewer's downtime
    /// actually comes from. When the replica owning a stream disappears, the encoder reconnects
    /// somewhere else and the name moves; a viewer served by a single attach would have its socket
    /// closed and would have to reconnect, which for a player means a black screen and a fresh
    /// handshake. Holding the socket open and re-attaching to wherever the stream went costs the
    /// viewer only the gap in the feed itself.
    ///
    /// The timeline carries across each re-attach, so the player is never asked to accept
    /// timestamps jumping back to zero in the middle of one connection.
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
        // carries no exact figure back. Stream time cannot outrun wall time except by the pre-roll
        // of the first attach, which the maximum keeps: a small forward jump at the seam is what a
        // player tolerates, and backwards is what breaks it.
        //
        // ponytail: wall clock rather than the owner's own figure. If a player visibly objects to
        // the jump, return it as an HTTP trailer - Kestrel only writes trailers on HTTP/2, so that
        // means pointing this client at the peer with a cleartext prior-knowledge version policy.
        // Not before a player complains.
        double Carried() => Math.Max(timeline, (DateTimeOffset.UtcNow - connected).TotalSeconds);

        try
        {
            while (!stopping.IsCancellationRequested)
            {
                var stream = await coordinator.GetAsync(name, stopping);

                if (stream is null)
                {
                    // Gone, or not there yet. A viewer is given the same grace an interrupted
                    // stream gets, because a stream moving between replicas looks exactly like
                    // this from here and dropping the player would be the more disruptive answer.
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
                    // can only be joined where a decoder can start. There is no response header on
                    // this transport, so it is logged and the stream reports what it holds.
                    logger.LogInformation(
                        "Serving '{Name}' to a viewer from {Given:0.#}s back (asked for {Asked:0.#}s)",
                        name,
                        coordinator.ResolvePreroll(name, from),
                        from);

                    timeline = await coordinator.WriteToViewerAsync(
                        new ViewerRequest(name, from),
                        viewer,
                        timeline,
                        stopping);
                }
                else
                {
                    await RelayAsync(stream, from, Carried(), viewer, stopping);

                    timeline = Carried();
                }

                if (viewer.Faulted)
                {
                    // The viewer left. Without this the loop would re-attach to a socket nobody is
                    // listening to and do it again every quarter second, forever, because the
                    // muxer treats a refused write as "stop writing" rather than as a failure.
                    logger.LogInformation("A viewer of '{Name}' went away", name);

                    return;
                }

                // Whatever was feeding this viewer stopped, and only now does the wait begin. Set
                // here rather than before the attach above, because that attach can last hours:
                // measured against its start, the grace a viewer gets is the grace period minus
                // however long it has been watching, which for anyone watching longer than that is
                // none at all. The symptom was a viewer on a healthy replica being dropped the
                // instant the owning replica shut down cleanly, while a force-killed owner left a
                // stale registry entry behind and so never reached this branch - a graceful
                // shutdown was worse for a viewer than a crash. Observed on k3s; see
                // .scratch/scale-to-1000/cross-pod.md.
                waitingSince = DateTimeOffset.UtcNow;

                // Only a rollback is a chosen position; resuming after a gap wants the live edge
                // rather than the same twenty seconds again.
                from = 0;

                await Task.Delay(TimeSpan.FromMilliseconds(250), stopping);
            }
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException)
        {
            // The viewer closed the player. Normal.
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
    ///
    /// A media relay, which an HTTP consumption port would not have needed. It is the price of a
    /// player speaking one protocol to one address: SRT has no redirect, so the replica the load
    /// balancer picked either serves the viewer or fetches for it.
    ///
    /// The fetch is HTTP over the peer address, the same hop a forwarded snapshot takes. Dialling
    /// the owner's SRT consumption port instead cost a second handshake, a second latency window
    /// and a second libav probe on this side - about two seconds on a viewer's join, measured on
    /// k3s against 1.8 seconds joining on the owner - and none of it was ever visible to the
    /// player. Media still never reaches a viewer over the API port; it travels over it between
    /// two pods, which is what the peer proxy was built for.
    /// </summary>
    /// <param name="timeline">
    /// Where this viewer's output timeline has already reached, so the owner's muxer carries on
    /// from it rather than starting again at zero.
    /// </param>
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
            // its registry entry behind until its heartbeat goes stale. A connect that runs out of
            // its second arrives as a cancellation rather than as a request failure.
            logger.LogWarning(ex, "Could not reach {Url} to relay a viewer", url);
        }

        // Straight round the loop: the registry is re-read every pass, so the moment the encoder
        // reconnects somewhere this viewer follows it.
        await Task.Delay(TimeSpan.FromMilliseconds(250), stopping);
    }

    private static string Figure(double seconds)
        => seconds.ToString("0.###", CultureInfo.InvariantCulture);
}
