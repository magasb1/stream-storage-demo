using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using StorageDemo.Api.Observability;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Controllers;

/// <param name="Url">Where to listen or connect, for a protocol that cannot name itself.</param>
public sealed record CreateManualStreamRequest(string Name, string Url);

/// <param name="Seconds">
/// How long to record for. Omitted means the configured default, and a further trigger extends
/// whatever is running rather than starting a second recording.
/// </param>
/// <param name="Detection">
/// The detection that asked for it, when one did. Omitted by a person pressing record, and the
/// document is then exactly what it was before.
/// </param>
public sealed record RecordRequest(double? Seconds, DetectionReference? Detection = null);

/// <param name="Rate">Detections per second. Zero means the worker's default.</param>
/// <param name="Model">rf-detr, yolo26, or null for the worker default.</param>
/// <param name="Labels">COCO labels to retain. Empty means all labels.</param>
public sealed record DetectRequest(
    bool Enabled,
    int Rate = 0,
    string? Model = null,
    IReadOnlyList<string>? Labels = null);

/// <param name="Worker">The worker's own name, as it is written into the stream's registry entry.</param>
public sealed record DetectorClaim(string Worker);

public sealed record LiveStatusResponse(
    IReadOnlyList<string> Transports,
    IReadOnlyList<LiveStream> Streams);

/// <summary>
/// Control plane for live streaming. REST rather than gRPC on purpose: these are a handful of
/// administrative calls.
///
/// Every call names a stream. Any replica answers the ones the registry can satisfy; the ones that
/// need the stream's actual bytes are forwarded to the replica that holds them, so a caller never
/// has to know which pod took the connection. Playback is not here - it is on the consumption port,
/// which is what keeps the firewall statement one sentence per port. The one route that does carry
/// media, <see cref="PeerView"/>, is how one pod fetches a stream from another and never answers a
/// player.
///
/// The verb comes before the name in every route, which reads oddly and is deliberate. A stream
/// name is a resource path and may contain slashes, so it has to be the trailing catch-all; put it
/// first and "live/camera1/record" is ambiguous with a stream called "live/camera1/record".
/// </summary>
[ApiController]
[Route("api/live")]
public sealed class LiveStreamsController(
    ILiveStreamService live,
    LivePeerProxy peers,
    ApiMetrics metrics,
    IOptions<LiveOptions> options) : ControllerBase
{
    /// <summary>
    /// A preview is counted with the document downloads, tagged as what it is. One instrument answers
    /// "how much is this API serving" for files and for live tiles, and the tag is what keeps either
    /// number meaningful: a wall of a thousand tiles refreshing is the heaviest read this surface
    /// takes and it is not a file download.
    /// </summary>
    private const string Surface = "rest";

    private const string PreviewKind = "preview";

    private readonly LiveOptions _options = options.Value;

    [HttpGet]
    public async Task<ActionResult<LiveStatusResponse>> Status(
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return new LiveStatusResponse(live.Transports, await live.StreamsAsync(cancellationToken));
    }

    [HttpGet("stream/{*name}")]
    public async Task<ActionResult<LiveStream>> Get(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await live.GetAsync(name, cancellationToken) is { } stream ? stream : NotFound();
    }

    /// <summary>
    /// Creates a stream by request, for a protocol that cannot name itself. It shares one
    /// namespace and one claim with automatic streams: a manual stream is simply one that claimed
    /// its name early, and an encoder presenting that name is the same conflict as any other.
    ///
    /// A live name is locked, so asking for one somebody else is publishing is a conflict rather
    /// than a take-over. An encoder meets the same rule as a refused handshake.
    /// </summary>
    [HttpPost("manual")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<ActionResult<LiveStream>> CreateManual(
        CreateManualStreamRequest request,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        try
        {
            return Accepted(await live.CreateManualAsync(request.Name, request.Url, cancellationToken));
        }
        catch (Exception ex) when (ex is NotSupportedException or ArgumentException)
        {
            return BadRequest(ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            return Conflict(ex.Message);
        }
    }

    /// <summary>The current preview: the picture now, not a poster frame.</summary>
    [HttpGet("preview/{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Preview(string name, CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            return NotFound();
        }

        // Deliberately unauthenticated, like a document thumbnail: it is a picture, and a client
        // renders it in an image tag rather than through a call it can add headers to.
        if (live.Preview(name) is { } local)
        {
            Response.Headers.XContentTypeOptions = "nosniff";

            // A cached live preview is a still picture of a moving stream, which is the one
            // failure this endpoint exists to avoid. Set on both branches.
            Response.Headers.CacheControl = "no-store";

            metrics.Downloaded(Surface, PreviewKind, "served");
            metrics.DownloadedBytes(Surface, PreviewKind, local.Length);

            return File(local, "image/jpeg");
        }

        var stream = await live.GetAsync(name, cancellationToken);

        if (stream is null || !stream.HasPreview || live.Owns(name))
        {
            metrics.Downloaded(Surface, PreviewKind, "missing");

            return NotFound();
        }

        metrics.Downloaded(Surface, PreviewKind, "served");

        return await ProxyAsync(stream, $"api/live/preview/{name}", cancellationToken);
    }

    /// <summary>
    /// The newest MISB KLV packet, decoded to the ST 0902 minimum set with the raw bytes alongside.
    /// Forwarded to the owner like the preview, because only the owner has the packets; the
    /// answer is JSON, so it rides the control-call relay rather than the byte proxy.
    /// </summary>
    [HttpGet("klv/{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Klv(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await ForwardOrRun(
            name,
            $"api/live/klv/{name}",
            token,
            () => Task.FromResult<IActionResult>(live.Klv(name) is { } sample ? Ok(sample) : NotFound()),
            cancellationToken,
            method: HttpMethod.Get);
    }

    /// <summary>
    /// Switches detection on or off for a stream, at a rate. Set through the owner like record,
    /// because the owner is what publishes the stream's entry and a worker reads it from there;
    /// nothing here touches a worker.
    /// </summary>
    [HttpPut("detect/{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Detect(
        string name,
        DetectRequest request,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        try
        {
            var normalized = request with
            {
                Model = DetectionModels.Normalize(request.Model),
                Labels = CocoClasses.Normalize(request.Labels),
            };

            return await ForwardOrRun(
                name,
                $"api/live/detect/{name}",
                token,
                async () => await live.SetDetectionAsync(
                    name, normalized.Enabled, normalized.Rate, normalized.Model, normalized.Labels, cancellationToken) is { } stream
                    ? Ok(stream)
                    : NotFound(),
                cancellationToken,
                method: HttpMethod.Put,
                body: normalized);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(ex.Message);
        }
    }

    /// <summary>
    /// The newest VMTI frame a worker posted, decoded with the raw ST 0903 packet beside it.
    /// Forwarded to the owner like KLV, because the ring lives with the stream's bytes.
    /// </summary>
    [HttpGet("detections/{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Detections(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await ForwardOrRun(
            name,
            $"api/live/detections/{name}",
            token,
            () => Task.FromResult<IActionResult>(live.Detections(name) is { } sample ? Ok(sample) : NotFound()),
            cancellationToken,
            method: HttpMethod.Get);
    }

    /// <summary>
    /// A worker handing the owner one VMTI frame, typed and raw. Owner-only like the peer view:
    /// a worker addresses the owner directly, having read its address from the listing. The raw
    /// packet has to be what the frame encodes to, or the two answers this route feeds would
    /// disagree with each other.
    /// </summary>
    [HttpPost("peer/detections/{*name}")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult PeerDetections(
        string name,
        VmtiSample sample,
        [FromHeader(Name = "X-Storage-Token")] string? token)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        try
        {
            if (!Misb0903.Encode(sample.Frame).AsSpan().SequenceEqual(sample.Raw))
            {
                return BadRequest("The raw packet is not the encoding of the frame beside it.");
            }
        }
        catch (ArgumentException ex)
        {
            return BadRequest(ex.Message);
        }

        return live.PostDetections(name, sample) ? Accepted() : NotFound();
    }

    /// <summary>
    /// A worker taking or renewing its hold on a stream. Conflict when another worker's lease is
    /// live, which is the answer that makes claiming a listing's free streams safe to race.
    /// </summary>
    [HttpPut("peer/detector/{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> ClaimDetector(
        string name,
        DetectorClaim claim,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        try
        {
            return await live.ClaimDetectorAsync(name, claim.Worker, cancellationToken) is { } stream
                ? Ok(stream)
                : NotFound();
        }
        catch (InvalidOperationException ex)
        {
            return Conflict(ex.Message);
        }
    }

    [HttpDelete("peer/detector/{*name}")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> ReleaseDetector(
        string name,
        [FromQuery] string worker,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await live.ReleaseDetectorAsync(name, worker, cancellationToken) ? Accepted() : NotFound();
    }

    /// <summary>
    /// Takes a picture now and stores it as a document. Served while a stream is interrupted, so
    /// the button still works while the tile shows the gap, and refused once the stream is gone.
    /// </summary>
    /// <param name="detection">
    /// The detection that asked for it, as a body, when one did. A person pressing the button posts
    /// nothing at all, exactly as before.
    /// </param>
    [HttpPost("snapshot/{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Snapshot(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken,
        DetectionReference? detection = null)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await ForwardOrRun(
            name,
            $"api/live/snapshot/{name}",
            token,
            async () => await live.SnapshotAsync(name, detection, cancellationToken) is { } id
                ? Ok(new { documentId = id })
                : NotFound(),
            cancellationToken,
            body: detection);
    }

    /// <summary>
    /// Starts a recording, or extends the one already running. A person pressing record and a
    /// detector firing arrive here identically, which is what makes detection free to add later.
    /// </summary>
    [HttpPost("record/{*name}")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Record(
        string name,
        RecordRequest? request,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        var duration = request?.Seconds is { } seconds and > 0
            ? TimeSpan.FromSeconds(seconds)
            : (TimeSpan?)null;

        return await ForwardOrRun(
            name,
            $"api/live/record/{name}",
            token,
            async () =>
            {
                // Accepted, not Ok: the recording is running and has no further relationship with
                // this call. The document appears when there is a file.
                var status = await live.RecordAsync(
                    name,
                    duration,
                    request?.Detection,
                    cancellationToken);

                return status is null ? NotFound() : Accepted(status);
            },
            cancellationToken,
            body: request);
    }

    [HttpDelete("record/{*name}")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> StopRecording(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await ForwardOrRun(
            name,
            $"api/live/record/{name}",
            token,
            async () => await live.StopRecordingAsync(name, cancellationToken) ? Accepted() : NotFound(),
            cancellationToken,
            method: HttpMethod.Delete);
    }

    [HttpDelete("stream/{*name}")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Stop(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await ForwardOrRun(
            name,
            $"api/live/stream/{name}",
            token,
            async () => await live.StopAsync(name, cancellationToken) ? Accepted() : NotFound(),
            cancellationToken,
            method: HttpMethod.Delete);
    }

    /// <summary>
    /// The stream's bytes, for another replica rather than for a player.
    ///
    /// A viewer's SRT connection lands on whichever pod the load balancer picked; when that is not
    /// the owner, that pod calls this and pumps the answer into the viewer's socket. HTTP rather
    /// than a second SRT hop, which cost a handshake, a second latency window and a second libav
    /// probe for something the player cannot see: it speaks SRT to one address either way.
    ///
    /// Guarded like every other call here, and not a playback route. A player is on the consumption
    /// port, which is the whole reason the ports are split.
    /// </summary>
    /// <param name="from">How far back to start, in seconds. Zero is the live edge.</param>
    /// <param name="continue">
    /// Where the viewer's timeline has already reached, so a stream that moves between replicas
    /// mid-connection does not ask the player to accept timestamps jumping back to zero.
    /// </param>
    [HttpGet("peer/view/{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> PeerView(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken,
        double from = 0,
        double @continue = 0)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        if (!live.Owns(name))
        {
            return NotFound();
        }

        Response.ContentType = "video/mp2t";

        // Asking for twenty seconds and receiving twenty-six is normal, since a stream can only be
        // joined where a decoder can start. The SRT path has to log this because that transport has
        // no way to say it back; here there is one, and the relaying replica logs what it was told.
        Response.Headers["X-Live-Preroll"] = live
            .ResolvePreroll(name, from)
            .ToString("0.###", CultureInfo.InvariantCulture);

        // A live stream that only flushes at the end is not a live stream.
        HttpContext.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        // libav writes through a synchronous callback and has no asynchronous form of it, so this
        // one response opts back in to what the server forbids by default.
        HttpContext.Features.Get<IHttpBodyControlFeature>()!.AllowSynchronousIO = true;

        await live.WriteToViewerAsync(
            new ViewerRequest(name, from),
            Response.Body,
            @continue,
            cancellationToken);

        return new EmptyResult();
    }

    /// <summary>
    /// Runs the call here when this replica owns the stream, and forwards it to the owner when it
    /// does not. Only the owner can act on a stream, so a caller reaching the wrong replica is
    /// routed rather than refused.
    /// </summary>
    private async Task<IActionResult> ForwardOrRun(
        string name,
        string path,
        string? token,
        Func<Task<IActionResult>> run,
        CancellationToken cancellationToken,
        HttpMethod? method = null,
        object? body = null)
    {
        if (live.Owns(name))
        {
            return await run();
        }

        var stream = await live.GetAsync(name, cancellationToken);

        if (stream is null)
        {
            return NotFound();
        }

        return await peers.RelayAsync(stream, method ?? HttpMethod.Post, path, token, body, cancellationToken)
            ?? NotFound();
    }

    private async Task<IActionResult> ProxyAsync(
        LiveStream stream,
        string path,
        CancellationToken cancellationToken)
    {
        var upstream = await peers.OpenAsync(stream, path, cancellationToken);

        if (upstream is null)
        {
            return NotFound();
        }

        Response.ContentType = "image/jpeg";
        Response.Headers.CacheControl = "no-store";

        try
        {
            await using var content = upstream;
            await content.CopyToAsync(Response.Body, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }

        return new EmptyResult();
    }

    /// <summary>Null when the caller may proceed.</summary>
    private ObjectResult? Guard(string? token)
    {
        if (!_options.Enabled)
        {
            // Not enabled means not here; do not advertise what is switched off.
            return NotFound(new ProblemDetails { Status = 404, Title = "Live streaming is disabled." });
        }

        if (string.IsNullOrEmpty(_options.Token))
        {
            return null;
        }

        var provided = Encoding.UTF8.GetBytes(token ?? string.Empty);
        var expected = Encoding.UTF8.GetBytes(_options.Token);

        return CryptographicOperations.FixedTimeEquals(provided, expected)
            ? null
            : new ObjectResult(new ProblemDetails { Status = 401, Title = "Bad token." }) { StatusCode = 401 };
    }
}
