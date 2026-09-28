using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Controllers;

/// <summary>
/// A configured source and what it is actually doing, in one row.
/// </summary>
/// <param name="Stream">
/// Null when the source is not on air anywhere, which is the normal state of a source that is
/// parked, mistyped, or simply not sending yet. It is not an error and the row is still shown: a
/// source that vanishes from the list because its feed stopped is the one thing an operator
/// watching for a broken feed must not see.
/// </param>
public sealed record LiveSourceResponse(LiveSource Source, LiveStream? Stream);

/// <param name="Url">Null or empty means an encoder brings this name in on the ingest port.</param>
/// <param name="Forwards">
/// The whole list, not a delta. Sending the row back without a forward is how one is removed,
/// because the source is the aggregate and a second way to change a forward would drift from this
/// one.
/// </param>
/// <param name="StaticSensor">
/// Where this camera is and where it looks, for a fixed one. Sending it is what makes this service
/// synthesise ST 0601 onto the stream's own metadata track; sending null again removes it. All of
/// it or none of it, which is why it is a nested object rather than eight fields that could be
/// half-filled.
/// </param>
public sealed record SaveLiveSourceRequest(
    string Name,
    string? Url,
    bool Enabled = true,
    IReadOnlyList<ForwardTarget>? Forwards = null,
    StaticSensor? StaticSensor = null);

/// <summary>
/// What a configured source has to satisfy before it is stored, in one place because both surfaces
/// ask the same question and an answer that differed between them would mean the gRPC port could
/// save a row REST refuses.
/// </summary>
public static class LiveSourceRules
{
    /// <summary>
    /// Mints an id for every forward that arrived without one and leaves the rest alone. A caller
    /// never has to invent an id, and one that already exists survives the edit, which is what makes
    /// a forward whose URL changed the same forward rather than a new one.
    /// </summary>
    public static IReadOnlyList<ForwardTarget> WithIds(IReadOnlyList<ForwardTarget>? forwards)
        => forwards is null
            ? []
            : [.. forwards.Select(forward => string.IsNullOrWhiteSpace(forward.Id)
                ? forward with { Id = Guid.NewGuid().ToString("N") }
                : forward)];

    /// <summary>Null when the source may be saved; otherwise a sentence saying what is wrong.</summary>
    public static string? Refuse(LiveSource source, string[] allowedSchemes)
    {
        // The same parser the ingest handshake uses, rather than a second rule here. Two name rules
        // that drifted apart would let a configured source and an encoder disagree about which name
        // is which, and the name is the identity. The parsed form must come back unchanged because
        // this name is also a URL segment and a store key: a name that had to be repaired to be
        // valid would be looked up under something the caller never sent.
        if (!StreamName.TryParse(source.Name, out var parsed, out var rejection) || parsed != source.Name)
        {
            return rejection.Length > 0 ? rejection : $"'{source.Name}' is not a usable stream name.";
        }

        if (source.Url is { Length: > 0 } url && Scheme(url, allowedSchemes) is { } refusedUrl)
        {
            return refusedUrl;
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var forward in source.Forwards)
        {
            if (!seen.Add(forward.Id))
            {
                // Two forwards under one id are two rows the operator cannot tell apart afterwards,
                // and only one of them would ever be reported on.
                return $"Two forwards share the id '{forward.Id}'.";
            }

            if (string.IsNullOrWhiteSpace(forward.Url))
            {
                return "A forward needs a URL.";
            }

            if (Scheme(forward.Url, allowedSchemes) is { } refusedForward)
            {
                return refusedForward;
            }
        }

        return Sensor(source.StaticSensor);
    }

    /// <summary>
    /// Null when the sensor may be stored, or when none was configured.
    ///
    /// Every range is the ST 0601 item's own, read from the scale table in <see cref="Misb0601"/>'s
    /// decoder - which is the table checked against a real stream in Misb0601RealStreamTests, so
    /// these are the encodable ranges rather than plausible-looking ones. Refused here rather than
    /// saturated at the encoder because a saturated value is indistinguishable from a measured one:
    /// a camera configured at 25,000 metres would be published, conformingly, at 19,000.
    ///
    /// Bearing is 0..360 rather than 0..359.x because ST 0601 tag 5's own scale ends at 360, and
    /// 360 encodes as the largest unsigned short rather than wrapping to zero. A caller that means
    /// due north may say either.
    /// </summary>
    private static string? Sensor(StaticSensor? sensor)
    {
        if (sensor is null)
        {
            return null;
        }

        if (sensor.Latitude is < -90 or > 90 || double.IsNaN(sensor.Latitude))
        {
            return "A sensor latitude is -90 to 90 degrees.";
        }

        if (sensor.Longitude is < -180 or > 180 || double.IsNaN(sensor.Longitude))
        {
            return "A sensor longitude is -180 to 180 degrees.";
        }

        // ST 0601 tag 15's own range. Below sea level as far as the Dead Sea shore and above as far
        // as anything that reports altitude this way flies.
        if (sensor.AltitudeMetres is < -900 or > 19_000 || double.IsNaN(sensor.AltitudeMetres))
        {
            return "A sensor altitude is -900 to 19000 metres.";
        }

        if (sensor.TrueBearing is < 0 or > 360 || double.IsNaN(sensor.TrueBearing))
        {
            return "A true bearing is 0 to 360 degrees.";
        }

        // Tag 19's range, which is the full sphere rather than the half a mast would use: a camera
        // may look up, and refusing that would be this service inventing a constraint ST 0601 does
        // not have.
        if (sensor.Depression is < -180 or > 180 || double.IsNaN(sensor.Depression))
        {
            return "A depression angle is -180 to 180 degrees.";
        }

        if (sensor.HorizontalFov is < 0 or > 180 || double.IsNaN(sensor.HorizontalFov)
            || sensor.VerticalFov is < 0 or > 180 || double.IsNaN(sensor.VerticalFov))
        {
            return "A field of view is 0 to 180 degrees.";
        }

        // Absent is unmarked, which Misb0601 already treats as an answer distinct from
        // "unclassified", so only a marking that was given and is not one of the five is refused.
        if (sensor.Classification is { Length: > 0 } marking
            && !Misb0601.Classifications.Contains(marking, StringComparer.OrdinalIgnoreCase))
        {
            return $"'{marking}' is not an ST 0102 classification. One of: {string.Join(", ", Misb0601.Classifications)}.";
        }

        return null;
    }

    /// <summary>
    /// The allowlist, checked here at the trust boundary so a mistyped URL is refused while the
    /// operator is still looking at it rather than in a log some minutes later.
    ///
    /// <see cref="LiveStreamCoordinator"/> enforces the same list again where the URL is actually
    /// handed to libav, and that copy is the one that matters: this one is the early, friendly
    /// rejection and could be removed without weakening anything. The rule itself must not be
    /// loosened here, because without it a caller makes the service read or write anywhere libav
    /// can reach, local files included - a URL with no scheme at all is a path, which is why the
    /// absent case is treated as file rather than waved through.
    /// </summary>
    private static string? Scheme(string url, string[] allowedSchemes)
    {
        var separator = url.IndexOf("://", StringComparison.Ordinal);
        var scheme = separator > 0 ? url[..separator].ToLowerInvariant() : "file";

        return allowedSchemes.Contains(scheme, StringComparer.OrdinalIgnoreCase)
            ? null
            : $"Transport '{scheme}' is not allowed. Allowed: {string.Join(", ", allowedSchemes)}.";
    }
}

/// <summary>
/// Configured sources: what an operator asked this service to fetch and where to copy it on, as
/// opposed to what happens to be on air. REST here and gRPC beside it, because a command-line tool
/// configures a source as readily as the desktop client does.
///
/// Unlike almost every route in <see cref="LiveStreamsController"/>, nothing here is forwarded to a
/// peer. Those routes need a stream's bytes and only its owner has them; these need the source
/// store, which every replica shares, so any replica answers and a caller behind a load balancer
/// never has to land anywhere in particular. That difference is why this is a separate controller
/// rather than more routes on that one.
///
/// The source is the aggregate. A forward is saved, edited and removed by sending the whole row
/// back, and there is deliberately no route for an individual forward: two ways to change one thing
/// drift apart, and the one that drifts is always the one nobody tested.
/// </summary>
[ApiController]
[Route("api/live/sources")]
public sealed class LiveSourcesController(
    ILiveSourceStore sources,
    ILiveStreamService live,
    IOptions<LiveOptions> options) : ControllerBase
{
    private readonly LiveOptions _options = options.Value;

    /// <summary>
    /// Every configured source with what it is currently doing, joined here rather than by the
    /// caller. This is what a wall of tiles polls, and a listing that made a caller ask after each
    /// row separately would cost a thousand calls a beat at the size this service is built for.
    /// </summary>
    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<LiveSourceResponse>>> List(
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        var configured = await sources.ListAsync(cancellationToken);
        var streams = (await live.StreamsAsync(cancellationToken)).ToDictionary(stream => stream.Name);

        return configured
            .Select(source => new LiveSourceResponse(source, streams.GetValueOrDefault(source.Name)))
            .ToList();
    }

    [HttpGet("{*name}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<LiveSourceResponse>> Get(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        return await sources.GetAsync(name, cancellationToken) is { } source
            ? new LiveSourceResponse(source, await live.GetAsync(name, cancellationToken))
            : NotFound();
    }

    /// <summary>
    /// Creates the source or replaces it whole, forwards included. Nothing is started here: the
    /// row says what should be running and whichever replica picks it up acts on it, which is what
    /// makes this call answerable by any of them.
    /// </summary>
    [HttpPut("{*name}")]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<LiveSource>> Save(
        string name,
        SaveLiveSourceRequest request,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        // A body naming a different source than the route is a caller that has lost track of which
        // row it is editing, and guessing which of the two it meant would overwrite the other one.
        if (!string.Equals(name, request.Name, StringComparison.Ordinal))
        {
            return BadRequest($"The route names '{name}' and the body names '{request.Name}'.");
        }

        var source = new LiveSource(
            request.Name,
            string.IsNullOrWhiteSpace(request.Url) ? null : request.Url,
            request.Enabled,
            LiveSourceRules.WithIds(request.Forwards),
            DateTimeOffset.UtcNow,
            request.StaticSensor);

        if (LiveSourceRules.Refuse(source, _options.AllowedSchemes) is { } rejection)
        {
            return BadRequest(rejection);
        }

        await sources.SaveAsync(source, cancellationToken);

        // The saved row rather than an empty success, because the ids minted above are on it and a
        // caller that had to fetch them back would be reading its own write to learn what it sent.
        return source;
    }

    /// <summary>
    /// Forgets the configuration. It does not stop a stream that is currently on air under this
    /// name: what this removes is the instruction to establish it again, and a feed with viewers on
    /// it is the operator's to stop through <c>DELETE api/live/stream/{name}</c>. Deleting a
    /// configuration row should never yank a live picture away from whoever is watching it without
    /// somebody having asked for exactly that.
    /// </summary>
    [HttpDelete("{*name}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> Delete(
        string name,
        [FromHeader(Name = "X-Storage-Token")] string? token,
        CancellationToken cancellationToken)
    {
        if (Guard(token) is { } refused)
        {
            return refused;
        }

        await sources.RemoveAsync(name, cancellationToken);

        return NoContent();
    }

    /// <summary>
    /// Null when the caller may proceed. The same check <see cref="LiveStreamsController"/> makes,
    /// duplicated because it is private there and this controller cannot inherit it; these routes
    /// decide what the service connects out to, so they are if anything more sensitive than the
    /// ones already behind it.
    /// </summary>
    private ObjectResult? Guard(string? token)
    {
        if (!_options.Enabled)
        {
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
