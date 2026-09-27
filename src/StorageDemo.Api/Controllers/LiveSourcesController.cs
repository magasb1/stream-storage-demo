using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Controllers;

/// <summary>A configured source and what it is actually doing, in one row.</summary>
/// <param name="Stream">
/// Null when the source is not on air anywhere, which is the normal state of a source that is
/// parked, mistyped, or simply not sending yet.
/// </param>
public sealed record LiveSourceResponse(LiveSource Source, LiveStream? Stream);

/// <param name="Url">Null or empty means an encoder brings this name in on the ingest port.</param>
/// <param name="Forwards">The whole list, not a delta.</param>
public sealed record SaveLiveSourceRequest(
    string Name,
    string? Url,
    bool Enabled = true,
    IReadOnlyList<ForwardTarget>? Forwards = null);

/// <summary>
/// What a configured source has to satisfy before it is stored, in one place because both surfaces
/// ask the same question and an answer that differed between them would mean the gRPC port could
/// save a row REST refuses.
/// </summary>
public static class LiveSourceRules
{
    /// <summary>Mints an id for every forward that arrived without one and leaves the rest alone.</summary>
    public static IReadOnlyList<ForwardTarget> WithIds(IReadOnlyList<ForwardTarget>? forwards)
        => forwards is null
            ? []
            : [.. forwards.Select(forward => string.IsNullOrWhiteSpace(forward.Id)
                ? forward with { Id = Guid.NewGuid().ToString("N") }
                : forward)];

    /// <summary>Null when the source may be saved; otherwise a sentence saying what is wrong.</summary>
    public static string? Refuse(LiveSource source, string[] allowedSchemes)
    {
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

        return null;
    }

    /// <summary>
    /// The allowlist, checked here at the trust boundary so a mistyped URL is refused while the
    /// operator is still looking at it rather than in a log some minutes later.
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
/// opposed to what happens to be on air.
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
    /// caller.
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

    /// <summary>Creates the source or replaces it whole, forwards included.</summary>
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

        if (!string.Equals(name, request.Name, StringComparison.Ordinal))
        {
            return BadRequest($"The route names '{name}' and the body names '{request.Name}'.");
        }

        var source = new LiveSource(
            request.Name,
            string.IsNullOrWhiteSpace(request.Url) ? null : request.Url,
            request.Enabled,
            LiveSourceRules.WithIds(request.Forwards),
            DateTimeOffset.UtcNow);

        if (LiveSourceRules.Refuse(source, _options.AllowedSchemes) is { } rejection)
        {
            return BadRequest(rejection);
        }

        await sources.SaveAsync(source, cancellationToken);

        return source;
    }

    /// <summary>Forgets the configuration.</summary>
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

    /// <summary>Null when the caller may proceed.</summary>
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
