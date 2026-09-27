using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using StorageDemo.Api.Observability;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Controllers;

/// <summary>
/// Reaches the replica that owns a stream.
///
/// Only the owner has the bytes and only the owner can act on a stream, so a call landing anywhere
/// else is routed rather than refused. This is what lets a caller keep talking to one address
/// while the stream itself exists on exactly one pod.
///
/// The address is the one the owner recorded when it claimed the name, not one derived from a
/// predictable pod name. That is what allows a Deployment instead of a StatefulSet, and it deletes
/// the headless Service and the per-pod ingest Services with it.
///
/// Media never travels over the API port to a viewer. It travels over it between two pods, which is
/// what this was built for: a viewer that lands on the wrong replica is served from the owner
/// through here, and only the last hop to the player is SRT.
/// </summary>
public sealed class LivePeerProxy(
    IHttpClientFactory clients,
    ILiveStreamService live,
    ApiMetrics metrics,
    ILogger<LivePeerProxy> logger)
{
    public string Owner => live.Owner;

    /// <summary>Reads a stream of bytes from the owner. Null when it cannot be reached.</summary>
    public async Task<Stream?> OpenAsync(LiveStream stream, string path, CancellationToken cancellationToken)
    {
        if (Address(stream, path) is not { } address)
        {
            metrics.Peered("media", "no-address");

            return null;
        }

        try
        {
            var response = await clients.CreateClient(LiveOptions.PeerClient).GetAsync(
                address,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken);

            metrics.Peered("media", response.IsSuccessStatusCode ? "ok" : "refused");

            return response.IsSuccessStatusCode
                ? await response.Content.ReadAsStreamAsync(cancellationToken)
                : null;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            logger.LogWarning(ex, "Could not read from {Address}", address);

            // The registry can name a pod that is already gone - a force-killed replica leaves its
            // entry behind until its heartbeat goes stale - so this is the count that says a
            // cluster is carrying a dead owner rather than a failing network.
            metrics.Peered("media", "unreachable");

            return null;
        }
    }

    /// <summary>
    /// Asks the owner a question and reads its JSON answer as a value, for a caller that is not
    /// itself an HTTP response. Null when the owner cannot be reached or answered anything but OK.
    /// A GET with no body unless told otherwise; the detection toggle is the one control call the
    /// gRPC surface forwards with a body.
    /// </summary>
    public async Task<T?> FetchAsync<T>(
        LiveStream stream,
        string path,
        string? token,
        CancellationToken cancellationToken,
        HttpMethod? method = null,
        object? body = null)
        where T : class
    {
        if (Address(stream, path) is not { } address)
        {
            metrics.Peered("fetch", "no-address");

            return null;
        }

        using var request = new HttpRequestMessage(method ?? HttpMethod.Get, address);

        if (token is { Length: > 0 })
        {
            request.Headers.Add("X-Storage-Token", token);
        }

        if (body is not null)
        {
            request.Content = JsonContent.Create(body);
        }

        try
        {
            using var response = await clients.CreateClient(LiveOptions.PeerClient)
                .SendAsync(request, cancellationToken);

            metrics.Peered("fetch", response.IsSuccessStatusCode ? "ok" : "refused");

            return response.IsSuccessStatusCode
                ? await response.Content.ReadFromJsonAsync<T>(cancellationToken)
                : null;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException)
        {
            logger.LogWarning(ex, "Could not read from {Address}", address);

            metrics.Peered("fetch", "unreachable");

            return null;
        }
    }

    /// <summary>
    /// Repeats a control call against the owner and hands back its answer. Null when the owner
    /// cannot be reached, which the caller reports as the stream not being found: an owner nobody
    /// can talk to is indistinguishable from a stream that is gone.
    /// </summary>
    public async Task<IActionResult?> RelayAsync(
        LiveStream stream,
        HttpMethod method,
        string path,
        string? token,
        object? body,
        CancellationToken cancellationToken)
    {
        if (Address(stream, path) is not { } address)
        {
            metrics.Peered("control", "no-address");

            return null;
        }

        using var request = new HttpRequestMessage(method, address);

        if (token is { Length: > 0 })
        {
            request.Headers.Add("X-Storage-Token", token);
        }

        if (body is not null)
        {
            request.Content = JsonContent.Create(body);
        }

        try
        {
            using var response = await clients.CreateClient(LiveOptions.PeerClient)
                .SendAsync(request, cancellationToken);

            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            // A forwarded call is counted for having been forwarded and answered. Whether the
            // owner's answer was a 404 is the caller's business and travels back untouched, which is
            // why "refused" here means the hop failed rather than the request did.
            metrics.Peered("control", response.IsSuccessStatusCode ? "ok" : "refused");

            return new ContentResult
            {
                StatusCode = (int)response.StatusCode,
                Content = content,
                ContentType = response.Content.Headers.ContentType?.ToString() ?? "application/json",
            };
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            logger.LogWarning(ex, "Could not reach {Address}", address);

            metrics.Peered("control", "unreachable");

            return null;
        }
    }

    private string? Address(LiveStream stream, string path)
    {
        if (string.IsNullOrWhiteSpace(stream.OwnerAddress))
        {
            logger.LogWarning(
                "'{Name}' is owned by {Owner}, which recorded no address, so it cannot be reached "
                + "from here. Set Live:PeerBaseUrl on every replica.",
                stream.Name,
                stream.Owner);

            return null;
        }

        return $"{stream.OwnerAddress.TrimEnd('/')}/{path}";
    }
}
