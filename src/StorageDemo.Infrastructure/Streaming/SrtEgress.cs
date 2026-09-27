using System.Net;
using System.Net.Sockets;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// Bytes actually put on the wire, the one fact <see cref="AvioWriter"/> and <see
/// cref="SrtSocketStream"/> both keep for the same reason: a forward's own byte count is meant to
/// answer "what left", not "what was muxed", and the two can differ by whatever a container's
/// overhead is.
/// </summary>
internal interface IWireWriter
{
    long Written { get; }
}

/// <summary>
/// What an SRT forward target's URL asks for, parsed once rather than re-read a field at a time.
/// </summary>
internal readonly record struct SrtTarget(
    string Host,
    int Port,
    bool Listen,
    string? StreamId,
    int? LatencyMs,
    string? Passphrase)
{
    public static SrtTarget Parse(string url)
    {
        var uri = new Uri(url);

        if (!string.Equals(uri.Scheme, "srt", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"'{url}' is not an SRT URL.", nameof(url));
        }

        if (uri.Port < 0)
        {
            throw new ArgumentException($"'{url}' names no port.", nameof(url));
        }

        var query = ParseQuery(uri.Query);

        return new SrtTarget(
            uri.Host,
            uri.Port,
            query.TryGetValue("mode", out var mode) && string.Equals(mode, "listener", StringComparison.OrdinalIgnoreCase),
            query.GetValueOrDefault("streamid"),
            query.TryGetValue("latency", out var latency) && int.TryParse(latency, out var ms) ? ms : null,
            query.GetValueOrDefault("passphrase"));
    }

    /// <summary>
    /// A minimal reader for the handful of keys an SRT forward target uses, kept local rather than
    /// reaching for <c>Microsoft.AspNetCore.WebUtilities</c> for four possible keys on a URL this
    /// service itself constructs the shape of.
    /// </summary>
    private static Dictionary<string, string> ParseQuery(string query)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var pair in query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = pair.IndexOf('=');
            var key = separator < 0 ? pair : pair[..separator];
            var value = separator < 0 ? string.Empty : Uri.UnescapeDataString(pair[(separator + 1)..]);

            result[Uri.UnescapeDataString(key)] = value;
        }

        return result;
    }
}

/// <summary>
/// Opens the direct-libsrt half of a forward: dials out as a caller, or waits as a listener, the
/// two shapes an SRT forward target's URL describes.
/// </summary>
internal static unsafe class SrtEgress
{
    /// <param name="defaultLatencyMs">
    /// Used when the target names none of its own, so an SRT forward behaves like every other
    /// connection this service makes rather than falling back to libsrt's own default silently.
    /// </param>
    /// <param name="cancellationToken">
    /// Honoured only while waiting for a puller in listener mode - the one libsrt call in this path
    /// that can block indefinitely with nothing else to interrupt it.
    /// </param>
    public static SrtSocketStream Open(string url, int defaultLatencyMs, CancellationToken cancellationToken)
    {
        var target = SrtTarget.Parse(url);

        Srt.EnsureStarted();

        var socket = Srt.srt_create_socket();

        if (socket == Srt.SRT_INVALID_SOCK)
        {
            throw new InvalidOperationException($"Could not create an SRT socket: {Srt.LastError()}");
        }

        try
        {
            // Pre-connect options only, the same rule the ingest listener follows: a connected or
            // accepted socket inherits whatever was set before the handshake and nothing set after
            // it.
            Srt.SetInt32(socket, SRT_SOCKOPT.SRTO_LATENCY, target.LatencyMs ?? defaultLatencyMs);

            if (target.Passphrase is { Length: > 0 } passphrase)
            {
                Srt.SetString(socket, SRT_SOCKOPT.SRTO_PASSPHRASE, passphrase);
            }

            return target.Listen
                ? OpenListener(socket, target, cancellationToken)
                : OpenCaller(socket, target);
        }
        catch
        {
            Srt.srt_close(socket);

            throw;
        }
    }

    private static SrtSocketStream OpenCaller(int socket, SrtTarget target)
    {
        if (target.StreamId is { Length: > 0 } streamId)
        {
            // The one option that names this connection to a listener on the other end - the far
            // end's own accept handler, our own ingest port included, reads this to decide what the
            // stream is called.
            Srt.SetString(socket, SRT_SOCKOPT.SRTO_STREAMID, streamId);
        }

        var address = Resolve(target.Host, target.Port).Serialize();

        fixed (byte* raw = address.Buffer.Span)
        {
            if (Srt.srt_connect(socket, raw, address.Size) != 0)
            {
                throw new InvalidOperationException(
                    $"Could not connect to '{target.Host}:{target.Port}': {Srt.LastError()}");
            }
        }

        return new SrtSocketStream(socket, writable: true);
    }

    /// <summary>
    /// Waits for the one puller this forward exists to serve, then closes the listening half: a
    /// second caller reaching this port is not this forward's business to accept, so the backlog is
    /// one and the socket stops listening the moment it has been used.
    /// </summary>
    private static SrtSocketStream OpenListener(int socket, SrtTarget target, CancellationToken cancellationToken)
    {
        Srt.SetBool(socket, SRT_SOCKOPT.SRTO_REUSEADDR, true);

        var address = new IPEndPoint(Resolve(target.Host, target.Port).Address, target.Port).Serialize();

        fixed (byte* raw = address.Buffer.Span)
        {
            if (Srt.srt_bind(socket, raw, address.Size) != 0)
            {
                throw new InvalidOperationException(
                    $"Could not bind '{target.Host}:{target.Port}' for a listening forward: {Srt.LastError()}");
            }
        }

        if (Srt.srt_listen(socket, 1) != 0)
        {
            throw new InvalidOperationException(
                $"Could not listen on '{target.Host}:{target.Port}': {Srt.LastError()}");
        }

        int accepted;

        // libsrt documents closing the listening socket from another thread as what unblocks a
        // blocked srt_accept, with SRT_ESCLOSED - the same proven mechanism the ingest listener
        // stops with, and the only handle this call gives on it.
        using (cancellationToken.Register(() => Srt.srt_close(socket)))
        {
            accepted = Srt.srt_accept(socket, null, null);
        }

        if (accepted == Srt.SRT_INVALID_SOCK)
        {
            var error = Srt.LastError();

            // Closed already when cancellation did it; a second close is an error return and
            // nothing more, the same tolerance the ingest listener's own shutdown relies on.
            Srt.srt_close(socket);

            cancellationToken.ThrowIfCancellationRequested();

            throw new InvalidOperationException(
                $"Waiting for a puller on '{target.Host}:{target.Port}' failed: {error}");
        }

        // One puller only.
        Srt.srt_close(socket);

        return new SrtSocketStream(accepted, writable: true);
    }

    private static IPEndPoint Resolve(string host, int port)
    {
        if (IPAddress.TryParse(host, out var literal))
        {
            return new IPEndPoint(literal, port);
        }

        var addresses = Dns.GetHostAddresses(host);

        var address = addresses.FirstOrDefault(candidate => candidate.AddressFamily == AddressFamily.InterNetwork)
            ?? addresses.FirstOrDefault()
            ?? throw new InvalidOperationException($"Could not resolve '{host}'.");

        return new IPEndPoint(address, port);
    }
}
