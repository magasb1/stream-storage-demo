using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>A connection that has been accepted and named, with the socket still open.</summary>
public sealed class AcceptedSocket(int socket, string streamId, string name) : IDisposable
{
    private int _socket = socket;

    /// <summary>The raw identifier, kept for logs and for telling one attempt from the next.</summary>
    public string StreamId { get; } = streamId;

    public string Name { get; } = name;

    /// <summary>Hands the open socket over.</summary>
    public int Release()
    {
        var taken = _socket;
        _socket = Srt.SRT_INVALID_SOCK;

        return taken;
    }

    public void Dispose()
    {
        if (Release() is var taken && taken != Srt.SRT_INVALID_SOCK)
        {
            Srt.srt_close(taken);
        }
    }
}

/// <summary>
/// Everything known about a caller before a connection exists: the handshake carries the identifier
/// and nothing else.
/// </summary>
public readonly record struct Admission(string Name, string StreamId, StreamIntent Intent);

/// <summary>Whether this caller may connect.</summary>
public delegate int? Admit(Admission admission);

/// <summary>One listening SRT port, owned outright rather than borrowed from libav.</summary>
public sealed unsafe class SrtListener(
    StreamIntent intent,
    LiveOptions options,
    Admit admit,
    Action<AcceptedSocket> onAccepted,
    ILogger logger,
    LiveListeners? listeners = null,
    LiveMetrics? metrics = null)
{
    /// <summary>
    /// One instance serves every port, so what the handshake hook needs to know about the port it
    /// is answering for travels beside the instance rather than on it.
    /// </summary>
    private sealed record Bound(SrtListener Listener, int Port);

    /// <summary>Deep enough that a cold start of a thousand encoders never meets a full queue.</summary>
    private const int Backlog = 128;

    /// <summary>How long a blocked read may sit before it looks up.</summary>
    private const int ReceiveTimeoutMilliseconds = 1000;

    private string PortName => intent == StreamIntent.Publish ? "ingest" : "consumption";

    /// <summary>
    /// Binds, listens and accepts until cancelled, handing every named caller to <c>onAccepted</c>.
    /// </summary>
    public void Run(int port, CancellationToken cancellationToken)
    {
        Srt.EnsureStarted();

        var listener = Srt.srt_create_socket();

        if (listener == Srt.SRT_INVALID_SOCK)
        {
            logger.LogError("Could not create the {Port} socket: {Error}", PortName, Srt.LastError());

            return;
        }

        // [UnmanagedCallersOnly] forbids capturing anything, so libsrt's hook_opaque is the only
        // road from the static hook back to this instance and to which port it is answering for.
        var self = GCHandle.Alloc(new Bound(this, port));

        try
        {
            if (!Open(listener, port, self))
            {
                return;
            }

            // A fact rather than a freshness window: the port is bound or the call above failed.
            listeners?.Bound(intent);

            logger.LogInformation(
                "Listening for SRT callers on {Address}:{Port}, the {Which} port",
                options.IngestAddress,
                port,
                PortName);

            // srt_accept blocks, and libsrt documents closing the listening socket from another
            // thread as what unblocks it, with SRT_ESCLOSED.
            using (cancellationToken.Register(() => Srt.srt_close(listener)))
            {
                Accept(listener, port, cancellationToken);
            }
        }
        finally
        {
            // Closed already when cancellation did it, and a second close is an error return and
            // nothing more.
            Srt.srt_close(listener);

            // Past the close no further handshake can reach the hook, so the handle it travels in
            // is safe to release.
            self.Free();

            listeners?.Stopped(intent);

            logger.LogInformation("Stopped listening on the {Which} port", PortName);
        }
    }

    private bool Open(int listener, int port, GCHandle self)
    {
        // Pre-bind, so it has to be set before srt_bind rather than beside the others.
        Srt.SetBool(listener, SRT_SOCKOPT.SRTO_REUSEADDR, true);

        // What makes a feed go interrupted instead of holding a socket nothing arrives on.
        Srt.SetInt32(listener, SRT_SOCKOPT.SRTO_PEERIDLETIMEO, options.FeedTimeoutSeconds * 1000);

        Srt.SetInt32(listener, SRT_SOCKOPT.SRTO_RCVTIMEO, ReceiveTimeoutMilliseconds);

        // SRTO_LATENCY rather than SRTO_RCVLATENCY, because it sets the peer half too and the two
        // ports sit on opposite ends of the negotiation: ingest receives, consumption sends, and
        // per direction the effective figure is the larger of the receiver's own latency and the
        // sender's peer latency.
        Srt.SetInt32(listener, SRT_SOCKOPT.SRTO_LATENCY, options.SrtLatencyMs);

        // srt_bind wants the raw sockaddr, and a serialised endpoint is exactly that, laid out the
        // way the running platform lays it out.
        var address = new IPEndPoint(IPAddress.Parse(options.IngestAddress), port).Serialize();

        fixed (byte* raw = address.Buffer.Span)
        {
            if (Srt.srt_bind(listener, raw, address.Size) != 0)
            {
                logger.LogError(
                    "Could not bind the {Which} port on {Address}:{Port}: {Error}",
                    PortName,
                    options.IngestAddress,
                    port,
                    Srt.LastError());

                return false;
            }
        }

        // Before srt_listen or it is never consulted.
        if (Srt.srt_listen_callback(listener, &OnHandshake, (void*)GCHandle.ToIntPtr(self)) != 0)
        {
            logger.LogError(
                "Could not install the handshake hook on the {Which} port: {Error}",
                PortName,
                Srt.LastError());

            return false;
        }

        if (Srt.srt_listen(listener, Backlog) != 0)
        {
            logger.LogError("Could not listen on the {Which} port: {Error}", PortName, Srt.LastError());

            return false;
        }

        return true;
    }

    private void Accept(int listener, int port, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var socket = Srt.srt_accept(listener, null, null);

            if (socket != Srt.SRT_INVALID_SOCK)
            {
                Handle(socket, port);

                continue;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            if (Srt.LastErrorCode() == Srt.SRT_ETIMEOUT)
            {
                // Whether srt_accept honours SRTO_RCVTIMEO is documented neither way.
                continue;
            }

            // Nothing srt_accept reports on a listening socket is transient, so retrying would be a
            // hot loop on a port that is gone.
            logger.LogError(
                "The {Which} port stopped accepting: {Error}",
                PortName,
                Srt.LastError());

            break;
        }
    }

    /// <summary>Names an accepted socket and hands it over, or closes it and says why.</summary>
    private void Handle(int socket, int port)
    {
        var streamId = Srt.GetString(socket, SRT_SOCKOPT.SRTO_STREAMID);

        if (streamId is null)
        {
            logger.LogError(
                "A caller was accepted on the {Which} port but SRTO_STREAMID could not be read back, "
                + "so it cannot be named: {Error}",
                PortName,
                Srt.LastError());

            Srt.srt_close(socket);

            return;
        }

        if (!StreamName.TryParse(streamId, out var name, out var rejection, intent))
        {
            logger.LogWarning("Dropped a caller the handshake had admitted: {Rejection}", rejection);

            Srt.srt_close(socket);

            return;
        }

        if (StreamName.CarriesSessionKey(streamId))
        {
            // Never the value.
            logger.LogDebug("The identifier for '{Name}' carries a session key, which is ignored today", name);
        }

        Srt.SetInt32(socket, SRT_SOCKOPT.SRTO_RCVTIMEO, ReceiveTimeoutMilliseconds);

        // What the handshake settled on, not what was asked for: the larger of the two sides wins,
        // so a caller that knows its link can raise this and an operator should be able to see that
        // it did.
        var negotiated = Srt.GetInt32(
            socket,
            intent == StreamIntent.Publish ? SRT_SOCKOPT.SRTO_RCVLATENCY : SRT_SOCKOPT.SRTO_PEERLATENCY);

        logger.LogInformation(
            "Accepted '{Name}' on the {Which} port at {Latency} ms of SRT latency",
            name,
            PortName,
            negotiated);

        metrics?.Accepted(port);

        using var accepted = new AcceptedSocket(socket, streamId, name);

        try
        {
            onAccepted(accepted);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Handing over the accepted stream '{Name}' failed", name);
        }
    }

    /// <summary>
    /// libsrt's <c>srt_listen_callback_fn</c>, on libsrt's receiver worker thread, after the
    /// caller's conclusion handshake and before the connection exists.
    /// </summary>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int OnHandshake(void* opaque, int socket, int handshakeVersion, void* peer, byte* streamId)
    {
        try
        {
            return GCHandle.FromIntPtr((IntPtr)opaque).Target is Bound bound
                ? bound.Listener.Handshake(socket, bound.Port, Marshal.PtrToStringUTF8((IntPtr)streamId))
                : -1;
        }
        catch (Exception)
        {
            return -1;
        }
    }

    private int Handshake(int socket, int port, string? streamId)
    {
        if (!StreamName.TryParse(streamId, out var name, out _, intent))
        {
            // The rejection text is not logged here.
            return Reject(socket, port, Srt.SRT_REJX_BAD_REQUEST);
        }

        var refusal = admit(new Admission(name, streamId!, intent));

        return refusal is null ? 0 : Reject(socket, port, refusal.Value);
    }

    private int Reject(int socket, int port, int code)
    {
        Srt.srt_setrejectreason(socket, code);

        metrics?.Rejected(port, Reason(code));

        return -1;
    }

    /// <summary>The rejection as a word, from a closed set.</summary>
    private static string Reason(int code) => code switch
    {
        Srt.SRT_REJX_BAD_REQUEST => "bad-request",
        Srt.SRT_REJX_OVERLOAD => "overload",
        Srt.SRT_REJX_CONFLICT => "conflict",
        _ => "other",
    };
}
