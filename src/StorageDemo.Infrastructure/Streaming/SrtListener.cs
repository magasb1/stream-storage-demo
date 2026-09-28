using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// A connection that has been accepted and named, with the socket still open. Whoever handles it
/// either takes the socket with <see cref="Release"/> or disposes it, which drops the caller.
/// </summary>
public sealed class AcceptedSocket(int socket, string streamId, string name) : IDisposable
{
    private int _socket = socket;

    /// <summary>The raw identifier, kept for logs and for telling one attempt from the next.</summary>
    public string StreamId { get; } = streamId;

    public string Name { get; } = name;

    /// <summary>Hands the open socket over. The caller owns closing it from then on.</summary>
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
/// and nothing else. A struct, because one is made on libsrt's receiver thread for every caller.
/// </summary>
public readonly record struct Admission(string Name, string StreamId, StreamIntent Intent);

/// <summary>
/// Whether this caller may connect. Null admits; anything else is an <c>SRT_REJX</c> code from
/// <see cref="Srt"/>, which travels intact to the caller's own <c>srt_getrejectreason</c>.
///
/// It runs on libsrt's receiver worker thread, the one carrying every packet for every socket on
/// the port, so it must not block, allocate a socket or wait on anything.
/// </summary>
public delegate int? Admit(Admission admission);

/// <summary>
/// One listening SRT port, owned outright rather than borrowed from libav.
///
/// libav's listener accepts a single caller per bind and only reveals the name afterwards, which
/// capped this service at a couple of accepts a second and left nothing refusable before the
/// connection existed. Calling libsrt directly buys both: a real backlog, and a handshake hook that
/// sees the identifier while the answer can still be no.
///
/// The hook is the reason this class exists, and it is also the one place in the service that must
/// never be slow. It parses a short string and asks <see cref="Admit"/>. Everything expensive -
/// logging, claiming a name, starting a demultiplexer - happens on this thread, after the accept.
/// </summary>
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

    /// <summary>
    /// Deep enough that a cold start of a thousand encoders never meets a full queue. It costs a
    /// pending-connection slot each, which is why it is not larger still.
    /// </summary>
    private const int Backlog = 128;

    /// <summary>
    /// How long a blocked read may sit before it looks up. Set on every accepted socket rather than
    /// left to inheritance, because libsrt only promises that a timeout option is "usually derived",
    /// and without it a shutdown waits out SRTO_PEERIDLETIMEO instead of a second.
    /// </summary>
    private const int ReceiveTimeoutMilliseconds = 1000;

    /// <summary>
    /// How often a blocked send looks up. A const rather than an option, because measurement says the
    /// value decides nothing: with it set, a peer advertising too-late-packet drop stalls about a
    /// second and recovers - that is libsrt's drop threshold, max(latency + SRTO_SNDDROPDELAY, 1000)
    /// + 20 ms, not this - and a peer that cleared the flag never recovers at any setting: probing
    /// libsrt 1.5.3 directly for #20 gave sixteen unbroken seconds of timeouts in a sixteen-second
    /// window. The two cases stay two orders of magnitude apart however this is set, so what a
    /// stalled viewer costs is decided by
    /// <see cref="LiveOptions.ViewerSendStallSeconds"/> in <c>SrtSocketStream.Write</c>. Exposing this
    /// as well would only invite an operator to tune the knob that does nothing.
    ///
    /// A second, matching <see cref="ReceiveTimeoutMilliseconds"/>, so a blocked send checks its
    /// budget about as often as a blocked read checks for a shutdown.
    /// </summary>
    private const int SendTimeoutMilliseconds = 1000;

    private string PortName => intent == StreamIntent.Publish ? "ingest" : "consumption";

    /// <summary>
    /// Binds, listens and accepts until cancelled, handing every named caller to
    /// <c>onAccepted</c>. Blocking, and meant for a thread of its own.
    ///
    /// The handler must not block: it is on the accept thread, and every millisecond spent there is
    /// a millisecond the port is not accepting.
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
            // thread as what unblocks it, with SRT_ESCLOSED. There is no other handle on the call.
            using (cancellationToken.Register(() => Srt.srt_close(listener)))
            {
                Accept(listener, port, cancellationToken);
            }
        }
        finally
        {
            // Closed already when cancellation did it, and a second close is an error return and
            // nothing more. The failure paths above have no other way out.
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
        // Pre-bind, so it has to be set before srt_bind rather than beside the others. Both media
        // ports are bound in one process and sharing a multiplexer per port is what keeps each to
        // one UDP socket and one receiver thread.
        Srt.SetBool(listener, SRT_SOCKOPT.SRTO_REUSEADDR, true);

        // What makes a feed go interrupted instead of holding a socket nothing arrives on.
        Srt.SetInt32(listener, SRT_SOCKOPT.SRTO_PEERIDLETIMEO, options.FeedTimeoutSeconds * 1000);

        Srt.SetInt32(listener, SRT_SOCKOPT.SRTO_RCVTIMEO, ReceiveTimeoutMilliseconds);

        // SRTO_LATENCY rather than SRTO_RCVLATENCY, because it sets the peer half too and the two
        // ports sit on opposite ends of the negotiation: ingest receives, consumption sends, and
        // per direction the effective figure is the larger of the receiver's own latency and the
        // sender's peer latency. Set here, with the other pre-connect options, because an accepted
        // socket inherits what the listener had before srt_listen and nothing set after it.
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
                // Whether srt_accept honours SRTO_RCVTIMEO is documented neither way. If it does,
                // this is the quiet second it was; if it does not, this never runs.
                continue;
            }

            // Nothing srt_accept reports on a listening socket is transient, so retrying would be a
            // hot loop on a port that is gone. Readiness sees it through Stopped on the way out.
            logger.LogError(
                "The {Which} port stopped accepting: {Error}",
                PortName,
                Srt.LastError());

            break;
        }
    }

    /// <summary>
    /// Names an accepted socket and hands it over, or closes it and says why.
    ///
    /// The name is read off the socket and parsed a second time rather than stashed by the hook.
    /// SRTO_STREAMID is the one option an accepted socket does not inherit, so it is already there
    /// for the asking, and two parses of a short string cost less than a dictionary keyed by socket
    /// that every rejected handshake would have to clean up.
    ///
    /// The two failures are deliberately distinct. No identifier at all means libsrt would not
    /// answer for a socket it has just handed over, which is an operator's problem and would make
    /// every stream arrive unnamed; an identifier that will not parse is a sender's problem, and
    /// here it is also a surprise, because the hook parsed the same string and admitted it.
    /// </summary>
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
            // Never the value. It is the slot a push token will occupy.
            logger.LogDebug("The identifier for '{Name}' carries a session key, which is ignored today", name);
        }

        Srt.SetInt32(socket, SRT_SOCKOPT.SRTO_RCVTIMEO, ReceiveTimeoutMilliseconds);

        if (intent == StreamIntent.Subscribe)
        {
            // Viewers only. A send on an ingest socket is the handshake's business and nothing
            // here writes to one, whereas a viewer's socket is the one a stranger can stop reading
            // from. libsrt's default is -1, which is a send that waits for as long as the peer
            // likes; this turns the wait into a poll, and SrtSocketStream.Write decides what to do
            // with it. Post-accept rather than on the listener, because only half of what this
            // listener accepts should have it.
            //
            // SRTO_SNDBUF is deliberately not set beside it - see
            // LiveOptions.ViewerSendStallSeconds for the measurement that rules it out.
            Srt.SetInt32(socket, SRT_SOCKOPT.SRTO_SNDTIMEO, SendTimeoutMilliseconds);
        }

        // What the handshake settled on, not what was asked for: the larger of the two sides wins,
        // so a caller that knows its link can raise this and an operator should be able to see that
        // it did. Whichever half describes the receiver of this port's direction is the one that
        // means anything - ingest receives, consumption sends to a receiver at the far end.
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
    ///
    /// Nothing may escape into C: an exception crossing that boundary is undefined, and a caller
    /// refused because the hook faulted retries, while a torn-down process does not. The bare -1
    /// leaves libsrt its own rejection reason, which is the most a hook that does not know why it
    /// failed can honestly say.
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
            // The rejection text is not logged here. This is the packet thread, and a caller who
            // spells a name wrong retries, so the line would come a thousand at a time. It is
            // counted instead, which is the same information without the thousand lines.
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

    /// <summary>
    /// The rejection as a word, from a closed set. Never the identifier that was refused and never
    /// the code as text: a tag whose values a caller can choose is a tag that can be made to cost
    /// whatever the caller likes.
    /// </summary>
    private static string Reason(int code) => code switch
    {
        Srt.SRT_REJX_BAD_REQUEST => "bad-request",
        Srt.SRT_REJX_OVERLOAD => "overload",
        Srt.SRT_REJX_CONFLICT => "conflict",
        _ => "other",
    };
}
