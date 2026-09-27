using System.Diagnostics.Metrics;

namespace StorageDemo.Api.Observability;

/// <summary>
/// What the two request surfaces publish about themselves, beyond what the framework already
/// publishes about them.
///
/// Deliberately small, because most of this question is already answered. ASP.NET Core's own meter
/// times and counts every request on both surfaces - a gRPC call is an HTTP/2 request and appears
/// there with its method as the route - and <c>Grpc.AspNetCore.Server</c> adds the gRPC status codes
/// that a trailer hides from the HTTP view. Duplicating either would be two numbers to keep honest
/// where one would do.
///
/// What is left is what a request-shaped meter cannot see. A server stream held open for a day is
/// one request with no duration to record until it ends, so it is counted as something that is
/// happening rather than something that happened. Bytes uploaded and downloaded are a storage
/// figure rather than a request figure, and they are the one thing a demo about storage providers
/// should be able to plot. And a call that this replica forwarded to another is invisible in a
/// duration histogram keyed by route, because both replicas answer the same route.
///
/// The same cardinality rule as <c>LiveMetrics</c>: every tag has a small, fixed set of values, and
/// no tag value is ever taken from what a caller sent. In particular nothing here is tagged with a
/// stream name or a document id.
/// </summary>
public sealed class ApiMetrics : IDisposable
{
    public const string MeterName = "StorageDemo.Api";

    private readonly Meter _meter;

    private readonly UpDownCounter<long> _streams;

    private readonly Counter<long> _messages;

    private readonly Counter<long> _uploads;

    private readonly Counter<long> _uploadBytes;

    private readonly Counter<long> _downloads;

    private readonly Counter<long> _downloadBytes;

    private readonly Counter<long> _peerCalls;

    public ApiMetrics()
    {
        _meter = new Meter(new MeterOptions(MeterName) { Scope = this });

        // The subscription RPCs: the change feed, the wall of live tiles, KLV and detections. Each
        // is one HTTP request for as long as a client is running, so a request rate says nothing
        // about them and a duration histogram only reports the ones that have already ended. How
        // many are open, and whether they are still sending, is the whole question.
        _streams = _meter.CreateUpDownCounter<long>(
            "api.grpc.streams.active",
            description: "Server-streaming calls open right now, by which RPC.");

        _messages = _meter.CreateCounter<long>(
            "api.grpc.stream.messages",
            unit: "{message}",
            description: "Messages written to a server stream, by which RPC.");

        _uploads = _meter.CreateCounter<long>(
            "api.documents.uploads",
            description: "Documents uploaded, by surface and by what came of it.");

        _uploadBytes = _meter.CreateCounter<long>(
            "api.documents.upload.bytes",
            unit: "By",
            description: "Bytes stored by uploads, as the stored document reports its own size.");

        _downloads = _meter.CreateCounter<long>(
            "api.documents.downloads",
            description: "Download requests, by surface, by what was asked for and by the answer.");

        // Only where this service copies the bytes itself, which is every gRPC download and a live
        // preview. A REST download hands a stream to Kestrel and may serve a range of it, so the
        // number here would be the document's size rather than what went over the wire; Kestrel's
        // own meter is the honest place for that, and inventing a second figure that disagrees with
        // it would be worse than not answering.
        _downloadBytes = _meter.CreateCounter<long>(
            "api.documents.download.bytes",
            unit: "By",
            description: "Bytes written to a caller by a streamed download.");

        // The in-cluster hop. Only the replica holding a stream can answer for it, so a call that
        // landed anywhere else is forwarded, and both replicas serve the same route: from the
        // framework's meter the two are indistinguishable. This is also where an owner that has
        // already gone shows up, as a registry entry naming a pod nothing can reach.
        _peerCalls = _meter.CreateCounter<long>(
            "api.peer.calls",
            description: "Calls forwarded to the replica owning a stream, by kind and by outcome.");
    }

    /// <summary>
    /// Marks a server stream open until the returned handle is disposed, and counts what it sent.
    /// </summary>
    /// <param name="rpc">
    /// The RPC's own name, from <see cref="ServerStreams"/> rather than from the request, so the tag
    /// stays a fixed set however many clients subscribe and whatever they subscribe about.
    /// </param>
    public ServerStream Streaming(string rpc) => new(this, rpc);

    public void Uploaded(string surface, string outcome, long bytes)
    {
        _uploads.Add(
            1,
            new KeyValuePair<string, object?>("surface", surface),
            new KeyValuePair<string, object?>("outcome", outcome));

        if (bytes > 0)
        {
            _uploadBytes.Add(bytes, new KeyValuePair<string, object?>("surface", surface));
        }
    }

    /// <param name="kind">
    /// What was asked for: <c>document</c>, <c>thumbnail</c> or <c>preview</c>. A live preview is a
    /// download like any other from here, and telling them apart is what shows a wall of tiles
    /// costing more than the files do.
    /// </param>
    /// <param name="outcome"><c>served</c> or <c>missing</c>.</param>
    public void Downloaded(string surface, string kind, string outcome)
        => _downloads.Add(
            1,
            new KeyValuePair<string, object?>("surface", surface),
            new KeyValuePair<string, object?>("kind", kind),
            new KeyValuePair<string, object?>("outcome", outcome));

    public void DownloadedBytes(string surface, string kind, long bytes)
    {
        if (bytes > 0)
        {
            _downloadBytes.Add(
                bytes,
                new KeyValuePair<string, object?>("surface", surface),
                new KeyValuePair<string, object?>("kind", kind));
        }
    }

    /// <param name="kind">
    /// <c>control</c> for a call repeated against the owner, <c>fetch</c> for a JSON answer read
    /// back, <c>media</c> for a stream of bytes opened from it.
    /// </param>
    /// <param name="outcome">
    /// <c>ok</c>, <c>refused</c> where the owner answered something other than success,
    /// <c>unreachable</c> where it could not be reached at all, and <c>no-address</c> where the
    /// registry entry carries no address to try - which is a missing <c>Live:PeerBaseUrl</c> rather
    /// than a network fault, and the two are worth telling apart at three in the morning.
    /// </param>
    public void Peered(string kind, string outcome)
        => _peerCalls.Add(
            1,
            new KeyValuePair<string, object?>("kind", kind),
            new KeyValuePair<string, object?>("outcome", outcome));

    public void Dispose() => _meter.Dispose();

    /// <summary>One open server stream, counted for exactly as long as it is held.</summary>
    public sealed class ServerStream : IDisposable
    {
        private readonly ApiMetrics _metrics;
        private readonly KeyValuePair<string, object?> _rpc;
        private bool _closed;

        internal ServerStream(ApiMetrics metrics, string rpc)
        {
            _metrics = metrics;
            _rpc = new KeyValuePair<string, object?>("rpc", rpc);

            _metrics._streams.Add(1, _rpc);
        }

        public void Sent() => _metrics._messages.Add(1, _rpc);

        public void Dispose()
        {
            // Guarded, because an up-down counter has no way back from a double decrement: a handle
            // disposed twice would leave this replica reporting fewer open streams than zero for as
            // long as the process ran.
            if (_closed)
            {
                return;
            }

            _closed = true;

            _metrics._streams.Add(-1, _rpc);
        }
    }
}

/// <summary>
/// The names the streaming RPCs are counted under. Constants rather than <c>nameof</c> at the call
/// site, so renaming a method cannot silently split one time series into two.
/// </summary>
public static class ServerStreams
{
    public const string Changes = "Watch";

    public const string LiveStreams = "WatchLiveStreams";

    public const string LiveKlv = "WatchLiveKlv";

    public const string LiveDetections = "WatchLiveDetections";
}
