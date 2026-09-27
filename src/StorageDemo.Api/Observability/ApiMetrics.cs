using System.Diagnostics.Metrics;

namespace StorageDemo.Api.Observability;

/// <summary>
/// What the two request surfaces publish about themselves, beyond what the framework already
/// publishes about them.
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

        _downloadBytes = _meter.CreateCounter<long>(
            "api.documents.download.bytes",
            unit: "By",
            description: "Bytes written to a caller by a streamed download.");

        _peerCalls = _meter.CreateCounter<long>(
            "api.peer.calls",
            description: "Calls forwarded to the replica owning a stream, by kind and by outcome.");
    }

    /// <summary>
    /// Marks a server stream open until the returned handle is disposed, and counts what it sent.
    /// </summary>
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

    /// <param name="kind">What was asked for: <c>document</c>, <c>thumbnail</c> or <c>preview</c>.</param>
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

/// <summary>The names the streaming RPCs are counted under.</summary>
public static class ServerStreams
{
    public const string Changes = "Watch";

    public const string LiveStreams = "WatchLiveStreams";

    public const string LiveKlv = "WatchLiveKlv";

    public const string LiveDetections = "WatchLiveDetections";
}
