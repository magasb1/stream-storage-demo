using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Grpc;

/// <summary>
/// The same guard the REST live routes apply, for the live RPCs: the configured token, carried as
/// metadata under the header's name, compared in fixed time.
/// </summary>
public sealed class LiveTokenInterceptor(IOptions<LiveOptions> options) : Interceptor
{
    public const string Header = "x-storage-token";

    private static readonly HashSet<string> Guarded =
    [
        "ListLive", "WatchLiveStreams", "GetLiveKlv", "WatchLiveKlv", "SnapshotLive", "RecordLive", "StopLiveRecording", "SetLiveDetection",
        "GetLiveDetections", "WatchLiveDetections", "ListLiveSources", "SaveLiveSource", "DeleteLiveSource",
    ];

    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        Guard(context);
        return continuation(request, context);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Guard(context);
        return continuation(request, responseStream, context);
    }

    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Guard(context);
        return continuation(requestStream, context);
    }

    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Guard(context);
        return continuation(requestStream, responseStream, context);
    }

    private void Guard(ServerCallContext context)
    {
        var live = options.Value;

        if (!live.Enabled
            || string.IsNullOrEmpty(live.Token)
            || !Guarded.Contains(context.Method[(context.Method.LastIndexOf('/') + 1)..]))
        {
            return;
        }

        var provided = Encoding.UTF8.GetBytes(context.RequestHeaders.GetValue(Header) ?? string.Empty);
        var expected = Encoding.UTF8.GetBytes(live.Token);

        if (!CryptographicOperations.FixedTimeEquals(provided, expected))
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Bad token."));
        }
    }
}
