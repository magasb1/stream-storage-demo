using System.Threading.Channels;

namespace StorageDemo.Core.Documents;

/// <summary>A queue inside one process.</summary>
public sealed class InMemoryAnalysisQueue : IAnalysisQueue
{
    private readonly Channel<AnalysisRequest> _channel =
        Channel.CreateUnbounded<AnalysisRequest>(new UnboundedChannelOptions { SingleReader = true });

    public Task EnqueueAsync(AnalysisRequest request, CancellationToken cancellationToken = default)
    {
        _channel.Writer.TryWrite(request);
        return Task.CompletedTask;
    }

    public IAsyncEnumerable<AnalysisRequest> DequeueAllAsync(CancellationToken cancellationToken)
        => _channel.Reader.ReadAllAsync(cancellationToken);
}
