using System.Collections.Concurrent;

namespace StorageDemo.Core.Streaming;

/// <summary>One replica's view, which is the whole cluster when there is only one.</summary>
public sealed class InMemoryLiveStreamRegistry : ILiveStreamRegistry
{
    private readonly ConcurrentDictionary<string, LiveStream> _streams = new(StringComparer.Ordinal);

    public Task UpsertAsync(LiveStream stream, CancellationToken cancellationToken = default)
    {
        _streams[stream.Name] = stream;
        return Task.CompletedTask;
    }

    public Task RemoveAsync(string name, CancellationToken cancellationToken = default)
    {
        _streams.TryRemove(name, out _);
        return Task.CompletedTask;
    }

    public Task<LiveStream?> GetAsync(string name, CancellationToken cancellationToken = default)
        => Task.FromResult(_streams.GetValueOrDefault(name));

    public Task<IReadOnlyList<LiveStream>> ListAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<LiveStream>>(
            [.. _streams.Values.OrderBy(stream => stream.Name, StringComparer.Ordinal)]);
}
