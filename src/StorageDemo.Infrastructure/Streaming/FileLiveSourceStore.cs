using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>Configured sources in a JSON file, which is what standalone runs on.</summary>
public sealed class FileLiveSourceStore : ILiveSourceStore
{
    private readonly string _path;
    private readonly ILogger<FileLiveSourceStore> _logger;

    // One lock over both the load and every save.
    private readonly SemaphoreSlim _gate = new(1, 1);

    private Dictionary<string, LiveSource>? _sources;

    public FileLiveSourceStore(IOptions<LiveOptions> options, ILogger<FileLiveSourceStore> logger)
    {
        var live = options.Value;

        // Resolved here rather than as a property initialiser because it depends on another
        // property, and an initialiser would capture the default recording directory even when the
        // deployment configured one.
        _path = live.SourceFile is { Length: > 0 } configured
            ? configured
            : Path.Combine(
                live.RecordingDirectory is { Length: > 0 } directory
                    ? directory
                    : Path.Combine(Path.GetTempPath(), "storagedemo-live"),
                "sources.json");

        _logger = logger;
    }

    public async Task<IReadOnlyList<LiveSource>> ListAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            return [.. Load().Values.OrderBy(source => source.Name, StringComparer.Ordinal)];
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<LiveSource?> GetAsync(string name, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            return Load().GetValueOrDefault(name);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task SaveAsync(LiveSource source, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            var sources = new Dictionary<string, LiveSource>(Load(), StringComparer.Ordinal)
            {
                [source.Name] = source,
            };

            await CommitAsync(sources, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task RemoveAsync(string name, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            var sources = new Dictionary<string, LiveSource>(Load(), StringComparer.Ordinal);

            if (sources.Remove(name))
            {
                await CommitAsync(sources, cancellationToken);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>The in-memory copy, read from disk the first time anything asks.</summary>
    private Dictionary<string, LiveSource> Load()
    {
        if (_sources is not null)
        {
            return _sources;
        }

        return _sources = Read().ToDictionary(source => source.Name, StringComparer.Ordinal);
    }

    private List<LiveSource> Read()
    {
        if (!File.Exists(_path))
        {
            return [];
        }

        try
        {
            return JsonSerializer.Deserialize<List<LiveSource>>(File.ReadAllText(_path)) ?? [];
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            // Starting empty rather than throwing, because refusing to start over an unreadable
            // configuration file takes the whole service down - including every stream an encoder
            // is pushing, which needs no source at all.
            var quarantine = _path + ".corrupt";

            try
            {
                File.Move(_path, quarantine, overwrite: true);
                _logger.LogError(ex, "Unreadable source file, moved to '{Path}' and starting empty", quarantine);
            }
            catch (Exception moveFailed) when (moveFailed is IOException or UnauthorizedAccessException)
            {
                _logger.LogError(ex, "Unreadable source file '{Path}', starting empty", _path);
                _logger.LogError(moveFailed, "Could not move the unreadable source file aside");
            }

            return [];
        }
    }

    /// <summary>Writes the new list, and only then adopts it in memory.</summary>
    private async Task CommitAsync(
        Dictionary<string, LiveSource> sources,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_path)!);

        var temporary = _path + ".tmp";
        var ordered = sources.Values.OrderBy(source => source.Name, StringComparer.Ordinal);

        // Indented: this file is meant to be read, and edited in anger, by a person.
        await File.WriteAllTextAsync(
            temporary,
            JsonSerializer.Serialize(ordered, IndentedJson),
            cancellationToken);

        File.Move(temporary, _path, overwrite: true);

        _sources = sources;
    }

    private static readonly JsonSerializerOptions IndentedJson = new() { WriteIndented = true };
}
