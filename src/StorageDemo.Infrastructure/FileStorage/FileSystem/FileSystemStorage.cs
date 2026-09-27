using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Storage;

namespace StorageDemo.Infrastructure.FileStorage.FileSystem;

/// <summary>Stores objects as files under a configured root.</summary>
public sealed class FileSystemStorage : IFileStorage
{
    /// <summary>The buffer size Stream.CopyTo uses by default.</summary>
    private const int BufferSize = 81920;

    private readonly string _root;

    public FileSystemStorage(IOptions<FileSystemStorageOptions> options)
    {
        _root = Path.GetFullPath(options.Value.RootPath);
        Directory.CreateDirectory(_root);
    }

    public async Task SaveAsync(
        string key,
        Stream content,
        string? contentType,
        CancellationToken cancellationToken = default)
    {
        var path = ResolvePath(key);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        try
        {
            await using var target = new FileStream(
                path,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: BufferSize,
                useAsync: true);

            await content.CopyToAsync(target, cancellationToken);
        }
        catch (IOException ex)
        {
            throw new StorageException($"Failed to write '{key}'.", ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            throw new StorageException($"Failed to write '{key}'.", ex);
        }
    }

    public Task<Stream?> OpenReadAsync(string key, CancellationToken cancellationToken = default)
        => OpenReadAsync(key, 0, cancellationToken);

    public Task<Stream?> OpenReadAsync(
        string key,
        long offset,
        CancellationToken cancellationToken = default)
    {
        var path = ResolvePath(key);
        if (!File.Exists(path))
        {
            return Task.FromResult<Stream?>(null);
        }

        try
        {
            Stream stream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: BufferSize,
                useAsync: true);

            if (offset > 0)
            {
                stream.Seek(offset, SeekOrigin.Begin);
            }

            return Task.FromResult<Stream?>(stream);
        }
        catch (IOException ex)
        {
            throw new StorageException($"Failed to read '{key}'.", ex);
        }
    }

    public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        var path = ResolvePath(key);

        try
        {
            File.Delete(path); // No-op when missing, which is the idempotency the contract wants.
        }
        catch (IOException ex)
        {
            throw new StorageException($"Failed to delete '{key}'.", ex);
        }

        return Task.CompletedTask;
    }

    public Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
        => Task.FromResult(File.Exists(ResolvePath(key)));

    public async IAsyncEnumerable<StorageObject> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var start = string.IsNullOrWhiteSpace(prefix) ? _root : ResolvePath(prefix.TrimEnd('/'));
        if (!Directory.Exists(start))
        {
            yield break;
        }

        foreach (var path in Directory.EnumerateFiles(start, "*", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var info = new FileInfo(path);
            var key = Path.GetRelativePath(_root, path).Replace(Path.DirectorySeparatorChar, '/');

            yield return new StorageObject(key, info.Length, info.LastWriteTimeUtc);
        }

        await Task.CompletedTask; // Enumeration is synchronous; the async shape is for S3's sake.
    }

    public bool RootIsAccessible() => Directory.Exists(_root);

    /// <summary>Maps a logical key to a physical path and refuses anything escaping the root.</summary>
    private string ResolvePath(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (Path.IsPathRooted(key) || key.Contains(':'))
        {
            throw new StorageException($"Invalid storage key '{key}'.");
        }

        var full = Path.GetFullPath(Path.Combine(_root, key.Replace('\\', '/')));

        // Compare with the separator appended so "/data/files-other" cannot pass as "/data/files".
        var rootPrefix = _root.EndsWith(Path.DirectorySeparatorChar)
            ? _root
            : _root + Path.DirectorySeparatorChar;

        if (!full.StartsWith(rootPrefix, StringComparison.Ordinal))
        {
            throw new StorageException($"Invalid storage key '{key}'.");
        }

        return full;
    }
}
