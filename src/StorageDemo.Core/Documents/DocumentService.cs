using Microsoft.Extensions.Logging;
using StorageDemo.Core.Storage;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Core.Documents;

/// <summary>
/// Coordinates the two independent infrastructure concerns: bytes in <see cref="IFileStorage"/>,
/// metadata in <see cref="IDocumentRepository"/>.
/// </summary>
public sealed class DocumentService(
    IFileStorage fileStorage,
    IDocumentRepository repository,
    IMediaAnalyzer mediaAnalyzer,
    IAnalysisQueue analysisQueue,
    IChangeFeed changeFeed,
    ILogger<DocumentService> logger) : IDocumentService
{
    /// <summary>Thumbnails live outside the documents prefix so the monitor never imports them.</summary>
    private const string ThumbnailPrefix = "thumbnails/";

    public async Task<Document> UploadAsync(
        string fileName,
        Stream content,
        string? contentType,
        CancellationToken cancellationToken = default,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
        ArgumentNullException.ThrowIfNull(content);

        var id = Guid.NewGuid();
        var safeName = SanitizeFileName(fileName);
        var storageKey = $"documents/{id}/{safeName}";

        // Counted on the way through, so an unseekable gRPC or HTTP upload still records a size.
        var counted = new CountingStream(content);
        await fileStorage.SaveAsync(storageKey, counted, contentType, cancellationToken);

        var document = new Document
        {
            Id = id,
            FileName = safeName,
            StorageKey = storageKey,
            ContentType = contentType,
            Size = counted.BytesRead,
            CreatedAt = DateTimeOffset.UtcNow,
            Metadata = metadata ?? new Dictionary<string, string>(),
        };

        try
        {
            await repository.AddAsync(document, cancellationToken);
        }
        catch
        {
            // Compensate: the file is stored but nothing points at it.
            await TryDeleteAsync(storageKey, id);
            throw;
        }

        logger.LogInformation(
            "Document uploaded {DocumentId} {StorageKey} {Size}",
            document.Id,
            document.StorageKey,
            document.Size);

        changeFeed.Publish(new DocumentChange(
            ChangeKind.Added,
            document.Id,
            document.StorageKey,
            document.FileName));

        // Probing and thumbnailing happen after the caller is answered.
        if (mediaAnalyzer.CanAnalyze(contentType, safeName))
        {
            await analysisQueue.EnqueueAsync(
                new AnalysisRequest(id, storageKey, safeName, contentType),
                cancellationToken);
        }

        return document;
    }

    /// <summary>Reads the object back out of storage to probe it and render a preview.</summary>
    public async Task<StoredAnalysis> AnalyzeStoredObjectAsync(
        Guid id,
        string storageKey,
        string fileName,
        string? contentType,
        CancellationToken cancellationToken = default)
    {
        if (!mediaAnalyzer.CanAnalyze(contentType, fileName))
        {
            return StoredAnalysis.None;
        }

        MediaAnalysis analysis;
        try
        {
            // A segmented document is probed through its first piece rather than its whole self.
            var document = await repository.GetAsync(id, cancellationToken);
            var key = document is { Segmented: true } ? document.Parts[0].Key : storageKey;

            await using var stored = await fileStorage.OpenReadAsync(key, cancellationToken);
            if (stored is null)
            {
                return StoredAnalysis.None;
            }

            analysis = await mediaAnalyzer.AnalyzeAsync(stored, fileName, contentType, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A missing or unhappy ffmpeg must not fail an upload; the file is already stored.
            logger.LogWarning(ex, "Media analysis failed for {StorageKey}", storageKey);
            return StoredAnalysis.None;
        }

        if (analysis.Thumbnail is null || analysis.Thumbnail.Length == 0)
        {
            return new StoredAnalysis(analysis.Metadata, null);
        }

        var thumbnailKey = $"{ThumbnailPrefix}{id}.jpg";

        try
        {
            using var thumbnail = new MemoryStream(analysis.Thumbnail);
            await fileStorage.SaveAsync(thumbnailKey, thumbnail, "image/jpeg", cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Could not store the thumbnail for {StorageKey}", storageKey);
            return new StoredAnalysis(analysis.Metadata, null);
        }

        return new StoredAnalysis(analysis.Metadata, thumbnailKey);
    }

    public Task<Document?> GetAsync(Guid id, CancellationToken cancellationToken = default)
        => repository.GetAsync(id, cancellationToken);

    public Task<IReadOnlyList<Document>> GetAllAsync(CancellationToken cancellationToken = default)
        => repository.GetAllAsync(cancellationToken);

    public async Task<IReadOnlyList<Document>> FindByDetectionAsync(
        DetectionReference detection,
        CancellationToken cancellationToken = default)
    {
        var reference = detection.ToString();

        // ponytail: a scan of the listing, because the reference is one string inside a metadata
        // blob and neither store indexes into it.
        return [.. (await repository.GetAllAsync(cancellationToken))
            .Where(document =>
                document.Metadata.TryGetValue(DetectionReference.MetadataKey, out var value)
                && string.Equals(value, reference, StringComparison.Ordinal))];
    }

    public SegmentedDocument BeginSegmented(
        string fileName,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata = null)
        => new(
            fileStorage,
            repository,
            changeFeed,
            logger,
            SanitizeFileName(fileName),
            contentType,
            metadata ?? new Dictionary<string, string>());

    public async Task<DocumentContent?> DownloadAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var document = await repository.GetAsync(id, cancellationToken);
        if (document is null)
        {
            return null;
        }

        if (document.Segmented)
        {
            // Joined on the way out, so nothing downstream has to know it was written in pieces.
            return new DocumentContent(
                new PartedStream(fileStorage, document.Parts, cancellationToken),
                document.FileName,
                document.ContentType);
        }

        var stream = await fileStorage.OpenReadAsync(document.StorageKey, cancellationToken);
        if (stream is null)
        {
            logger.LogWarning(
                "Metadata references a missing object {DocumentId} {StorageKey}",
                document.Id,
                document.StorageKey);

            return null;
        }

        return new DocumentContent(stream, document.FileName, document.ContentType);
    }

    public async Task<DocumentContent?> DownloadThumbnailAsync(
        Guid id,
        CancellationToken cancellationToken = default)
    {
        var document = await repository.GetAsync(id, cancellationToken);
        if (document?.ThumbnailKey is null)
        {
            return null;
        }

        var stream = await fileStorage.OpenReadAsync(document.ThumbnailKey, cancellationToken);

        return stream is null
            ? null
            : new DocumentContent(stream, $"{document.FileName}.jpg", "image/jpeg");
    }

    public async Task DeleteAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var document = await repository.GetAsync(id, cancellationToken);
        if (document is null)
        {
            return;
        }

        // A segmented document has no object at its own key; its bytes are the pieces.
        await fileStorage.DeleteAsync(document.StorageKey, cancellationToken);

        foreach (var part in document.Parts)
        {
            await fileStorage.DeleteAsync(part.Key, cancellationToken);
        }

        if (document.ThumbnailKey is not null)
        {
            // Idempotent, so a thumbnail that was never written is not an error.
            await fileStorage.DeleteAsync(document.ThumbnailKey, cancellationToken);
        }

        await repository.DeleteAsync(id, cancellationToken);

        logger.LogInformation("Document deleted {DocumentId} {StorageKey}", id, document.StorageKey);

        changeFeed.Publish(new DocumentChange(
            ChangeKind.Removed,
            document.Id,
            document.StorageKey,
            document.FileName));
    }

    private async Task TryDeleteAsync(string key, Guid id)
    {
        try
        {
            await fileStorage.DeleteAsync(key, CancellationToken.None);
        }
        catch (Exception cleanupFailure)
        {
            logger.LogError(
                cleanupFailure,
                "Orphaned object left behind after metadata write failed {DocumentId} {StorageKey}",
                id,
                key);
        }
    }

    /// <summary>Strips any client-supplied path.</summary>
    private static string SanitizeFileName(string fileName)
    {
        var name = Path.GetFileName(fileName.Replace('\\', '/'));
        foreach (var invalid in Path.GetInvalidFileNameChars())
        {
            name = name.Replace(invalid, '_');
        }

        return string.IsNullOrWhiteSpace(name) ? "file" : name;
    }
}

/// <summary>What analysis produced for an object that is already in the store.</summary>
public sealed record StoredAnalysis(IReadOnlyDictionary<string, string> Metadata, string? ThumbnailKey)
{
    public static readonly StoredAnalysis None = new(new Dictionary<string, string>(), null);
}
