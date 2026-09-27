using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Storage;

namespace StorageDemo.Core.Documents;

public sealed record ReconciliationResult(int Added, int Updated, int Removed)
{
    public bool AnyChanges => Added + Updated + Removed > 0;
}

/// <summary>
/// Brings the metadata database back in line with what is actually in the store, so files added,
/// replaced or deleted by something other than this application still show up.
/// </summary>
public sealed class StorageReconciler(
    IFileStorage fileStorage,
    IDocumentRepository repository,
    IAnalysisQueue analysisQueue,
    IChangeFeed changeFeed,
    ILogger<StorageReconciler> logger)
{
    public async Task<ReconciliationResult> ReconcileAsync(
        string prefix,
        CancellationToken cancellationToken = default)
    {
        // Documents written in pieces are left alone.
        var known = (await repository.GetAllAsync(cancellationToken))
            .Where(d => !d.Segmented)
            .ToDictionary(d => d.StorageKey, StringComparer.Ordinal);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var added = 0;
        var updated = 0;

        // The listing is drained before anything is written, because importing an object writes a
        // thumbnail back to the same store and mutating a store mid-scan is asking for trouble.
        var objects = new List<StorageObject>();
        await foreach (var obj in fileStorage.ListAsync(prefix, cancellationToken))
        {
            objects.Add(obj);
        }

        foreach (var obj in objects)
        {
            seen.Add(obj.Key);

            if (!known.TryGetValue(obj.Key, out var existing))
            {
                var imported = Import(obj);
                await repository.UpsertAsync(imported, cancellationToken);
                await analysisQueue.EnqueueAsync(
                    new AnalysisRequest(
                        imported.Id,
                        imported.StorageKey,
                        imported.FileName,
                        imported.ContentType),
                    cancellationToken);
                added++;
                logger.LogInformation("Imported external object {StorageKey} {Size}", obj.Key, obj.Size);
                changeFeed.Publish(new DocumentChange(
                    ChangeKind.Added,
                    imported.Id,
                    imported.StorageKey,
                    imported.FileName));
                continue;
            }

            // The store reports the truth about bytes; a size change means the object was replaced.
            if (existing.Size != obj.Size)
            {
                await repository.UpsertAsync(
                    new Document
                    {
                        Id = existing.Id,
                        FileName = existing.FileName,
                        StorageKey = existing.StorageKey,
                        ContentType = existing.ContentType,
                        Size = obj.Size,
                        CreatedAt = existing.CreatedAt,
                        ThumbnailKey = existing.ThumbnailKey,
                        Metadata = existing.Metadata,
                    },
                    cancellationToken);

                // The old preview is of the old bytes, so it is regenerated rather than kept.
                await analysisQueue.EnqueueAsync(
                    new AnalysisRequest(
                        existing.Id,
                        existing.StorageKey,
                        existing.FileName,
                        existing.ContentType),
                    cancellationToken);

                updated++;
                logger.LogInformation(
                    "Updated externally modified object {StorageKey} {Size}",
                    obj.Key,
                    obj.Size);
                changeFeed.Publish(new DocumentChange(
                    ChangeKind.Updated,
                    existing.Id,
                    existing.StorageKey,
                    existing.FileName));
            }
        }

        var removed = 0;
        foreach (var orphan in known.Values.Where(d => !seen.Contains(d.StorageKey)))
        {
            await repository.DeleteAsync(orphan.Id, cancellationToken);
            removed++;
            logger.LogInformation(
                "Removed metadata for a deleted object {DocumentId} {StorageKey}",
                orphan.Id,
                orphan.StorageKey);
            changeFeed.Publish(new DocumentChange(
                ChangeKind.Removed,
                orphan.Id,
                orphan.StorageKey,
                orphan.FileName));
        }

        return new ReconciliationResult(added, updated, removed);
    }

    /// <summary>
    /// A file that arrived outside the application becomes a document straight away; its thumbnail
    /// and metadata follow from the queue, exactly as they do for an upload.
    /// </summary>
    private static Document Import(StorageObject obj)
    {
        var fileName = obj.Key.Split('/').Last();
        if (string.IsNullOrWhiteSpace(fileName))
        {
            fileName = obj.Key;
        }

        return new Document
        {
            Id = IdFor(obj.Key),
            FileName = fileName,
            StorageKey = obj.Key,
            ContentType = ContentTypes.Guess(fileName),
            Size = obj.Size,
            CreatedAt = obj.LastModified,
        };
    }

    /// <summary>Keys this application wrote already carry their id.</summary>
    private static Guid IdFor(string key)
    {
        var segments = key.Split('/');
        if (segments.Length >= 2 && Guid.TryParse(segments[^2], out var embedded))
        {
            return embedded;
        }

        return new Guid(MD5.HashData(Encoding.UTF8.GetBytes(key)));
    }
}
