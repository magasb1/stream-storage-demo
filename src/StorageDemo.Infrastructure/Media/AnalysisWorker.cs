using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Storage;

namespace StorageDemo.Infrastructure.Media;

/// <summary>
/// Produces thumbnails and metadata after the upload has already been answered, then publishes an
/// Updated change so connected clients fill the tile in without asking.
/// </summary>
public sealed class AnalysisWorker(
    IAnalysisQueue queue,
    IServiceScopeFactory scopeFactory,
    IChangeFeed changeFeed,
    ILogger<AnalysisWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var request in queue.DequeueAllAsync(stoppingToken))
        {
            try
            {
                await ProcessAsync(request, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // One bad file must not stop every later upload from getting its preview.
                logger.LogError(ex, "Analysis failed for {StorageKey}", request.StorageKey);
            }
        }
    }

    private async Task ProcessAsync(AnalysisRequest request, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();

        var documents = scope.ServiceProvider.GetRequiredService<IDocumentService>();
        var repository = scope.ServiceProvider.GetRequiredService<IDocumentRepository>();

        var analysis = await documents.AnalyzeStoredObjectAsync(
            request.Id,
            request.StorageKey,
            request.FileName,
            request.ContentType,
            cancellationToken);

        if (analysis.ThumbnailKey is null && analysis.Metadata.Count == 0)
        {
            return;
        }

        // The document can be deleted while its analysis is in flight.
        var current = await repository.GetAsync(request.Id, cancellationToken);
        if (current is null)
        {
            if (analysis.ThumbnailKey is not null)
            {
                var storage = scope.ServiceProvider.GetRequiredService<IFileStorage>();
                await storage.DeleteAsync(analysis.ThumbnailKey, CancellationToken.None);
                logger.LogInformation(
                    "Discarded a thumbnail for {DocumentId}, deleted while it was being analyzed",
                    request.Id);
            }

            return;
        }

        await repository.UpsertAsync(
            new Document
            {
                Id = current.Id,
                FileName = current.FileName,
                StorageKey = current.StorageKey,
                ContentType = current.ContentType,
                Size = current.Size,
                CreatedAt = current.CreatedAt,
                ThumbnailKey = analysis.ThumbnailKey,

                Metadata = Merge(current.Metadata, analysis.Metadata),
            },
            cancellationToken);

        logger.LogInformation(
            "Analyzed {DocumentId} {StorageKey} {HasThumbnail}",
            current.Id,
            current.StorageKey,
            analysis.ThumbnailKey is not null);

        changeFeed.Publish(new DocumentChange(
            ChangeKind.Updated,
            current.Id,
            current.StorageKey,
            current.FileName));
    }

    /// <summary>The uploader's own entries win, because it knows things the probe cannot read.</summary>
    private static Dictionary<string, string> Merge(
        IReadOnlyDictionary<string, string> supplied,
        IReadOnlyDictionary<string, string> probed)
    {
        var merged = new Dictionary<string, string>(probed);

        foreach (var (key, value) in supplied)
        {
            merged[key] = value;
        }

        return merged;
    }
}
