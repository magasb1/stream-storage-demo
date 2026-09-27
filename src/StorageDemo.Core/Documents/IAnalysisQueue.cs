namespace StorageDemo.Core.Documents;

/// <param name="Id">The document whose row gets the thumbnail and metadata.</param>
/// <param name="StorageKey">Where the bytes already are.</param>
public sealed record AnalysisRequest(Guid Id, string StorageKey, string FileName, string? ContentType);

/// <summary>Work handed off so an upload can return as soon as the bytes and the row are safe.</summary>
public interface IAnalysisQueue
{
    Task EnqueueAsync(AnalysisRequest request, CancellationToken cancellationToken = default);

    /// <summary>Runs until cancelled.</summary>
    IAsyncEnumerable<AnalysisRequest> DequeueAllAsync(CancellationToken cancellationToken);
}
