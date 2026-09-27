namespace StorageDemo.Core.Documents;

public sealed record DocumentContent(Stream Stream, string FileName, string? ContentType);

public interface IDocumentService
{
    /// <param name="metadata">
    /// What the caller knows and the media probe cannot work out: which live stream a recording
    /// came from, the presentation timestamp a snapshot was taken at, whether a recording was
    /// truncated.
    /// </param>
    Task<Document> UploadAsync(
        string fileName,
        Stream content,
        string? contentType,
        CancellationToken cancellationToken = default,
        IReadOnlyDictionary<string, string>? metadata = null);

    /// <summary>
    /// Starts a document that will be written a piece at a time, for something too long to hold
    /// whole before storing it.
    /// </summary>
    SegmentedDocument BeginSegmented(
        string fileName,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata = null);

    Task<DocumentContent?> DownloadAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>The generated preview image, or null when the document has none.</summary>
    Task<DocumentContent?> DownloadThumbnailAsync(Guid id, CancellationToken cancellationToken = default);

    Task<Document?> GetAsync(Guid id, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<Document>> GetAllAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Every recording and snapshot one detection caused, newest first as the listing gives them.
    /// </summary>
    Task<IReadOnlyList<Document>> FindByDetectionAsync(
        Streaming.DetectionReference detection,
        CancellationToken cancellationToken = default);

    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>Probes an object that is already in the store and writes its thumbnail.</summary>
    Task<StoredAnalysis> AnalyzeStoredObjectAsync(
        Guid id,
        string storageKey,
        string fileName,
        string? contentType,
        CancellationToken cancellationToken = default);
}
