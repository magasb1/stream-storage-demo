using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Tests.Application;

public sealed class DocumentServiceTests
{
    private readonly FakeFileStorage _storage = new();
    private readonly FakeDocumentRepository _repository = new();

    private readonly FakeMediaAnalyzer _analyzer = new();

    private readonly InMemoryAnalysisQueue _queue = new();

    private DocumentService CreateService()
        => new(
            _storage,
            _repository,
            _analyzer,
            _queue,
            new InMemoryChangeFeed(),
            NullLogger<DocumentService>.Instance);

    private static MemoryStream Content(string text = "hello")
        => new(System.Text.Encoding.UTF8.GetBytes(text));

    [Fact]
    public async Task Upload_stores_file_then_metadata()
    {
        var document = await CreateService().UploadAsync("report.pdf", Content(), "application/pdf");

        Assert.Equal("report.pdf", document.FileName);
        Assert.Equal($"documents/{document.Id}/report.pdf", document.StorageKey);
        Assert.Equal(5, document.Size);
        Assert.True(_storage.Objects.ContainsKey(document.StorageKey));
        Assert.True(_repository.Documents.ContainsKey(document.Id));
    }

    /// <summary>
    /// A snapshot knows which live stream it came from and when it was captured, and a recording
    /// knows whether it was truncated.
    /// </summary>
    [Fact]
    public async Task Upload_keeps_what_the_uploader_knew_that_a_probe_could_not()
    {
        var document = await CreateService().UploadAsync(
            "camera1-20260101-120000.jpg",
            Content(),
            "image/jpeg",
            CancellationToken.None,
            new Dictionary<string, string>
            {
                ["Live stream"] = "live/camera1",
                ["Captured"] = "2026-01-01 12:00:00Z",
            });

        Assert.Equal("live/camera1", document.Metadata["Live stream"]);
        Assert.Equal("2026-01-01 12:00:00Z", document.Metadata["Captured"]);
    }

    /// <summary>The other half of provenance: from a detection, every document it produced.</summary>
    [Fact]
    public async Task Documents_are_found_by_the_detection_that_caused_them()
    {
        var frame = new DateTimeOffset(2026, 9, 12, 10, 31, 2, TimeSpan.Zero).AddTicks(1234560);
        var wanted = new DetectionReference("live/camera1", frame, 7);
        var neighbour = new DetectionReference("live/camera1", frame, 8);

        var service = CreateService();
        var snapshot = await Capture(service, "camera1.jpg", wanted);
        var recording = await Capture(service, "camera1.ts", wanted);
        await Capture(service, "other.jpg", neighbour);
        await service.UploadAsync("holiday.jpg", Content(), "image/jpeg");

        var found = await service.FindByDetectionAsync(wanted);

        Assert.Equal(
            new[] { snapshot.Id, recording.Id }.OrderBy(id => id),
            found.Select(document => document.Id).OrderBy(id => id));
        Assert.Empty(await service.FindByDetectionAsync(new DetectionReference("live/camera1", frame, 9)));
    }

    /// <summary>A detection on another stream is another detection, even at the same instant.</summary>
    [Fact]
    public async Task A_detection_on_another_stream_finds_nothing()
    {
        var frame = DateTimeOffset.UtcNow;
        var service = CreateService();

        await Capture(service, "camera1.jpg", new DetectionReference("live/camera1", frame, 7));

        Assert.Empty(await service.FindByDetectionAsync(new DetectionReference("live/camera2", frame, 7)));
    }

    private static Task<Document> Capture(
        DocumentService service,
        string fileName,
        DetectionReference detection)
        => service.UploadAsync(
            fileName,
            Content(),
            null,
            CancellationToken.None,
            new Dictionary<string, string>
            {
                ["Live stream"] = detection.Stream,
                [DetectionReference.MetadataKey] = detection.ToString(),
            });

    [Fact]
    public async Task Upload_without_metadata_carries_none()
        => Assert.Empty((await CreateService().UploadAsync("report.pdf", Content(), null)).Metadata);

    [Fact]
    public async Task Upload_strips_client_supplied_paths_from_the_filename()
    {
        var document = await CreateService().UploadAsync(@"..\..\etc\passwd", Content(), null);

        Assert.Equal("passwd", document.FileName);
        Assert.DoesNotContain("..", document.StorageKey, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Upload_deletes_the_file_when_the_metadata_write_fails()
    {
        _repository.FailOnAdd = true;

        await Assert.ThrowsAsync<PersistenceException>(
            () => CreateService().UploadAsync("report.pdf", Content(), null));

        Assert.Empty(_storage.Objects);
    }

    [Fact]
    public async Task Upload_rethrows_the_original_failure_even_when_cleanup_fails()
    {
        _repository.FailOnAdd = true;
        _storage.FailOnDelete = true;

        await Assert.ThrowsAsync<PersistenceException>(
            () => CreateService().UploadAsync("report.pdf", Content(), null));
    }

    [Fact]
    public async Task Download_returns_null_for_an_unknown_document()
        => Assert.Null(await CreateService().DownloadAsync(Guid.NewGuid()));

    [Fact]
    public async Task Download_returns_null_when_metadata_points_at_a_missing_object()
    {
        var service = CreateService();
        var document = await service.UploadAsync("report.pdf", Content(), null);
        _storage.Objects.Clear();

        Assert.Null(await service.DownloadAsync(document.Id));
    }

    [Fact]
    public async Task Download_returns_the_stored_bytes()
    {
        var service = CreateService();
        var document = await service.UploadAsync("report.pdf", Content("payload"), "application/pdf");

        var content = await service.DownloadAsync(document.Id);

        Assert.NotNull(content);
        using var reader = new StreamReader(content.Stream);
        Assert.Equal("payload", await reader.ReadToEndAsync());
        Assert.Equal("report.pdf", content.FileName);
    }

    [Fact]
    public async Task Delete_removes_both_the_file_and_the_metadata()
    {
        var service = CreateService();
        var document = await service.UploadAsync("report.pdf", Content(), null);

        await service.DeleteAsync(document.Id);

        Assert.Empty(_storage.Objects);
        Assert.Empty(_repository.Documents);
    }

    [Fact]
    public async Task Delete_is_idempotent_for_an_unknown_document()
        => await CreateService().DeleteAsync(Guid.NewGuid());

    [Fact]
    public async Task Upload_returns_before_the_thumbnail_exists_and_queues_the_work()
    {
        var document = await CreateService().UploadAsync("holiday.jpg", Content(), "image/jpeg");

        // The caller is answered as soon as the bytes and the row are safe.
        Assert.Null(document.ThumbnailKey);
        Assert.Empty(document.Metadata);
        Assert.Empty(_analyzer.Analyzed);

        var queued = Assert.Single(await DrainQueueAsync());
        Assert.Equal(document.Id, queued.Id);
        Assert.Equal(document.StorageKey, queued.StorageKey);
    }

    [Fact]
    public async Task Uploading_something_that_is_not_media_queues_nothing()
    {
        await CreateService().UploadAsync("notes.txt", Content(), "text/plain");

        Assert.Empty(await DrainQueueAsync());
    }

    [Fact]
    public async Task Analysis_stores_the_thumbnail_alongside_the_document()
    {
        var service = CreateService();
        var document = await service.UploadAsync("holiday.jpg", Content(), "image/jpeg");

        var analysis = await service.AnalyzeStoredObjectAsync(
            document.Id,
            document.StorageKey,
            document.FileName,
            document.ContentType);

        Assert.Equal($"thumbnails/{document.Id}.jpg", analysis.ThumbnailKey);
        Assert.Equal(FakeMediaAnalyzer.ThumbnailBytes, _storage.Objects[analysis.ThumbnailKey!]);
    }

    [Fact]
    public async Task Deleting_a_document_removes_its_thumbnail_too()
    {
        var service = CreateService();
        var document = await service.UploadAsync("holiday.jpg", Content(), "image/jpeg");

        var analysis = await service.AnalyzeStoredObjectAsync(
            document.Id,
            document.StorageKey,
            document.FileName,
            document.ContentType);

        // The row only learns about its thumbnail once analysis has finished.
        await _repository.UpsertAsync(new Document
        {
            Id = document.Id,
            FileName = document.FileName,
            StorageKey = document.StorageKey,
            ContentType = document.ContentType,
            Size = document.Size,
            CreatedAt = document.CreatedAt,
            ThumbnailKey = analysis.ThumbnailKey,
            Metadata = analysis.Metadata,
        });

        await service.DeleteAsync(document.Id);

        Assert.Empty(_storage.Objects);
        Assert.Empty(_repository.Documents);
    }

    [Fact]
    public async Task A_thumbnail_is_still_downloadable_when_the_original_object_is_gone()
    {
        var service = CreateService();
        var document = await service.UploadAsync("holiday.jpg", Content(), "image/jpeg");
        var analysis = await service.AnalyzeStoredObjectAsync(
            document.Id,
            document.StorageKey,
            document.FileName,
            document.ContentType);

        await _repository.UpsertAsync(new Document
        {
            Id = document.Id,
            FileName = document.FileName,
            StorageKey = document.StorageKey,
            ContentType = document.ContentType,
            Size = document.Size,
            CreatedAt = document.CreatedAt,
            ThumbnailKey = analysis.ThumbnailKey,
        });

        var thumbnail = await service.DownloadThumbnailAsync(document.Id);

        Assert.NotNull(thumbnail);
        Assert.Equal("image/jpeg", thumbnail.ContentType);
    }

    [Fact]
    public async Task There_is_no_thumbnail_to_download_for_a_document_without_one()
    {
        var service = CreateService();
        var document = await service.UploadAsync("notes.txt", Content(), "text/plain");

        Assert.Null(await service.DownloadThumbnailAsync(document.Id));
    }

    private async Task<List<AnalysisRequest>> DrainQueueAsync()
    {
        // The queue never completes, so read whatever is waiting and stop.
        using var window = new CancellationTokenSource(TimeSpan.FromMilliseconds(150));
        var requests = new List<AnalysisRequest>();

        try
        {
            await foreach (var request in _queue.DequeueAllAsync(window.Token))
            {
                requests.Add(request);
            }
        }
        catch (OperationCanceledException)
        {
        }

        return requests;
    }
}
