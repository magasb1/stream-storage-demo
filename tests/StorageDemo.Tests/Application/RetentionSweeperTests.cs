using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Tests.Application;

/// <summary>
/// Retention deletes data nobody asked it to delete if it is wrong, so every one of these is about
/// what survives rather than what goes.
/// </summary>
public sealed class RetentionSweeperTests
{
    private static readonly TimeSpan Week = TimeSpan.FromDays(7);

    private static readonly TimeSpan TenMinutes = TimeSpan.FromMinutes(10);

    private readonly FakeFileStorage _storage = new();
    private readonly FakeDocumentRepository _repository = new();
    private readonly InMemoryLiveStreamRegistry _registry = new();

    private RetentionSweeper CreateSweeper()
        => new(
            new DocumentService(
                _storage,
                _repository,
                new FakeMediaAnalyzer(),
                new InMemoryAnalysisQueue(),
                new InMemoryChangeFeed(),
                NullLogger<DocumentService>.Instance),
            _registry,
            NullLogger<RetentionSweeper>.Instance);

    [Fact]
    public async Task An_old_recording_loses_its_bytes_and_its_row()
    {
        var old = Recording("cam-1", Ago(30), parts: 3);

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(1, result.Documents);
        Assert.Empty(_repository.Documents);
        Assert.Empty(_storage.Objects);
        Assert.All(old.Parts, part => Assert.DoesNotContain(part.Key, _storage.Objects.Keys));
    }

    [Fact]
    public async Task A_recent_recording_is_left_alone()
    {
        Recording("cam-1", Ago(2), parts: 2);

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(0, result.Documents);
        Assert.Single(_repository.Documents);
        Assert.Equal(2, _storage.Objects.Count);
    }

    [Fact]
    public async Task Every_part_of_a_segmented_recording_goes_and_not_only_its_row()
    {
        // The trap: a recording's parts live under recordings/ rather than the prefix the
        // reconciler scans, so parts left behind by a row-only delete are orphaned forever.
        Recording("cam-1", Ago(30), parts: 40);
        var keepers = Recording("cam-2", Ago(1), parts: 5).Parts;

        await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(5, _storage.Objects.Count);
        Assert.All(keepers, part => Assert.Contains(part.Key, _storage.Objects.Keys));
    }

    [Fact]
    public async Task An_old_snapshot_is_expired_too()
    {
        Snapshot("cam-1", Ago(30));

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(1, result.Documents);
        Assert.Empty(_storage.Objects);
    }

    [Fact]
    public async Task A_recording_still_being_written_survives_however_old_it_is()
    {
        // Long enough that age alone would take it, which is exactly the six-hour recording the
        // recorder is still appending parts to.
        var startedAt = Ago(30);
        Recording("cam-1", startedAt, parts: 60);
        await _registry.UpsertAsync(Live("cam-1", Recorded(startedAt)));

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(0, result.Documents);
        Assert.Single(_repository.Documents);
        Assert.Equal(60, _storage.Objects.Count);
    }

    [Fact]
    public async Task An_earlier_recording_of_a_stream_that_is_recording_now_still_expires()
    {
        // The open check is per recording, not per stream.
        var earlier = Recording("cam-1", Ago(30));
        var running = Recording("cam-1", Ago(29));
        await _registry.UpsertAsync(Live("cam-1", Recorded(running.CreatedAt)));

        await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.DoesNotContain(earlier.Id, _repository.Documents.Keys);
        Assert.Contains(running.Id, _repository.Documents.Keys);
    }

    /// <summary>
    /// The sweeper tells a capture from an upload by one metadata key, so a capture carrying a new
    /// one must still be swept and an upload must still be safe.
    /// </summary>
    [Fact]
    public async Task A_capture_with_a_detection_reference_is_still_told_from_an_upload()
    {
        var detection = new DetectionReference("cam-1", Ago(30), 7);

        var recording = Recording("cam-1", Ago(30), parts: 2, detection: detection);
        var snapshot = Snapshot("cam-1", Ago(30), detection);

        _storage.Objects["documents/holiday.jpg"] = [1];
        var upload = Store(new Document
        {
            Id = Guid.NewGuid(),
            FileName = "holiday.jpg",
            StorageKey = "documents/holiday.jpg",
            CreatedAt = Ago(3000),
        });

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(2, result.Documents);
        Assert.DoesNotContain(recording.Id, _repository.Documents.Keys);
        Assert.DoesNotContain(snapshot.Id, _repository.Documents.Keys);
        Assert.Contains(upload.Id, _repository.Documents.Keys);
    }

    [Fact]
    public async Task An_uploaded_document_is_never_expired()
    {
        // Nothing here has been given permission to delete a file somebody uploaded, at any age.
        _storage.Objects["documents/holiday.jpg"] = [1];
        Store(new Document
        {
            Id = Guid.NewGuid(),
            FileName = "holiday.jpg",
            StorageKey = "documents/holiday.jpg",
            CreatedAt = Ago(3000),
        });

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(0, result.Documents);
        Assert.Single(_storage.Objects);
    }

    [Fact]
    public async Task Nothing_is_expired_when_no_age_is_configured()
    {
        Recording("cam-1", Ago(3000));

        var result = await CreateSweeper().SweepAsync(TimeSpan.Zero, TenMinutes);

        Assert.Equal(0, result.Documents);
        Assert.Single(_repository.Documents);
    }

    [Fact]
    public async Task A_registry_entry_whose_owner_stopped_heartbeating_is_removed()
    {
        await _registry.UpsertAsync(Live("ghost", heartbeat: DateTimeOffset.UtcNow - TimeSpan.FromHours(1)));

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(1, result.Entries);
        Assert.Empty(await _registry.ListAsync());
    }

    [Fact]
    public async Task A_live_streams_entry_is_not_removed()
    {
        // The regression that would matter most: sweeping a living stream's entry unlocks its name
        // to a second publisher and drops it out of every replica's view of the cluster.
        await _registry.UpsertAsync(Live("cam-1"));
        await _registry.UpsertAsync(Live("cam-2", heartbeat: DateTimeOffset.UtcNow - TimeSpan.FromSeconds(90)));

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(0, result.Entries);
        Assert.Equal(2, (await _registry.ListAsync()).Count);
    }

    [Fact]
    public async Task An_interrupted_stream_inside_its_grace_period_keeps_its_entry()
    {
        // Interrupted is a state the owner keeps reporting, so its heartbeat is current and the
        // sweeper must not confuse "the feed stopped" with "the pod died".
        await _registry.UpsertAsync(Live("cam-1") with { State = LiveStreamState.Interrupted });

        var result = await CreateSweeper().SweepAsync(Week, TenMinutes);

        Assert.Equal(0, result.Entries);
        Assert.Single(await _registry.ListAsync());
    }

    private static DateTimeOffset Ago(int days) => DateTimeOffset.UtcNow - TimeSpan.FromDays(days);

    /// <summary>What the coordinator publishes for a stream that is recording right now.</summary>
    private static RecordingStatus Recorded(DateTimeOffset startedAt)
        => new(Guid.NewGuid(), startedAt, startedAt.AddHours(6), 1024);

    private static LiveStream Live(
        string name,
        RecordingStatus? recording = null,
        DateTimeOffset? heartbeat = null)
        => new(
            name,
            LiveStreamState.Live,
            DateTimeOffset.UtcNow,
            heartbeat ?? DateTimeOffset.UtcNow,
            "pod-a",
            "http://10.0.0.1:8080",
            0,
            0,
            false,
            true,
            false,
            0,
            null,
            recording,
            null);

    /// <summary>
    /// A segmented recording as the recorder leaves one: parts under recordings/, no object at its
    /// own key, and the stream name and start time in its metadata.
    /// </summary>
    private Document Recording(
        string stream,
        DateTimeOffset startedAt,
        int parts = 1,
        DetectionReference? detection = null)
    {
        var id = Guid.NewGuid();
        var pieces = Enumerable
            .Range(0, parts)
            .Select(part => new DocumentPart($"{SegmentedDocument.Prefix}{id}/{part:D5}.ts", 3))
            .ToList();

        foreach (var piece in pieces)
        {
            _storage.Objects[piece.Key] = [1, 2, 3];
        }

        return Store(new Document
        {
            Id = id,
            FileName = $"{stream}.ts",
            StorageKey = $"{SegmentedDocument.Prefix}{id}/{stream}.ts",
            ContentType = "video/mp2t",
            Size = pieces.Sum(piece => piece.Size),
            CreatedAt = startedAt,
            Parts = pieces,
            Metadata = With(detection, new Dictionary<string, string>
            {
                ["Live stream"] = stream,
                ["Recording started"] = startedAt.ToString("u"),
            }),
        });
    }

    /// <summary>
    /// A snapshot names its stream but has no start time, because nothing is still writing it.
    /// </summary>
    private Document Snapshot(string stream, DateTimeOffset takenAt, DetectionReference? detection = null)
    {
        var id = Guid.NewGuid();
        var key = $"documents/{id}/{stream}.jpg";
        _storage.Objects[key] = [1];

        return Store(new Document
        {
            Id = id,
            FileName = $"{stream}.jpg",
            StorageKey = key,
            ContentType = "image/jpeg",
            Size = 1,
            CreatedAt = takenAt,
            Metadata = With(detection, new Dictionary<string, string>
            {
                ["Live stream"] = stream,
                ["Captured"] = takenAt.ToString("u"),
            }),
        });
    }

    private static Dictionary<string, string> With(
        DetectionReference? detection,
        Dictionary<string, string> metadata)
    {
        if (detection is not null)
        {
            metadata[DetectionReference.MetadataKey] = detection.ToString();
        }

        return metadata;
    }

    private Document Store(Document document)
    {
        _repository.Documents[document.Id] = document;

        return document;
    }
}
