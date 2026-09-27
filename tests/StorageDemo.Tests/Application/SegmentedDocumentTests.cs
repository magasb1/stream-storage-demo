using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Core.Documents;

namespace StorageDemo.Tests.Application;

/// <summary>
/// A recording that runs for hours is written a piece at a time so that no pod holds it whole, and
/// read back as one file so that nobody opening it can tell.
/// </summary>
public sealed class SegmentedDocumentTests
{
    private readonly FakeFileStorage _storage = new();
    private readonly FakeDocumentRepository _repository = new();
    private readonly InMemoryChangeFeed _changeFeed = new();

    private DocumentService CreateService() => new(
        _storage,
        _repository,
        new FakeMediaAnalyzer(),
        new InMemoryAnalysisQueue(),
        _changeFeed,
        NullLogger<DocumentService>.Instance);

    private static byte[] Bytes(int count, byte seed)
        => [.. Enumerable.Range(0, count).Select(i => (byte)(seed + i))];

    /// <summary>Writes the given pieces and returns the finished document.</summary>
    private async Task<Document> RecordAsync(params byte[][] pieces)
    {
        var document = CreateService().BeginSegmented("camera1-20260101.ts", "video/mp2t");

        foreach (var piece in pieces)
        {
            using var content = new MemoryStream(piece);
            await document.AppendAsync(content, piece.Length);
        }

        return (await document.CompleteAsync())!;
    }

    [Fact]
    public async Task Each_segment_is_stored_as_it_completes_rather_than_at_the_end()
    {
        var document = CreateService().BeginSegmented("camera1.ts", "video/mp2t");

        using (var first = new MemoryStream(Bytes(100, 0)))
        {
            await document.AppendAsync(first, 100);
        }

        // Stored and readable already: nothing waits for the recording to finish, which is the
        // whole reason for writing it in pieces.
        Assert.Single(_storage.Objects);
        Assert.NotNull(await _repository.GetAsync(document.Id));

        using (var second = new MemoryStream(Bytes(50, 100)))
        {
            await document.AppendAsync(second, 50);
        }

        Assert.Equal(2, _storage.Objects.Count);
    }

    /// <summary>One document, one name, one size.</summary>
    [Fact]
    public async Task The_pieces_read_back_as_one_file()
    {
        var pieces = new[] { Bytes(100, 0), Bytes(100, 100), Bytes(55, 200) };
        var document = await RecordAsync(pieces);

        Assert.Equal(255, document.Size);
        Assert.Equal(3, document.Parts.Count);
        Assert.True(document.Segmented);

        var content = await CreateService().DownloadAsync(document.Id);

        Assert.NotNull(content);

        using var joined = new MemoryStream();
        await content.Stream.CopyToAsync(joined);

        Assert.Equal(pieces.SelectMany(p => p).ToArray(), joined.ToArray());
    }

    /// <summary>
    /// Seeking is what makes a six hour recording usable from the document list, and it has to land
    /// on the right byte wherever it is asked to: at a boundary, inside a piece, and at the very
    /// end.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(99)]
    [InlineData(100)]
    [InlineData(101)]
    [InlineData(199)]
    [InlineData(200)]
    [InlineData(254)]
    public async Task Seeking_lands_on_the_right_byte_wherever_it_is_asked_to(int position)
    {
        var pieces = new[] { Bytes(100, 0), Bytes(100, 100), Bytes(55, 200) };
        var whole = pieces.SelectMany(p => p).ToArray();
        var document = await RecordAsync(pieces);

        var content = await CreateService().DownloadAsync(document.Id);
        Assert.NotNull(content);

        var stream = content.Stream;

        Assert.True(stream.CanSeek, "a document written in pieces has to be seekable to be usable");
        Assert.Equal(whole.Length, stream.Length);

        stream.Seek(position, SeekOrigin.Begin);

        var rest = new byte[whole.Length - position];
        await stream.ReadExactlyAsync(rest);

        Assert.Equal(whole[position..], rest);
    }

    [Fact]
    public async Task Seeking_backwards_and_from_the_end_works_too()
    {
        var pieces = new[] { Bytes(100, 0), Bytes(100, 100) };
        var whole = pieces.SelectMany(p => p).ToArray();

        var content = await CreateService().DownloadAsync((await RecordAsync(pieces)).Id);
        Assert.NotNull(content);

        var stream = content.Stream;

        stream.Seek(-10, SeekOrigin.End);
        var tail = new byte[10];
        await stream.ReadExactlyAsync(tail);
        Assert.Equal(whole[^10..], tail);

        // Back to the start of the first piece, having already been in the second.
        stream.Seek(0, SeekOrigin.Begin);
        var head = new byte[10];
        await stream.ReadExactlyAsync(head);
        Assert.Equal(whole[..10], head);
    }

    [Fact]
    public async Task Reading_at_the_very_end_returns_nothing_rather_than_failing()
    {
        var content = await CreateService().DownloadAsync(
            (await RecordAsync(Bytes(10, 0), Bytes(10, 10))).Id);

        Assert.NotNull(content);

        content.Stream.Seek(20, SeekOrigin.Begin);

        Assert.Equal(0, await content.Stream.ReadAsync(new byte[8]));
    }

    /// <summary>Deleting has to take the pieces with it.</summary>
    [Fact]
    public async Task Deleting_removes_every_piece()
    {
        var document = await RecordAsync(Bytes(10, 0), Bytes(10, 10), Bytes(10, 20));

        Assert.Equal(3, _storage.Objects.Count);

        await CreateService().DeleteAsync(document.Id);

        Assert.Empty(_storage.Objects);
        Assert.Null(await _repository.GetAsync(document.Id));
    }

    /// <summary>A recording that captured nothing leaves nothing behind.</summary>
    [Fact]
    public async Task A_recording_that_stored_no_pieces_is_not_a_document()
    {
        var document = CreateService().BeginSegmented("empty.ts", "video/mp2t");

        Assert.Null(document.Current());
        Assert.Null(await document.CompleteAsync());
        Assert.Empty(_storage.Objects);
    }

    /// <summary>
    /// The pieces live outside the prefix the storage monitor scans, exactly as thumbnails do.
    /// </summary>
    [Fact]
    public async Task The_pieces_are_kept_out_of_the_way_of_the_storage_monitor()
    {
        await RecordAsync(Bytes(10, 0), Bytes(10, 10));

        Assert.All(_storage.Objects.Keys, key => Assert.StartsWith("recordings/", key, StringComparison.Ordinal));
        Assert.DoesNotContain(_storage.Objects.Keys, key => key.StartsWith("documents/", StringComparison.Ordinal));
    }

    /// <summary>The reconciler removes rows whose object has gone.</summary>
    [Fact]
    public async Task The_reconciler_leaves_a_recording_alone()
    {
        var document = await RecordAsync(Bytes(10, 0), Bytes(10, 10));

        var reconciler = new StorageReconciler(
            _storage,
            _repository,
            new InMemoryAnalysisQueue(),
            _changeFeed,
            NullLogger<StorageReconciler>.Instance);

        var result = await reconciler.ReconcileAsync("documents/");

        Assert.Equal(0, result.Removed);
        Assert.NotNull(await _repository.GetAsync(document.Id));
    }
}
