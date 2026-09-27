using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The store exists because a source is a setting rather than a reading, so the property under test
/// throughout is that a save reaches a disk and a later process finds it there.
/// </summary>
public sealed class FileLiveSourceStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-tests",
        Guid.NewGuid().ToString("N"));

    /// <summary>A store over the shared temp file.</summary>
    private FileLiveSourceStore Store()
        => new(
            Options.Create(new LiveOptions { SourceFile = Path.Combine(_directory, "sources.json") }),
            NullLogger<FileLiveSourceStore>.Instance);

    private static LiveSource Source(string name, params ForwardTarget[] forwards)
        => new(name, $"srt://camera/{name}", true, forwards, DateTimeOffset.UnixEpoch);

    [Fact]
    public async Task A_saved_source_comes_back_with_its_forwards()
    {
        var store = Store();
        await store.SaveAsync(Source("north", new ForwardTarget("f1", "srt://gateway:9000?streamid=north")));

        var source = Assert.Single(await store.ListAsync());

        Assert.Equal("north", source.Name);
        Assert.Equal("srt://camera/north", source.Url);
        Assert.True(source.IsPull);

        var forward = Assert.Single(source.Forwards);
        Assert.Equal("f1", forward.Id);
        Assert.Equal("srt://gateway:9000?streamid=north", forward.Url);
        Assert.True(forward.Enabled);
    }

    /// <summary>The reason the file exists at all.</summary>
    [Fact]
    public async Task A_new_store_over_the_same_file_sees_what_the_last_one_saved()
    {
        await Store().SaveAsync(Source("north"));

        var source = await Store().GetAsync("north");

        Assert.NotNull(source);
        Assert.Equal("srt://camera/north", source.Url);
    }

    [Fact]
    public async Task A_removed_source_is_gone_from_the_file_too()
    {
        var store = Store();
        await store.SaveAsync(Source("north"));
        await store.SaveAsync(Source("south"));

        await store.RemoveAsync("north");

        Assert.Equal(["south"], (await store.ListAsync()).Select(source => source.Name));
        Assert.Equal(["south"], (await Store().ListAsync()).Select(source => source.Name));
    }

    /// <summary>Saving is by name, so editing a source's URL has to replace the row.</summary>
    [Fact]
    public async Task Saving_an_existing_name_replaces_it_rather_than_duplicating()
    {
        var store = Store();
        await store.SaveAsync(Source("north"));
        await store.SaveAsync(new LiveSource("north", "srt://other/north", false, [], DateTimeOffset.UnixEpoch));

        var source = Assert.Single(await store.ListAsync());

        Assert.Equal("srt://other/north", source.Url);
        Assert.False(source.Enabled);
    }

    /// <summary>
    /// A file half-written by a killed process must not stop the service from starting, because
    /// every stream an encoder is pushing needs no source at all and would go down with it.
    /// </summary>
    [Fact]
    public async Task A_corrupt_file_reads_as_empty_and_is_kept_beside_the_new_one()
    {
        var path = Path.Combine(_directory, "sources.json");
        Directory.CreateDirectory(_directory);
        await File.WriteAllTextAsync(path, "[{\"Name\": \"north\", tru");

        Assert.Empty(await Store().ListAsync());

        Assert.True(File.Exists(path + ".corrupt"));
        Assert.Contains("north", await File.ReadAllTextAsync(path + ".corrupt"));
    }

    /// <summary>
    /// Every save rewrites the whole file, so two of them at once is exactly the case where one
    /// overwrites the other's row.
    /// </summary>
    [Fact]
    public async Task Concurrent_saves_of_different_names_both_survive()
    {
        var store = Store();
        var names = Enumerable.Range(0, 20).Select(index => $"camera-{index:00}").ToArray();

        await Task.WhenAll(names.Select(name => store.SaveAsync(Source(name))));

        Assert.Equal(names, (await Store().ListAsync()).Select(source => source.Name));
    }

    /// <summary>A save that cannot reach the disk must change nothing at all.</summary>
    [Fact]
    public async Task A_save_that_cannot_be_written_leaves_the_previous_list_alone()
    {
        var store = Store();
        await store.SaveAsync(Source("north"));

        Directory.CreateDirectory(Path.Combine(_directory, "sources.json.tmp"));

        await Assert.ThrowsAnyAsync<Exception>(() => store.SaveAsync(Source("south")));

        Assert.Equal("north", Assert.Single(await store.ListAsync()).Name);

        // And on disk too, which is the half that outlives this process.
        Directory.Delete(Path.Combine(_directory, "sources.json.tmp"));
        Assert.Equal("north", Assert.Single(await Store().ListAsync()).Name);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
