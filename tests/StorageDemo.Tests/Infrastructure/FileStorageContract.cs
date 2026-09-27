using StorageDemo.Core.Storage;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>One behavioural spec, run against every <see cref="IFileStorage"/> implementation.</summary>
public abstract class FileStorageContract
{
    protected abstract IFileStorage CreateStorage();

    private static MemoryStream Content(string text) => new(System.Text.Encoding.UTF8.GetBytes(text));

    [Fact]
    public async Task Save_then_read_returns_the_same_bytes()
    {
        var storage = CreateStorage();
        await storage.SaveAsync("documents/a/file.txt", Content("payload"), "text/plain");

        await using var stream = await storage.OpenReadAsync("documents/a/file.txt");

        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("payload", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Content_arriving_as_a_forward_only_stream_is_stored()
    {
        // What an upload actually looks like: no length, no seeking.
        var storage = CreateStorage();

        await storage.SaveAsync("documents/streamed.txt", new ForwardOnlyStream("payload"u8.ToArray()), null);

        await using var stream = await storage.OpenReadAsync("documents/streamed.txt");
        Assert.NotNull(stream);

        using var reader = new StreamReader(stream);
        Assert.Equal("payload", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Nested_keys_are_supported()
    {
        var storage = CreateStorage();
        await storage.SaveAsync("a/b/c/d/file.txt", Content("deep"), null);

        Assert.True(await storage.ExistsAsync("a/b/c/d/file.txt"));
    }

    [Fact]
    public async Task Save_overwrites_an_existing_key()
    {
        var storage = CreateStorage();
        await storage.SaveAsync("k.txt", Content("first"), null);
        await storage.SaveAsync("k.txt", Content("second"), null);

        await using var stream = await storage.OpenReadAsync("k.txt");
        using var reader = new StreamReader(stream!);
        Assert.Equal("second", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Reading_a_missing_key_returns_null()
        => Assert.Null(await CreateStorage().OpenReadAsync("missing.txt"));

    [Fact]
    public async Task Exists_is_false_for_a_missing_key()
        => Assert.False(await CreateStorage().ExistsAsync("missing.txt"));

    [Fact]
    public async Task Delete_removes_the_object()
    {
        var storage = CreateStorage();
        await storage.SaveAsync("k.txt", Content("x"), null);

        await storage.DeleteAsync("k.txt");

        Assert.False(await storage.ExistsAsync("k.txt"));
    }

    [Fact]
    public async Task Delete_of_a_missing_key_succeeds()
        => await CreateStorage().DeleteAsync("missing.txt");

    [Fact]
    public async Task List_returns_every_object_under_the_prefix()
    {
        var storage = CreateStorage();
        await storage.SaveAsync("documents/a/one.txt", Content("one"), null);
        await storage.SaveAsync("documents/b/two.txt", Content("two"), null);
        await storage.SaveAsync("other/three.txt", Content("three"), null);

        var keys = new List<string>();
        await foreach (var obj in storage.ListAsync("documents/"))
        {
            keys.Add(obj.Key);
        }

        Assert.Equal(
            ["documents/a/one.txt", "documents/b/two.txt"],
            keys.Order().ToArray());
    }

    [Fact]
    public async Task List_reports_the_object_size()
    {
        var storage = CreateStorage();
        await storage.SaveAsync("documents/one.txt", Content("payload"), null);

        var listed = new List<StorageObject>();
        await foreach (var obj in storage.ListAsync("documents/"))
        {
            listed.Add(obj);
        }

        Assert.Equal(7, Assert.Single(listed).Size);
    }

    /// <summary>A network upload: readable once, no length, no seeking.</summary>
    private sealed class ForwardOnlyStream(byte[] payload) : Stream
    {
        private int _position;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = Math.Min(count, payload.Length - _position);
            Array.Copy(payload, _position, buffer, offset, read);
            _position += read;

            return read;
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public async Task List_of_an_empty_prefix_yields_nothing()
    {
        var listed = new List<StorageObject>();
        await foreach (var obj in CreateStorage().ListAsync("documents/"))
        {
            listed.Add(obj);
        }

        Assert.Empty(listed);
    }
}
