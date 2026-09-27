using MagicBytesValidator.Services;
using MagicBytesValidator.Services.Streams;
using StorageDemo.Core.Documents;

namespace StorageDemo.Api.Uploads;

/// <summary>
/// Decides what an upload actually is, from its leading bytes rather than from what the client
/// claimed.
/// </summary>
public sealed class ContentTypeSniffer(ILogger<ContentTypeSniffer> logger)
{
    /// <summary>Enough for every signature in the table; the longest are a few dozen bytes.</summary>
    private const int HeadBytes = 512;

    private readonly StreamFileTypeProvider _provider = new(CreateMapping());

    public async Task<string?> ResolveAsync(
        string? declared,
        string fileName,
        ReadOnlyMemory<byte> head,
        CancellationToken cancellationToken)
    {
        var detected = await DetectAsync(head, cancellationToken);

        if (detected is null)
        {
            return Fallback(declared, fileName);
        }

        if (declared is not null
            && !declared.Equals(detected, StringComparison.OrdinalIgnoreCase)
            && declared != ContentTypes.Unknown)
        {
            logger.LogInformation(
                "Upload {FileName} was declared {DeclaredType} but its bytes say {DetectedType}",
                fileName,
                declared,
                detected);
        }

        return detected;
    }

    /// <summary>For callers holding a seekable stream, such as a buffered form upload.</summary>
    public async Task<string?> ResolveAsync(
        string? declared,
        string fileName,
        Stream content,
        CancellationToken cancellationToken)
    {
        if (!content.CanSeek)
        {
            return Fallback(declared, fileName);
        }

        var buffer = new byte[HeadBytes];
        var read = await content.ReadAsync(buffer, cancellationToken);
        content.Position = 0;

        return await ResolveAsync(declared, fileName, buffer.AsMemory(0, read), cancellationToken);
    }

    private async Task<string?> DetectAsync(ReadOnlyMemory<byte> head, CancellationToken cancellationToken)
    {
        if (head.IsEmpty)
        {
            return null;
        }

        using var stream = new MemoryStream(head.ToArray(), writable: false);

        try
        {
            // Unambiguous: a signature that several formats share tells us nothing worth acting on.
            var fileType = await _provider.TryFindUnambiguousAsync(stream, cancellationToken);

            return fileType?.MimeTypes.FirstOrDefault();
        }
        catch (Exception ex)
        {
            // Never fail an upload over type detection; the declared type still gets us there.
            logger.LogWarning(ex, "Content sniffing failed");
            return null;
        }
    }

    private static string Fallback(string? declared, string fileName)
        => string.IsNullOrWhiteSpace(declared) || declared == ContentTypes.Unknown
            ? ContentTypes.Guess(fileName)
            : declared;

    private static IMapping CreateMapping()
    {
        var mapping = new Mapping();
        mapping.Register(typeof(Mapping).Assembly);

        return mapping;
    }
}
