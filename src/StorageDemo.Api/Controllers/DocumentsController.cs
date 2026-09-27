using Microsoft.AspNetCore.Mvc;
using Microsoft.Net.Http.Headers;
using StorageDemo.Api.Observability;
using StorageDemo.Api.Uploads;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Api.Controllers;

public sealed record DocumentResponse(
    Guid Id,
    string FileName,
    string? ContentType,
    long Size,
    DateTimeOffset CreatedAt,
    bool HasThumbnail,
    IReadOnlyDictionary<string, string> Metadata)
{
    public static DocumentResponse From(Document d)
        => new(d.Id, d.FileName, d.ContentType, d.Size, d.CreatedAt, d.ThumbnailKey is not null, d.Metadata);
}

[ApiController]
[Route("api/documents")]
public sealed class DocumentsController(
    IDocumentService documents,
    ContentTypeSniffer sniffer,
    ApiMetrics metrics) : ControllerBase
{
    /// <summary>
    /// Which surface these figures came from. REST and gRPC write to the same instruments with this
    /// telling them apart, because "how much was uploaded" is a question about the store and not
    /// about the protocol, and "which surface is anybody actually using" is worth asking too.
    /// </summary>
    private const string Surface = "rest";

    private const string Document = "document";

    private const string Thumbnail = "thumbnail";

    [HttpGet]
    public async Task<IReadOnlyList<DocumentResponse>> GetAll(CancellationToken cancellationToken)
        => (await documents.GetAllAsync(cancellationToken)).Select(DocumentResponse.From).ToList();

    /// <summary>
    /// Every recording and snapshot one detection caused. The three parameters are the three things
    /// that identify a detection: the stream, the VMTI precision timestamp of the frame, and the
    /// target id within it.
    ///
    /// It is here rather than on the live surface because by the time anyone asks, the stream may
    /// be long gone and the document is what survives.
    /// </summary>
    [HttpGet("by-detection")]
    public async Task<IReadOnlyList<DocumentResponse>> GetByDetection(
        [FromQuery] string stream,
        [FromQuery] DateTimeOffset timestamp,
        [FromQuery] int targetId,
        CancellationToken cancellationToken)
        => (await documents.FindByDetectionAsync(
                new DetectionReference(stream, timestamp, targetId),
                cancellationToken))
            .Select(DocumentResponse.From)
            .ToList();

    [HttpGet("{id:guid}")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<DocumentResponse>> Get(Guid id, CancellationToken cancellationToken)
    {
        var document = await documents.GetAsync(id, cancellationToken);
        return document is null ? NotFound() : DocumentResponse.From(document);
    }

    /// <param name="download">
    /// False (the default) serves the object inline so the browser can display it, which is what
    /// the explorer UI needs. True forces a save dialog.
    /// </param>
    [HttpGet("{id:guid}/content")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetContent(
        Guid id,
        CancellationToken cancellationToken,
        [FromQuery] bool download = false)
    {
        var content = await documents.DownloadAsync(id, cancellationToken);
        if (content is null)
        {
            metrics.Downloaded(Surface, Document, "missing");

            return NotFound();
        }

        // Counted as a request served, and no byte figure: what follows is handed to Kestrel and may
        // be a range of the file rather than all of it, so the only number available here would be
        // the document's size and it would often be wrong. Kestrel's own meter answers that honestly.
        metrics.Downloaded(Surface, Document, "served");

        // Uploaded bytes are served from our own origin, so an uploaded .html or .svg would
        // otherwise run as first-party script. nosniff pins the declared type and the sandbox
        // directive strips scripting and same-origin access from whatever is rendered.
        Response.Headers.XContentTypeOptions = "nosniff";
        Response.Headers.ContentSecurityPolicy = "sandbox; default-src 'none'; img-src 'self'; media-src 'self'";
        Response.Headers.ContentDisposition = new ContentDispositionHeaderValue(
            download ? "attachment" : "inline")
        {
            FileNameStar = content.FileName,
        }.ToString();

        // FileStreamResult streams and disposes the stream; the whole file is never buffered.
        // Range processing lets a browser seek within a video instead of refetching it.
        return File(
            content.Stream,
            content.ContentType ?? "application/octet-stream",
            enableRangeProcessing: true);
    }

    [HttpGet("{id:guid}/thumbnail")]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetThumbnail(Guid id, CancellationToken cancellationToken)
    {
        var content = await documents.DownloadThumbnailAsync(id, cancellationToken);
        if (content is null)
        {
            metrics.Downloaded(Surface, Thumbnail, "missing");

            return NotFound();
        }

        metrics.Downloaded(Surface, Thumbnail, "served");

        // Generated by us, not by the uploader, so it carries no untrusted markup.
        Response.Headers.XContentTypeOptions = "nosniff";

        return File(content.Stream, "image/jpeg");
    }

    [HttpPost]
    [ProducesResponseType(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<DocumentResponse>> Upload(
        IFormFile file,
        CancellationToken cancellationToken)
    {
        if (file is null || file.Length == 0)
        {
            metrics.Uploaded(Surface, "rejected", 0);

            return BadRequest("A non-empty file is required.");
        }

        // IFormFile stops at the Api layer; the service only sees a Stream.
        await using var stream = file.OpenReadStream();

        var contentType = await sniffer.ResolveAsync(
            file.ContentType,
            file.FileName,
            stream,
            cancellationToken);

        var document = await documents.UploadAsync(
            file.FileName,
            stream,
            contentType,
            cancellationToken);

        metrics.Uploaded(Surface, "stored", document.Size);

        return CreatedAtAction(
            nameof(Get),
            new { id = document.Id },
            DocumentResponse.From(document));
    }

    [HttpDelete("{id:guid}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> Delete(Guid id, CancellationToken cancellationToken)
    {
        await documents.DeleteAsync(id, cancellationToken);
        return NoContent();
    }
}
