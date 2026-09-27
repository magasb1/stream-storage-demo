using System.Net;
using System.Runtime.CompilerServices;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Storage;

namespace StorageDemo.Infrastructure.FileStorage.S3;

/// <summary>Stores objects in an S3 bucket.</summary>
public sealed class S3Storage(IAmazonS3 client, IOptions<S3StorageOptions> options) : IFileStorage
{
    private readonly string _bucket = options.Value.Bucket;

    public async Task SaveAsync(
        string key,
        Stream content,
        string? contentType,
        CancellationToken cancellationToken = default)
    {
        // The SDK checksums the body before sending, which means rewinding it.
        if (content.CanSeek)
        {
            await PutAsync(key, content, contentType, cancellationToken);
            return;
        }

        var spoolPath = Path.Combine(Path.GetTempPath(), $"storagedemo-s3-{Guid.NewGuid():N}");

        await using var spool = new FileStream(
            spoolPath,
            FileMode.CreateNew,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: 81920,
            FileOptions.DeleteOnClose | FileOptions.Asynchronous);

        await content.CopyToAsync(spool, cancellationToken);
        spool.Position = 0;

        await PutAsync(key, spool, contentType, cancellationToken);
    }

    private async Task PutAsync(
        string key,
        Stream content,
        string? contentType,
        CancellationToken cancellationToken)
    {
        var request = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = Normalize(key),
            InputStream = content,
            AutoCloseStream = false,
            ContentType = contentType,
        };

        try
        {
            await client.PutObjectAsync(request, cancellationToken);
        }
        catch (AmazonS3Exception ex)
        {
            throw new StorageException($"Failed to write '{key}' to bucket '{_bucket}'.", ex);
        }
    }

    public Task<Stream?> OpenReadAsync(string key, CancellationToken cancellationToken = default)
        => OpenReadAsync(key, 0, cancellationToken);

    public async Task<Stream?> OpenReadAsync(
        string key,
        long offset,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (offset <= 0)
            {
                var whole = await client.GetObjectAsync(_bucket, Normalize(key), cancellationToken);

                return whole.ResponseStream;
            }

            // An open-ended range, which is a plain GET with a Range header rather than anything S3
            // has that a filesystem does not.
            var response = await client.GetObjectAsync(
                new GetObjectRequest
                {
                    BucketName = _bucket,
                    Key = Normalize(key),
                    ByteRange = new ByteRange(offset, long.MaxValue),
                },
                cancellationToken);

            return response.ResponseStream;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (AmazonS3Exception ex)
        {
            throw new StorageException($"Failed to read '{key}' from bucket '{_bucket}'.", ex);
        }
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            // S3 delete is already idempotent: a missing key returns 204.
            await client.DeleteObjectAsync(_bucket, Normalize(key), cancellationToken);
        }
        catch (AmazonS3Exception ex)
        {
            throw new StorageException($"Failed to delete '{key}' from bucket '{_bucket}'.", ex);
        }
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            await client.GetObjectMetadataAsync(_bucket, Normalize(key), cancellationToken);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (AmazonS3Exception ex)
        {
            throw new StorageException($"Failed to stat '{key}' in bucket '{_bucket}'.", ex);
        }
    }

    public async IAsyncEnumerable<StorageObject> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var request = new ListObjectsV2Request
        {
            BucketName = _bucket,
            Prefix = string.IsNullOrWhiteSpace(prefix) ? null : Normalize(prefix),
        };

        do
        {
            ListObjectsV2Response response;
            try
            {
                response = await client.ListObjectsV2Async(request, cancellationToken);
            }
            catch (AmazonS3Exception ex)
            {
                throw new StorageException($"Failed to list bucket '{_bucket}'.", ex);
            }

            foreach (var entry in response.S3Objects ?? [])
            {
                yield return new StorageObject(
                    entry.Key,
                    entry.Size ?? 0,
                    entry.LastModified ?? DateTimeOffset.MinValue);
            }

            // S3 pages at 1000 keys; keep following the continuation token.
            request.ContinuationToken = response.NextContinuationToken;
        }
        while (request.ContinuationToken is not null);
    }

    public async Task<bool> BucketIsReachableAsync(CancellationToken cancellationToken)
    {
        try
        {
            await client.GetBucketLocationAsync(_bucket, cancellationToken);
            return true;
        }
        catch (AmazonS3Exception)
        {
            return false;
        }
        catch (HttpRequestException)
        {
            return false;
        }
    }

    private static string Normalize(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return key.Replace('\\', '/').TrimStart('/');
    }
}
