using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Infrastructure.FileStorage.S3;

public sealed class S3StorageOptions
{
    public const string SectionName = "Storage:S3";

    [Required(AllowEmptyStrings = false)]
    public string Bucket { get; init; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    public string Region { get; init; } = string.Empty;

    /// <summary>Set only for S3-compatible systems such as SeaweedFS.</summary>
    public string? ServiceUrl { get; init; }

    public bool ForcePathStyle { get; init; }
}
