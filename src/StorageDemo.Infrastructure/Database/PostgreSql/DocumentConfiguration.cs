using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using StorageDemo.Core.Documents;

namespace StorageDemo.Infrastructure.Database.PostgreSql;

/// <summary>Keeps EF mapping out of the domain entity.</summary>
public sealed class DocumentConfiguration : IEntityTypeConfiguration<Document>
{
    public void Configure(EntityTypeBuilder<Document> builder)
    {
        builder.ToTable("documents");
        builder.HasKey(d => d.Id);

        builder.Property(d => d.FileName).HasMaxLength(512).IsRequired();
        builder.Property(d => d.StorageKey).HasMaxLength(1024).IsRequired();
        builder.Property(d => d.ContentType).HasMaxLength(256);
        builder.Property(d => d.Size);
        builder.Property(d => d.CreatedAt);
        builder.Property(d => d.ThumbnailKey).HasMaxLength(1024);

        // Media metadata is free-form, so it goes in one jsonb column rather than a column per key.
        builder.Property(d => d.Metadata)
            .HasColumnType("jsonb")
            .HasConversion(
                metadata => DocumentMetadata.Serialize(metadata)!,
                json => DocumentMetadata.Deserialize(json),
                new ValueComparer<IReadOnlyDictionary<string, string>>(
                    (left, right) => DocumentMetadata.Serialize(left) == DocumentMetadata.Serialize(right),
                    metadata => DocumentMetadata.Serialize(metadata)!.GetHashCode(),
                    metadata => DocumentMetadata.Deserialize(DocumentMetadata.Serialize(metadata))));

        builder.Property(d => d.Parts)
            .HasColumnType("jsonb")
            .HasConversion(
                parts => DocumentMetadata.SerializeParts(parts),
                json => DocumentMetadata.DeserializeParts(json),
                new ValueComparer<IReadOnlyList<DocumentPart>>(
                    (left, right) => DocumentMetadata.SerializeParts(left) == DocumentMetadata.SerializeParts(right),
                    parts => DocumentMetadata.SerializeParts(parts).GetHashCode(),
                    parts => DocumentMetadata.DeserializeParts(DocumentMetadata.SerializeParts(parts))));

        builder.Ignore(d => d.Segmented);

        builder.HasIndex(d => d.StorageKey).IsUnique();
    }
}
