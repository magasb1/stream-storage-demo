using System.Text.Json;

namespace StorageDemo.Core.Documents;

/// <summary>
/// One place that decides how the metadata bag is written down, so LiteDB and PostgreSQL store
/// byte-identical JSON and the contract tests can hold them to the same result.
/// </summary>
public static class DocumentMetadata
{
    private static readonly JsonSerializerOptions Options = new() { WriteIndented = false };

    public static string Serialize(IReadOnlyDictionary<string, string>? metadata)
        => metadata is null || metadata.Count == 0
            ? "{}"
            : JsonSerializer.Serialize(metadata, Options);

    /// <summary>
    /// The pieces of a segmented document, written down the same way and for the same reason: both
    /// stores keep them as one JSON value rather than a table, and the contract tests hold the two
    /// to the same result.
    /// </summary>
    public static string SerializeParts(IReadOnlyList<DocumentPart>? parts)
        => parts is null || parts.Count == 0 ? "[]" : JsonSerializer.Serialize(parts, Options);

    public static IReadOnlyList<DocumentPart> DeserializeParts(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return [];
        }

        try
        {
            return JsonSerializer.Deserialize<List<DocumentPart>>(json, Options) ?? [];
        }
        catch (JsonException)
        {
            // Losing the pieces would make a recording unreadable, so this is worth a loud failure
            // rather than the quiet empty answer metadata gets.
            throw new PersistenceException($"A document's parts could not be read: {json}");
        }
    }

    public static IReadOnlyDictionary<string, string> Deserialize(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return new Dictionary<string, string>();
        }

        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, string>>(json, Options)
                ?? new Dictionary<string, string>();
        }
        catch (JsonException)
        {
            // A row written by an older version, or hand-edited.
            return new Dictionary<string, string>();
        }
    }
}
