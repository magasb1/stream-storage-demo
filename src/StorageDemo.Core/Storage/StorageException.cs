namespace StorageDemo.Core.Storage;

/// <summary>Provider-neutral failure.</summary>
public sealed class StorageException(string message, Exception? inner = null)
    : Exception(message, inner);
