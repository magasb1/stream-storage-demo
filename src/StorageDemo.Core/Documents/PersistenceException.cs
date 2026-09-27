namespace StorageDemo.Core.Documents;

/// <summary>Provider-neutral failure.</summary>
public sealed class PersistenceException(string message, Exception? inner = null)
    : Exception(message, inner);
