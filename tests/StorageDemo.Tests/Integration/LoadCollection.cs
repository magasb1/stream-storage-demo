namespace StorageDemo.Tests.Integration;

/// <summary>
/// Runs alone, and nothing else runs while it does.
///
/// For the tests whose subject is the machine rather than a mechanism: what the process is spending,
/// how many threads it holds, whether a queue overflowed. Every one of those reads something
/// process-global, and xunit runs collections in parallel by default, so a neighbour is not noise -
/// it is the measurement. A thread-count bound is the sharpest example: the pool of a process that
/// has been running the rest of the suite is already grown, so blocking two dozen workers needs no
/// new ones and the bound holds while the fault it exists to catch is present.
///
/// The ONNX tests have a collection of their own for the same reason.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LoadCollection
{
    public const string Name = "live-load";
}
