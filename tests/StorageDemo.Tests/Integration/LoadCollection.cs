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
/// It cuts both ways, which is why one collection serves both kinds of test in here. A thread-count
/// bound false-passes beside a busy neighbour; a load test that takes every core the machine has
/// makes its neighbours time out, because the rest of the live suite measures grace periods against
/// a wall clock. Run either in parallel with anything and both halves lie.
///
/// The ONNX tests have a collection of their own for the same reason.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LoadCollection
{
    public const string Name = "live-load";
}
