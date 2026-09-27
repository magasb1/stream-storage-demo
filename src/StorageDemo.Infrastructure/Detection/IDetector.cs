using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Detection;

/// <summary>
/// The seam from detection-plan.md: a decoded picture in, boxes in original-frame pixels out,
/// whatever model sits behind it.
/// </summary>
public interface IDetector
{
    /// <summary>Several frames as one model batch, one result array per frame in the same order.</summary>
    VmtiDetection[][] Detect(ReadOnlySpan<IntPtr> frames);

    VmtiDetection[] Detect(IntPtr frame) => Detect([frame])[0];
}
