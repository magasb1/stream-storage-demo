using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("StorageDemo.Tests")]

namespace StorageDemo.Core.Streaming;

/// <summary>
/// ByteTrack (Zhang et al., "ByteTrack: Multi-Object Tracking by Associating Every Detection Box",
/// ECCV 2022, arXiv:2110.06864), following the reference implementation ifzhang/ByteTrack,
/// yolox/tracker/byte_tracker.py, kalman_filter.py and matching.py.
/// </summary>
public sealed class ByteTracker
{
    /// <summary>Goes out as VTracker LS tag 6, which wants a name that identifies the method.</summary>
    public const string Algorithm = "ByteTrack (Zhang et al., ECCV 2022)";

    private readonly List<Track> _tracks = [];
    private readonly double _detectionThreshold;
    private readonly double _matchThreshold;
    private readonly int _trackBuffer;
    private int _nextId;

    /// <param name="frameWidth">Predicted boxes are clamped to the frame, so it must be known.</param>
    /// <param name="detectionThreshold">
    /// The paper's tau, 0.6 (section 4.1, "the default detection score threshold is 0.6").
    /// </param>
    /// <param name="matchThreshold">
    /// Cost above which the first association refuses a pair, on a 1 - IoU scale: the reference's
    /// match_thresh of 0.8, which is the paper's "if the IoU ...
    /// </param>
    /// <param name="trackBuffer">
    /// Frames a track survives unseen before it is removed: 30, "we keep it for 30 frames in case
    /// it appears again" (section 4.1; byte_tracker.py 155-156, 272).
    /// </param>
    public ByteTracker(
        int frameWidth,
        int frameHeight,
        double detectionThreshold = 0.6,
        double matchThreshold = 0.8,
        int trackBuffer = 30)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(frameWidth, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(frameHeight, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(trackBuffer, 1);

        FrameWidth = frameWidth;
        FrameHeight = frameHeight;
        _detectionThreshold = detectionThreshold;
        _matchThreshold = matchThreshold;
        _trackBuffer = trackBuffer;
    }

    internal int FrameWidth { get; }

    internal int FrameHeight { get; }

    internal int TrackBuffer => _trackBuffer;

    /// <summary>How many steps have been taken.</summary>
    public int Frame { get; private set; }

    /// <summary>The tracks a consumer should see: active and confirmed, in creation order.</summary>
    public IReadOnlyList<Track> Tracks
        => _tracks.Where(t => t.Status == VmtiTrackStatus.Active && t.Confirmed).ToList();

    /// <summary>A frame with no detector run: every track coasts on its motion model.</summary>
    public void Predict(DateTimeOffset timestamp)
    {
        Frame++;
        PredictAll();
        Expire();
    }

    /// <summary>A frame the detector ran on, possibly finding nothing.</summary>
    public void Update(DateTimeOffset timestamp, IReadOnlyList<VmtiDetection> detections)
    {
        ArgumentNullException.ThrowIfNull(detections);

        Frame++;
        PredictAll();

        // byte_tracker.py 177-181 splits at track_thresh with strict inequalities on both sides,
        // which drops a score that lands exactly on it; here the low set takes everything the high
        // set did not, above the 0.1 floor the reference treats as background.
        var high = detections.Where(d => Score(d) > _detectionThreshold).ToList();
        var low = detections.Where(d => Score(d) > 0.1 && Score(d) <= _detectionThreshold).ToList();

        // byte_tracker.py 194-204: the pool for the first association is the confirmed tracks plus
        // the lost ones; tracks seen only once so far are handled after.
        var pool = _tracks.Where(t => t.Confirmed).ToList();
        var unconfirmed = _tracks.Where(t => !t.Confirmed).ToList();

        // First association (Algorithm 1 lines 17-19; byte_tracker.py 207-221): IoU fused with the
        // detection score, matching.py fuse_score, the non-MOT20 branch.
        var (matches, unmatchedTracks, unmatchedHigh) = Assign(Fused(pool, high), _matchThreshold);

        foreach (var (t, d) in matches)
        {
            pool[t].Observe(high[d], timestamp, Frame);
        }

        // Second association (Algorithm 1 lines 20-21; byte_tracker.py 230-241): the tracks still
        // active but unmatched, against the low-score boxes, on IoU alone at 0.5.
        var remaining = unmatchedTracks.Select(i => pool[i]).Where(t => t.Status == VmtiTrackStatus.Active).ToList();
        var (secondMatches, stillUnmatched, _) = Assign(IouCost(remaining, low), 0.5);

        foreach (var (t, d) in secondMatches)
        {
            remaining[t].Observe(low[d], timestamp, Frame);
        }

        foreach (var i in stillUnmatched)
        {
            remaining[i].Status = VmtiTrackStatus.Dropped; // mark_lost, byte_tracker.py 243-247
        }

        // byte_tracker.py 249-261: a track from a single earlier detection must be seen again to be
        // confirmed, at a stricter 0.7, or it is removed.
        var leftover = unmatchedHigh.Select(i => high[i]).ToList();
        var (confirmations, unconfirmedLost, unmatchedLeftover) = Assign(Fused(unconfirmed, leftover), 0.7);

        foreach (var (t, d) in confirmations)
        {
            unconfirmed[t].Observe(leftover[d], timestamp, Frame);
        }

        foreach (var i in unconfirmedLost)
        {
            unconfirmed[i].Status = VmtiTrackStatus.Inactive;
        }

        // Algorithm 1 lines 23-25; byte_tracker.py 263-269: new tracks from the high boxes nobody
        // claimed, if they clear det_thresh = track_thresh + 0.1 (line 154).
        foreach (var i in unmatchedLeftover)
        {
            if (Score(leftover[i]) < _detectionThreshold + 0.1)
            {
                continue;
            }

            _tracks.Add(new Track(this, ++_nextId, leftover[i], timestamp, confirmed: Frame == 1));
        }

        Expire();
    }

    /// <summary>
    /// byte_tracker.py 206 predicts only the confirmed pool; here the unconfirmed are advanced too,
    /// because with detections every k-th frame their next chance to be confirmed is k frames away,
    /// and a box left where it was is not going to overlap a moving object by then.
    /// </summary>
    private void PredictAll()
    {
        foreach (var track in _tracks)
        {
            track.Filter.Predict(lost: track.Status != VmtiTrackStatus.Active);
        }
    }

    /// <summary>byte_tracker.py 271-274: a lost track older than the buffer is removed.</summary>
    private void Expire()
    {
        foreach (var track in _tracks)
        {
            if (track.Status == VmtiTrackStatus.Dropped && Frame - track.LastFrame > _trackBuffer)
            {
                track.Status = VmtiTrackStatus.Inactive;
            }
        }

        _tracks.RemoveAll(t => t.Status == VmtiTrackStatus.Inactive);
    }

    /// <summary>ST 0903 carries a percentage; the paper works in 0..1.</summary>
    private static double Score(VmtiDetection detection) => (detection.ConfidencePercent ?? 100) / 100.0;

    private static double[,] IouCost(List<Track> tracks, List<VmtiDetection> detections)
    {
        var cost = new double[tracks.Count, detections.Count];

        for (var t = 0; t < tracks.Count; t++)
        {
            for (var d = 0; d < detections.Count; d++)
            {
                cost[t, d] = 1 - Iou(tracks[t].Filter.Tlbr(), Track.Tlbr(detections[d]));
            }
        }

        return cost;
    }

    /// <summary>matching.py fuse_score: cost = 1 - IoU x score, so a weak box is a weaker claim.</summary>
    private static double[,] Fused(List<Track> tracks, List<VmtiDetection> detections)
    {
        var cost = IouCost(tracks, detections);

        for (var t = 0; t < tracks.Count; t++)
        {
            for (var d = 0; d < detections.Count; d++)
            {
                cost[t, d] = 1 - ((1 - cost[t, d]) * Score(detections[d]));
            }
        }

        return cost;
    }

    private static double Iou((double L, double T, double R, double B) a, (double L, double T, double R, double B) b)
    {
        var w = Math.Max(0, Math.Min(a.R, b.R) - Math.Max(a.L, b.L));
        var h = Math.Max(0, Math.Min(a.B, b.B) - Math.Max(a.T, b.T));
        var intersection = w * h;
        var union = ((a.R - a.L) * (a.B - a.T)) + ((b.R - b.L) * (b.B - b.T)) - intersection;

        return union <= 0 ? 0 : intersection / union;
    }

    /// <summary>
    /// matching.py linear_assignment, which calls lap.lapjv(extend_cost=True, cost_limit=thresh).
    /// </summary>
    private static (List<(int Track, int Detection)> Matches, List<int> UnmatchedRows, List<int> UnmatchedColumns) Assign(
        double[,] cost,
        double limit)
    {
        var n = cost.GetLength(0);
        var m = cost.GetLength(1);
        var matches = new List<(int, int)>();
        var unmatchedRows = new List<int>();
        var unmatchedColumns = new List<int>();

        if (n == 0 || m == 0)
        {
            unmatchedRows.AddRange(Enumerable.Range(0, n));
            unmatchedColumns.AddRange(Enumerable.Range(0, m));

            return (matches, unmatchedRows, unmatchedColumns);
        }

        var extended = new double[n + m, n + m];

        for (var i = 0; i < n + m; i++)
        {
            for (var j = 0; j < n + m; j++)
            {
                extended[i, j] = i < n && j < m ? cost[i, j] : i >= n && j >= m ? 0 : limit / 2;
            }
        }

        var assignment = Hungarian(extended);
        var matchedColumns = new bool[m];

        for (var i = 0; i < n; i++)
        {
            if (assignment[i] < m)
            {
                matches.Add((i, assignment[i]));
                matchedColumns[assignment[i]] = true;
            }
            else
            {
                unmatchedRows.Add(i);
            }
        }

        unmatchedColumns.AddRange(Enumerable.Range(0, m).Where(j => !matchedColumns[j]));

        return (matches, unmatchedRows, unmatchedColumns);
    }

    /// <summary>
    /// Kuhn's Hungarian method (Kuhn, "The Hungarian method for the assignment problem", Naval
    /// Research Logistics Quarterly 2, 1955) in the O(n^3) potentials form given at
    /// cp-algorithms.com/graph/hungarian-algorithm.html, one-indexed as written there.
    /// </summary>
    private static int[] Hungarian(double[,] a)
    {
        var n = a.GetLength(0);
        var u = new double[n + 1];
        var v = new double[n + 1];
        var p = new int[n + 1];
        var way = new int[n + 1];

        for (var i = 1; i <= n; i++)
        {
            p[0] = i;
            var j0 = 0;
            var minv = new double[n + 1];
            var used = new bool[n + 1];
            Array.Fill(minv, double.PositiveInfinity);

            do
            {
                used[j0] = true;
                var i0 = p[j0];
                var delta = double.PositiveInfinity;
                var j1 = 0;

                for (var j = 1; j <= n; j++)
                {
                    if (used[j])
                    {
                        continue;
                    }

                    var cur = a[i0 - 1, j - 1] - u[i0] - v[j];

                    if (cur < minv[j])
                    {
                        minv[j] = cur;
                        way[j] = j0;
                    }

                    if (minv[j] < delta)
                    {
                        delta = minv[j];
                        j1 = j;
                    }
                }

                for (var j = 0; j <= n; j++)
                {
                    if (used[j])
                    {
                        u[p[j]] += delta;
                        v[j] -= delta;
                    }
                    else
                    {
                        minv[j] -= delta;
                    }
                }

                j0 = j1;
            }
            while (p[j0] != 0);

            do
            {
                var j1 = way[j0];
                p[j0] = p[j1];
                j0 = j1;
            }
            while (j0 != 0);
        }

        var result = new int[n];

        for (var j = 1; j <= n; j++)
        {
            if (p[j] != 0)
            {
                result[p[j] - 1] = j - 1;
            }
        }

        return result;
    }
}

/// <summary>
/// One tracked object: a stable identity across detections, and where the motion model puts it now.
/// </summary>
public sealed class Track
{
    private readonly ByteTracker _owner;
    private readonly List<(DateTimeOffset Timestamp, VmtiDetection Box)> _history = [];

    internal Track(ByteTracker owner, int id, VmtiDetection first, DateTimeOffset timestamp, bool confirmed)
    {
        _owner = owner;
        Id = id;
        Started = timestamp;
        var (x, y, a, h) = Xyah(first);
        Filter = new KalmanFilter(x, y, a, h); // STrack.activate, byte_tracker.py 45-57
        Observe(first, timestamp, owner.Frame);
        Confirmed = confirmed;
    }

    /// <summary>Monotonic within a session, never reused; the VTarget Pack Target ID.</summary>
    public int Id { get; }

    /// <summary>The VTracker LS Track ID, a UUID per ST 0903.4-53, minted at birth.</summary>
    public Guid Uuid { get; } = Guid.NewGuid();

    public VmtiTrackStatus Status { get; internal set; } = VmtiTrackStatus.Active;

    public DateTimeOffset Started { get; }

    /// <summary>When a detection last confirmed this track, which a prediction does not move.</summary>
    public DateTimeOffset LastSeen { get; private set; }

    /// <summary>
    /// Confidence and class of the detection last matched, which is what the reference reports
    /// (byte_tracker.py 88).
    /// </summary>
    public int? ConfidencePercent { get; private set; }

    public string? OntologyClass { get; private set; }

    /// <summary>
    /// The last <see cref="ByteTracker.TrackBuffer"/> observed boxes, oldest first, relabelled with
    /// this track's id.
    /// </summary>
    public IReadOnlyList<(DateTimeOffset Timestamp, VmtiDetection Box)> History => _history;

    /// <summary>
    /// The current box, predicted or corrected, ready for <see cref="Misb0903.Encode"/>: clamped to
    /// the frame, one-based and inclusive as <see cref="VmtiDetection"/> requires, carrying the
    /// VTracker LS.
    /// </summary>
    public VmtiDetection Box
    {
        get
        {
            var (l, t, r, b) = Filter.Tlbr();
            var left = Math.Clamp((int)Math.Round(l), 1, _owner.FrameWidth);
            var top = Math.Clamp((int)Math.Round(t), 1, _owner.FrameHeight);

            return new VmtiDetection(
                Id,
                left,
                top,
                Math.Clamp((int)Math.Round(r) - 1, left, _owner.FrameWidth),
                Math.Clamp((int)Math.Round(b) - 1, top, _owner.FrameHeight),
                ConfidencePercent,
                OntologyClass,
                new VmtiTrack(Uuid, Status, Started, LastSeen, ByteTracker.Algorithm));
        }
    }

    internal KalmanFilter Filter { get; }

    /// <summary>STrack.is_activated: seen in two frames, or born on the first one.</summary>
    internal bool Confirmed { get; private set; }

    /// <summary>The frame of the last observation; STrack.end_frame.</summary>
    internal int LastFrame { get; private set; }

    /// <summary>
    /// STrack.update and re_activate (byte_tracker.py 59-88) collapsed: correct the filter, go
    /// active, remember what was seen.
    /// </summary>
    internal void Observe(VmtiDetection detection, DateTimeOffset timestamp, int frame)
    {
        var (x, y, a, h) = Xyah(detection);
        Filter.Update(x, y, a, h);
        Status = VmtiTrackStatus.Active;
        Confirmed = true;
        LastSeen = timestamp;
        LastFrame = frame;
        ConfidencePercent = detection.ConfidencePercent;
        OntologyClass = detection.OntologyClass;

        if (_history.Count == _owner.TrackBuffer)
        {
            _history.RemoveAt(0);
        }

        _history.Add((timestamp, detection with { Id = Id, Track = null }));
    }

    /// <summary>
    /// A one-based inclusive pixel box as continuous edges: the right and bottom edges sit one past
    /// the last pixel, so width is Right - Left + 1, which is also what the reference's bbox_ious
    /// assumes of integer boxes.
    /// </summary>
    internal static (double L, double T, double R, double B) Tlbr(VmtiDetection d)
        => (d.Left, d.Top, d.Right + 1, d.Bottom + 1);

    /// <summary>STrack.tlwh_to_xyah, byte_tracker.py 115-122: centre, width over height, height.</summary>
    private static (double X, double Y, double A, double H) Xyah(VmtiDetection d)
    {
        var (l, t, r, b) = Tlbr(d);

        return ((l + r) / 2, (t + b) / 2, (r - l) / (b - t), b - t);
    }
}

/// <summary>
/// kalman_filter.py: an eight-dimensional constant-velocity filter over centre x, centre y, aspect
/// ratio, height and their velocities, observing the first four directly.
/// </summary>
internal sealed class KalmanFilter
{
    // kalman_filter.py __init__: _std_weight_position and _std_weight_velocity.
    private const double PositionWeight = 1.0 / 20;
    private const double VelocityWeight = 1.0 / 160;

    // _motion_mat: identity with dt = 1 on the velocity terms.
    private static readonly double[,] Motion = BuildMotion();

    /// <summary>
    /// kalman_filter.py initiate: the box, zero velocity, and the initial standard deviations from
    /// that method.
    /// </summary>
    public KalmanFilter(double x, double y, double aspect, double height)
    {
        Mean = [x, y, aspect, height, 0, 0, 0, 0];
        Covariance = DiagonalOfSquares(
            2 * PositionWeight * height,
            2 * PositionWeight * height,
            1e-2,
            2 * PositionWeight * height,
            10 * VelocityWeight * height,
            10 * VelocityWeight * height,
            1e-5,
            10 * VelocityWeight * height);
    }

    /// <summary>x, y, a, h, vx, vy, va, vh.</summary>
    public double[] Mean { get; }

    public double[,] Covariance { get; private set; }

    /// <summary>kalman_filter.py predict.</summary>
    public void Predict(bool lost)
    {
        if (lost)
        {
            Mean[7] = 0;
        }

        var h = Mean[3];
        var noise = DiagonalOfSquares(
            PositionWeight * h,
            PositionWeight * h,
            1e-2,
            PositionWeight * h,
            VelocityWeight * h,
            VelocityWeight * h,
            1e-5,
            VelocityWeight * h);

        for (var i = 0; i < 4; i++)
        {
            Mean[i] += Mean[i + 4];
        }

        Covariance = Add(Multiply(Multiply(Motion, Covariance), Transpose(Motion)), noise);
    }

    /// <summary>kalman_filter.py project then update.</summary>
    public void Update(double x, double y, double aspect, double height)
    {
        var h = Mean[3];
        var measurementNoise = DiagonalOfSquares(PositionWeight * h, PositionWeight * h, 1e-1, PositionWeight * h);
        var projected = new double[4, 4];
        var crossCovariance = new double[8, 4];

        for (var i = 0; i < 8; i++)
        {
            for (var j = 0; j < 4; j++)
            {
                crossCovariance[i, j] = Covariance[i, j];

                if (i < 4)
                {
                    projected[i, j] = Covariance[i, j] + measurementNoise[i, j];
                }
            }
        }

        var gain = Multiply(crossCovariance, Invert(projected));
        double[] innovation = [x - Mean[0], y - Mean[1], aspect - Mean[2], height - Mean[3]];

        for (var i = 0; i < 8; i++)
        {
            for (var j = 0; j < 4; j++)
            {
                Mean[i] += gain[i, j] * innovation[j];
            }
        }

        Covariance = Subtract(Covariance, Multiply(Multiply(gain, projected), Transpose(gain)));
    }

    /// <summary>STrack.tlbr via tlwh, byte_tracker.py 90-111: edges from centre, aspect and height.</summary>
    public (double L, double T, double R, double B) Tlbr()
    {
        var h = Math.Max(Mean[3], 1);
        var w = Math.Max(Mean[2] * h, 1);

        return (Mean[0] - (w / 2), Mean[1] - (h / 2), Mean[0] + (w / 2), Mean[1] + (h / 2));
    }

    private static double[,] BuildMotion()
    {
        var motion = new double[8, 8];

        for (var i = 0; i < 8; i++)
        {
            motion[i, i] = 1;

            if (i < 4)
            {
                motion[i, i + 4] = 1;
            }
        }

        return motion;
    }

    private static double[,] DiagonalOfSquares(params double[] std)
    {
        var matrix = new double[std.Length, std.Length];

        for (var i = 0; i < std.Length; i++)
        {
            matrix[i, i] = std[i] * std[i];
        }

        return matrix;
    }

    private static double[,] Multiply(double[,] a, double[,] b)
    {
        var (n, k, m) = (a.GetLength(0), a.GetLength(1), b.GetLength(1));
        var result = new double[n, m];

        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < m; j++)
            {
                for (var x = 0; x < k; x++)
                {
                    result[i, j] += a[i, x] * b[x, j];
                }
            }
        }

        return result;
    }

    private static double[,] Transpose(double[,] a)
    {
        var result = new double[a.GetLength(1), a.GetLength(0)];

        for (var i = 0; i < a.GetLength(0); i++)
        {
            for (var j = 0; j < a.GetLength(1); j++)
            {
                result[j, i] = a[i, j];
            }
        }

        return result;
    }

    private static double[,] Add(double[,] a, double[,] b) => Combine(a, b, 1);

    private static double[,] Subtract(double[,] a, double[,] b) => Combine(a, b, -1);

    private static double[,] Combine(double[,] a, double[,] b, double sign)
    {
        var result = new double[a.GetLength(0), a.GetLength(1)];

        for (var i = 0; i < a.GetLength(0); i++)
        {
            for (var j = 0; j < a.GetLength(1); j++)
            {
                result[i, j] = a[i, j] + (sign * b[i, j]);
            }
        }

        return result;
    }

    /// <summary>
    /// Gauss-Jordan with partial pivoting; the matrix is a 4x4 covariance, so it is invertible.
    /// </summary>
    private static double[,] Invert(double[,] a)
    {
        var n = a.GetLength(0);
        var work = new double[n, 2 * n];

        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < n; j++)
            {
                work[i, j] = a[i, j];
            }

            work[i, n + i] = 1;
        }

        for (var column = 0; column < n; column++)
        {
            var pivot = column;

            for (var row = column + 1; row < n; row++)
            {
                if (Math.Abs(work[row, column]) > Math.Abs(work[pivot, column]))
                {
                    pivot = row;
                }
            }

            for (var j = 0; j < 2 * n; j++)
            {
                (work[column, j], work[pivot, j]) = (work[pivot, j], work[column, j]);
            }

            var scale = work[column, column];

            for (var j = 0; j < 2 * n; j++)
            {
                work[column, j] /= scale;
            }

            for (var row = 0; row < n; row++)
            {
                if (row == column)
                {
                    continue;
                }

                var factor = work[row, column];

                for (var j = 0; j < 2 * n; j++)
                {
                    work[row, j] -= factor * work[column, j];
                }
            }
        }

        var result = new double[n, n];

        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < n; j++)
            {
                result[i, j] = work[i, n + j];
            }
        }

        return result;
    }
}
