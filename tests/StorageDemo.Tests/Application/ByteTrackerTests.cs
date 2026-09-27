using StorageDemo.Core.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Application;

/// <summary>
/// The tracker against the behaviours ByteTrack (Zhang et al., ECCV 2022) claims, arranged so that
/// a tracker which merely runs fails them: the same object under a new id, two ids swapped at a
/// crossing, a lost object never recovered.
/// </summary>
public sealed class ByteTrackerTests
{
    private const int Width = 1920;
    private const int Height = 1080;
    private static readonly DateTimeOffset Start = new(2024, 5, 6, 7, 8, 9, TimeSpan.Zero);

    private static DateTimeOffset At(int frame) => Start.AddMilliseconds(frame * 40.0);

    private static VmtiDetection Box(int x, int y, int confidence = 90, string? cls = "Vehicle")
        => new(1, x, y, x + 79, y + 79, confidence, cls);

    private static int Centre(Track track) => (track.Box.Left + track.Box.Right) / 2;

    [Fact]
    public void A_linear_mover_keeps_one_id_with_detections_only_every_fifth_frame()
    {
        var tracker = new ByteTracker(Width, Height);
        var ids = new HashSet<int>();

        for (var frame = 1; frame <= 60; frame++)
        {
            var x = 100 + (6 * frame);

            if (frame % 5 == 1)
            {
                tracker.Update(At(frame), [Box(x, 500)]);
            }
            else
            {
                tracker.Predict(At(frame));
            }

            var track = Assert.Single(tracker.Tracks);
            ids.Add(track.Id);

            // Once the velocity has been estimated from a few detections, the coasted box should
            // sit on the object, not where the object was last seen.
            if (frame > 30)
            {
                Assert.InRange(Centre(track), x + 40 - 6, x + 40 + 6);
            }
        }

        var only = Assert.Single(tracker.Tracks);

        Assert.Single(ids);
        Assert.Equal("Vehicle", only.OntologyClass);
        Assert.Equal(At(56), only.LastSeen); // the last detection frame, not the last predicted one
        Assert.Equal(At(1), only.Started);
        Assert.Equal(12, only.History.Count);
        Assert.All(only.History, h => Assert.Equal(only.Id, h.Box.Id));

        var packet = Misb0903.Encode(new VmtiFrame(At(60), Width, Height, "EO", [only.Box]));
        var vtracker = Vmti.Nested(Assert.Single(Vmti.Decode(packet).Targets).Items[104]);

        Assert.Equal(only.Uuid, Vmti.Uuid(vtracker[1]));
        Assert.Equal((ulong)VmtiTrackStatus.Active, Vmti.Integer(vtracker[2]));
        Assert.Equal(ByteTracker.Algorithm, Vmti.Text(vtracker[6]));
    }

    [Fact]
    public void Two_objects_crossing_keep_their_ids()
    {
        // Same size, same row, same class, opposite directions, detected every fifth frame.
        var tracker = new ByteTracker(Width, Height);
        int? a = null;
        int? b = null;

        for (var frame = 1; frame <= 100; frame++)
        {
            var ax = 100 + (6 * frame);
            var bx = 705 - (6 * frame);

            if (frame % 5 == 1)
            {
                tracker.Update(At(frame), [Box(ax, 500), Box(bx, 500)]);
            }
            else
            {
                tracker.Predict(At(frame));
            }

            if (frame == 6)
            {
                a = tracker.Tracks.Single(t => Centre(t) < 400).Id;
                b = tracker.Tracks.Single(t => Centre(t) > 400).Id;
            }
        }

        var tracks = tracker.Tracks;

        Assert.Equal(2, tracks.Count);
        Assert.InRange(Centre(tracks.Single(t => t.Id == a)), 700 + 40 - 10, 700 + 40 + 10);
        Assert.InRange(Centre(tracks.Single(t => t.Id == b)), 105 + 40 - 10, 105 + 40 + 10);
    }

    [Theory]
    [InlineData(10, true)]
    [InlineData(40, false)]
    public void An_occluded_object_is_recovered_within_the_buffer_and_renamed_beyond_it(int gap, bool sameId)
    {
        // Detected every frame, then absent for `gap` frames while still moving, then back at its
        // true position.
        var tracker = new ByteTracker(Width, Height);
        var before = 0;
        var frame = 1;

        for (; frame <= 20; frame++)
        {
            tracker.Update(At(frame), [Box(100 + (4 * frame), 300)]);
            before = Assert.Single(tracker.Tracks).Id;
        }

        for (; frame <= 20 + gap; frame++)
        {
            tracker.Update(At(frame), []);
            Assert.Empty(tracker.Tracks); // lost tracks are not reported (paper section 3)
        }

        for (; frame <= 20 + gap + 5; frame++)
        {
            tracker.Update(At(frame), [Box(100 + (4 * frame), 300)]);
        }

        var after = Assert.Single(tracker.Tracks);

        Assert.Equal(sameId, after.Id == before);

        if (sameId)
        {
            // Five observations after the gap, and the one before them is frame 20: the gap left no
            // observations behind, only predictions.
            Assert.Equal(At(20), after.History[^6].Timestamp);
        }
    }

    [Fact]
    public void A_low_confidence_detection_keeps_a_track_alive_but_does_not_start_one()
    {
        var tracker = new ByteTracker(Width, Height);

        // Below tau: the second association has nothing to attach it to.
        for (var frame = 1; frame <= 10; frame++)
        {
            tracker.Update(At(frame), [Box(100, 100, confidence: 30), Box(600, 600, confidence: 65)]);
            Assert.Empty(tracker.Tracks);
        }

        for (var frame = 11; frame <= 15; frame++)
        {
            tracker.Update(At(frame), [Box(100 + (4 * frame), 100)]);
        }

        var id = Assert.Single(tracker.Tracks).Id;

        // The second association: a weak box recovers the track that a strong-only tracker would
        // have marked lost.
        for (var frame = 16; frame <= 40; frame++)
        {
            tracker.Update(At(frame), [Box(100 + (4 * frame), 100, confidence: 30)]);

            var track = Assert.Single(tracker.Tracks);

            Assert.Equal(id, track.Id);
            Assert.Equal(30, track.ConfidencePercent);
        }

        // Below the 0.1 floor (byte_tracker.py 178) a box is background: the track is lost.
        tracker.Update(At(41), [Box(100 + (4 * 41), 100, confidence: 5)]);

        Assert.Empty(tracker.Tracks);
    }

    [Fact]
    public void The_kalman_filter_holds_a_static_box_and_moves_a_moving_one_by_its_velocity()
    {
        var still = new KalmanFilter(100, 200, 1, 50);
        still.Predict(lost: false);

        Assert.Equal([100, 200, 1, 50], still.Mean[..4]);

        var moving = new KalmanFilter(100, 200, 1, 50);
        moving.Mean[4] = 3;
        moving.Mean[5] = -2;
        moving.Mean[7] = 4;
        moving.Predict(lost: false);

        Assert.Equal([103, 198, 1, 54], moving.Mean[..4]);

        // STrack.predict zeroes the height velocity of a track that is not being seen.
        moving.Predict(lost: true);

        Assert.Equal([106, 196, 1, 54], moving.Mean[..4]);

        // Corrections from displaced measurements teach the filter a velocity it was not given.
        var learning = new KalmanFilter(100, 200, 1, 50);

        for (var i = 1; i <= 10; i++)
        {
            learning.Predict(lost: false);
            learning.Update(100 + (5 * i), 200, 1, 50);
        }

        Assert.InRange(learning.Mean[4], 4, 6);
    }
}
