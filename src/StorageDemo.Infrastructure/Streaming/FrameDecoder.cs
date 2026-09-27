using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>How often a frame subscriber needs a picture, as frames per second.</summary>
public readonly record struct DecodeRate(double FramesPerSecond)
{
    public static readonly DecodeRate Keyframes = new(0);

    public static readonly DecodeRate Everything = new(double.PositiveInfinity);

    public static DecodeRate PerSecond(double framesPerSecond) => new(Math.Max(0, framesPerSecond));

    public bool IsKeyframes => FramesPerSecond <= 0;

    public bool IsEverything => double.IsPositiveInfinity(FramesPerSecond);
}

/// <summary>Receives one decoded picture, as a pointer to libav's own frame.</summary>
public delegate void FrameHandler(IntPtr frame);

/// <summary>The frame tier: one decoder, itself a packet subscriber, republishing pictures.</summary>
public sealed class FrameDecoder(StreamHub hub, ILogger logger) : IDisposable
{
    private readonly Lock _gate = new();

    private Subscriber[] _subscribers = [];

    /// <summary>The rate actually decoded at, which is the highest anybody asked for.</summary>
    public DecodeRate Rate => _subscribers.Length == 0
        ? DecodeRate.Keyframes
        : new DecodeRate(_subscribers.Max(subscriber => subscriber.Rate.FramesPerSecond));

    public IDisposable Subscribe(DecodeRate rate, FrameHandler handler)
    {
        var subscriber = new Subscriber(rate, handler);

        lock (_gate)
        {
            _subscribers = [.. _subscribers, subscriber];
        }

        return new Subscription(() =>
        {
            lock (_gate)
            {
                _subscribers = [.. _subscribers.Where(existing => !ReferenceEquals(existing, subscriber))];
            }
        });
    }

    /// <summary>Runs until the hub closes or the token is cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested && !hub.Closed)
            {
                var layout = hub.Layout;

                if (layout is null)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
                    continue;
                }

                await DecodeAsync(layout, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "The decoder for '{Name}' stopped", hub.Name);
        }
    }

    private async Task DecodeAsync(StreamLayout layout, CancellationToken cancellationToken)
    {
        // Small and skip-to-live: a decoder that falls behind wants the newest picture, not the
        // backlog.
        using var subscription = hub.Subscribe(
            capacity: 240,
            OverflowPolicy.SkipToLive,
            streamIndexes: layout.VideoIndex >= 0 ? [layout.VideoIndex] : [],
            preroll: 0);

        using var decoder = VideoDecoder.Open(layout, hub.Name, logger);

        if (decoder is null)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            return;
        }

        var secondsPerTick = ffmpeg.av_q2d(layout.TimeBase(layout.VideoIndex));

        // The keyframe interval is measured off the stream rather than configured: it is the
        // sender's, and it decides whether keyframes alone satisfy the rate asked for.
        long? lastKeyframePts = null;
        var keyframeInterval = 0d;

        var awaitingKeyframe = false;

        await foreach (var media in subscription.Packets.ReadAllAsync(cancellationToken))
        {
            if (!ReferenceEquals(hub.Layout, layout))
            {
                // The encoder came back as something else; this decoder is for the old shape.
                return;
            }

            var rate = Rate;

            if (media.IsKeyframe)
            {
                if (lastKeyframePts is { } previous && media.Pts > previous)
                {
                    keyframeInterval = (media.Pts - previous) * secondsPerTick;
                }

                lastKeyframePts = media.Pts;
                awaitingKeyframe = false;
            }
            else if (rate.IsKeyframes || awaitingKeyframe || (!rate.IsEverything && KeyframesSuffice(rate, keyframeInterval)))
            {
                awaitingKeyframe = true;
                continue;
            }

            decoder.Decode(media, frame => Publish(frame, secondsPerTick));
        }
    }

    /// <summary>Whether keyframes arrive at least as often as the rate asks for.</summary>
    private static bool KeyframesSuffice(DecodeRate rate, double keyframeInterval)
        => keyframeInterval <= 0 || rate.FramesPerSecond * keyframeInterval <= 1.0001;

    private unsafe void Publish(IntPtr frame, double secondsPerTick)
    {
        var pts = ((AVFrame*)frame)->pts;
        var seconds = pts == ffmpeg.AV_NOPTS_VALUE ? double.NaN : pts * secondsPerTick;

        foreach (var subscriber in _subscribers)
        {
            if (subscriber.Due(seconds))
            {
                subscriber.Handler(frame);
            }
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _subscribers = [];
        }
    }

    private sealed class Subscriber(DecodeRate rate, FrameHandler handler)
    {
        private double _nextDue = double.NegativeInfinity;

        public DecodeRate Rate { get; } = rate;

        public FrameHandler Handler { get; } = handler;

        /// <summary>
        /// A keyframe subscriber gets every picture the decoder produced: when something else has
        /// raised the rate it simply sees more of them, which is free, and filtering back down
        /// would be work for nobody's benefit.
        /// </summary>
        public bool Due(double seconds)
        {
            if (Rate.IsKeyframes || Rate.IsEverything || double.IsNaN(seconds))
            {
                return true;
            }

            var interval = 1 / Rate.FramesPerSecond;

            // The clock went backwards, which a reconnect does: start again from here rather than
            // waiting for the stream to reach a time it may never see again.
            if (seconds < _nextDue - interval)
            {
                _nextDue = seconds;
            }

            if (seconds < _nextDue)
            {
                return false;
            }

            _nextDue = seconds + interval;

            return true;
        }
    }

    private sealed class Subscription(Action release) : IDisposable
    {
        public void Dispose() => release();
    }
}
