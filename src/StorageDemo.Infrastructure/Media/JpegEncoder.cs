using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;

namespace StorageDemo.Infrastructure.Media;

/// <summary>Turns one decoded picture into a JPEG.</summary>
public static unsafe class JpegEncoder
{
    /// <summary>swscale's flag for bilinear scaling.</summary>
    private const int Bilinear = 2;

    /// <param name="maxEdge">Longest edge of the result.</param>
    /// <param name="quality">libav's scale, where 1 is best and 31 is worst.</param>
    public static byte[]? Encode(AVFrame* source, int maxEdge, int quality)
    {
        if (source->width <= 0 || source->height <= 0)
        {
            return null;
        }

        var encoder = ffmpeg.avcodec_find_encoder(AVCodecID.AV_CODEC_ID_MJPEG);
        if (encoder is null)
        {
            return null;
        }

        var target = PreferredPixelFormat(encoder);

        var (width, height) = Fit(source->width, source->height, maxEdge);

        AVFrame* scaled = null;
        SwsContext* scaler = null;
        AVCodecContext* codec = null;
        AVPacket* packet = null;

        try
        {
            scaled = ffmpeg.av_frame_alloc();
            if (scaled is null)
            {
                return null;
            }

            scaled->format = (int)target;
            scaled->width = width;
            scaled->height = height;

            if (ffmpeg.av_frame_get_buffer(scaled, 32) < 0)
            {
                return null;
            }

            scaler = ffmpeg.sws_getContext(
                source->width,
                source->height,
                (AVPixelFormat)source->format,
                width,
                height,
                target,
                Bilinear,
                null,
                null,
                null);

            if (scaler is null)
            {
                return null;
            }

            ffmpeg.sws_scale(
                scaler,
                source->data.ToArray(),
                source->linesize.ToArray(),
                0,
                source->height,
                scaled->data.ToArray(),
                scaled->linesize.ToArray());

            codec = ffmpeg.avcodec_alloc_context3(encoder);
            if (codec is null)
            {
                return null;
            }

            codec->width = width;
            codec->height = height;
            codec->pix_fmt = target;

            // A single still, so the time base is a formality the encoder still requires.
            codec->time_base = new AVRational { num = 1, den = 1 };

            // Fixed quality rather than a bitrate target: one frame has no bitrate.
            codec->flags |= ffmpeg.AV_CODEC_FLAG_QSCALE;
            codec->global_quality = ffmpeg.FF_QP2LAMBDA * quality;

            if (ffmpeg.avcodec_open2(codec, encoder, null) < 0)
            {
                return null;
            }

            scaled->pts = 0;
            scaled->quality = codec->global_quality;

            if (ffmpeg.avcodec_send_frame(codec, scaled) < 0)
            {
                return null;
            }

            // Tell the encoder that was the only frame, so it emits the picture now.
            ffmpeg.avcodec_send_frame(codec, null);

            packet = ffmpeg.av_packet_alloc();
            if (packet is null || ffmpeg.avcodec_receive_packet(codec, packet) < 0 || packet->size <= 0)
            {
                return null;
            }

            var bytes = new byte[packet->size];
            Marshal.Copy((IntPtr)packet->data, bytes, 0, packet->size);

            return bytes;
        }
        finally
        {
            if (packet is not null)
            {
                ffmpeg.av_packet_free(&packet);
            }

            if (codec is not null)
            {
                ffmpeg.avcodec_free_context(&codec);
            }

            if (scaler is not null)
            {
                ffmpeg.sws_freeContext(scaler);
            }

            if (scaled is not null)
            {
                ffmpeg.av_frame_free(&scaled);
            }
        }
    }

    private static AVPixelFormat PreferredPixelFormat(AVCodec* encoder)
    {
        void* configs = null;
        var count = 0;

        var queried = ffmpeg.avcodec_get_supported_config(
            null,
            encoder,
            AVCodecConfig.AV_CODEC_CONFIG_PIX_FORMAT,
            0,
            &configs,
            &count);

        return queried >= 0 && configs is not null && count > 0
            ? ((AVPixelFormat*)configs)[0]
            : AVPixelFormat.AV_PIX_FMT_YUVJ420P;
    }

    /// <summary>Fits inside a square box without distorting, and keeps both sides even.</summary>
    private static (int Width, int Height) Fit(int width, int height, int box)
    {
        var scale = box <= 0
            ? 1.0
            : Math.Min(1.0, Math.Min((double)box / width, (double)box / height));

        return (Even((int)Math.Round(width * scale)), Even((int)Math.Round(height * scale)));
    }

    private static int Even(int value) => Math.Max(2, value % 2 == 0 ? value : value - 1);
}
