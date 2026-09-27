using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>One open video decoder, and the packet and frame it works through.</summary>
public sealed unsafe class VideoDecoder : IDisposable
{
    private AVCodecContext* _codec;
    private AVFrame* _frame;
    private AVPacket* _packet;

    private VideoDecoder(AVCodecContext* codec, AVFrame* frame, AVPacket* packet)
    {
        _codec = codec;
        _frame = frame;
        _packet = packet;
    }

    /// <summary>Null when the stream carries no picture, or none this build can decode.</summary>
    public static VideoDecoder? Open(StreamLayout layout, string name, ILogger logger)
    {
        if (layout.VideoIndex < 0)
        {
            return null;
        }

        var parameters = layout.Parameters(layout.VideoIndex);
        var decoder = ffmpeg.avcodec_find_decoder(parameters->codec_id);

        if (decoder is null)
        {
            logger.LogWarning(
                "No decoder for {Codec} on '{Name}', so it will have no preview",
                ffmpeg.avcodec_get_name(parameters->codec_id),
                name);

            return null;
        }

        var codec = ffmpeg.avcodec_alloc_context3(decoder);
        AVFrame* frame = null;
        AVPacket* packet = null;

        if (codec is not null
            && ffmpeg.avcodec_parameters_to_context(codec, parameters) >= 0
            && ffmpeg.avcodec_open2(codec, decoder, null) >= 0)
        {
            frame = ffmpeg.av_frame_alloc();
            packet = ffmpeg.av_packet_alloc();

            if (frame is not null && packet is not null)
            {
                return new VideoDecoder(codec, frame, packet);
            }
        }

        if (packet is not null)
        {
            ffmpeg.av_packet_free(&packet);
        }

        if (frame is not null)
        {
            ffmpeg.av_frame_free(&frame);
        }

        if (codec is not null)
        {
            ffmpeg.avcodec_free_context(&codec);
        }

        return null;
    }

    /// <summary>
    /// Decodes one packet and hands every picture it produced to <paramref name="onFrame"/>.
    /// </summary>
    public void Decode(MediaPacket media, Action<IntPtr> onFrame)
    {
        if (_codec is null || ffmpeg.av_new_packet(_packet, media.Data.Length) < 0)
        {
            return;
        }

        try
        {
            Marshal.Copy(media.Data, 0, (IntPtr)_packet->data, media.Data.Length);

            _packet->pts = media.Pts;
            _packet->dts = media.Dts;
            _packet->flags = media.IsKeyframe ? ffmpeg.AV_PKT_FLAG_KEY : 0;

            if (ffmpeg.avcodec_send_packet(_codec, _packet) < 0)
            {
                return;
            }

            while (ffmpeg.avcodec_receive_frame(_codec, _frame) == 0)
            {
                onFrame((IntPtr)_frame);
                ffmpeg.av_frame_unref(_frame);
            }
        }
        finally
        {
            ffmpeg.av_packet_unref(_packet);
        }
    }

    public void Dispose()
    {
        if (_packet is not null)
        {
            var packet = _packet;
            _packet = null;
            ffmpeg.av_packet_free(&packet);
        }

        if (_frame is not null)
        {
            var frame = _frame;
            _frame = null;
            ffmpeg.av_frame_free(&frame);
        }

        if (_codec is not null)
        {
            var codec = _codec;
            _codec = null;
            ffmpeg.avcodec_free_context(&codec);
        }
    }
}
