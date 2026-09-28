using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using FFmpeg.AutoGen.Abstractions;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// Hand-built MISB ST 0601 packets with known values, and a way to get one into an MPEG-TS the
/// bundled ffmpeg can push.
///
/// The encoders here are written from ST 0601.8 Table 1 independently of the decoder, so a
/// scale that is wrong in one place fails against the other rather than agreeing with it.
/// </summary>
internal static unsafe class Misb
{
    /// <summary>The values <see cref="MinimumSet"/> carries. Chosen off-centre so a sign or hemisphere slip shows.</summary>
    public static class Known
    {
        public static readonly DateTimeOffset Timestamp = new(2024, 5, 6, 7, 8, 9, 123, TimeSpan.Zero);
        public const string MissionId = "MISSION-42";
        public const double PlatformHeading = 123.4;
        public const double PlatformPitch = -3.5;
        public const double PlatformRoll = 7.25;
        public const string PlatformDesignation = "MQ-9";
        public const string ImageSourceSensor = "EO";
        public const string ImageCoordinateSystem = "Geodetic WGS84";
        public const double SensorLatitude = 59.9139;
        public const double SensorLongitude = 10.7522;
        public const double SensorTrueAltitude = 1500.5;
        public const double SensorHorizontalFov = 12.5;
        public const double SensorVerticalFov = 9.4;
        public const double SensorRelativeAzimuth = 270.5;
        public const double SensorRelativeElevation = -45.25;
        public const double SensorRelativeRoll = 1.5;
        public const double SlantRange = 4321.5;
        public const double FrameCenterLatitude = -33.8688;
        public const double FrameCenterLongitude = 151.2093;
        public const double FrameCenterElevation = 120.0;
        public const string Classification = "SECRET";
        public const int Version = 14;
        public static readonly byte[] TailNumber = "N123"u8.ToArray();
    }

    /// <summary>A complete ST 0902 minimum set, plus one item outside it (tag 4, tail number).</summary>
    public static byte[] MinimumSet() => Packet(
        (2, U64((ulong)(Known.Timestamp - DateTimeOffset.UnixEpoch).Ticks / 10)),
        (3, Text(Known.MissionId)),
        (4, Known.TailNumber),
        (5, U16(Known.PlatformHeading, 0, 360)),
        (6, S16(Known.PlatformPitch, 20)),
        (7, S16(Known.PlatformRoll, 50)),
        (10, Text(Known.PlatformDesignation)),
        (11, Text(Known.ImageSourceSensor)),
        (12, Text(Known.ImageCoordinateSystem)),
        (13, S32(Known.SensorLatitude, 90)),
        (14, S32(Known.SensorLongitude, 180)),
        (15, U16(Known.SensorTrueAltitude, -900, 19000)),
        (16, U16(Known.SensorHorizontalFov, 0, 180)),
        (17, U16(Known.SensorVerticalFov, 0, 180)),
        (18, U32(Known.SensorRelativeAzimuth, 0, 360)),
        (19, S32(Known.SensorRelativeElevation, 180)),
        (20, U32(Known.SensorRelativeRoll, 0, 360)),
        (21, U32(Known.SlantRange, 0, 5_000_000)),
        (23, S32(Known.FrameCenterLatitude, 90)),
        (24, S32(Known.FrameCenterLongitude, 180)),
        (25, U16(Known.FrameCenterElevation, -900, 19000)),
        (48, Items((1, [0x04]), (2, [0x01]), (3, Text("//NOR")), (22, [0x00, 0x0C]))),
        (65, [(byte)Known.Version]));

    /// <summary>A local set under the ST 0601 key, checksum appended as the last item.</summary>
    public static byte[] Packet(params (int Tag, byte[] Value)[] items)
    {
        var body = Items(items);

        // The checksum item is tag 1, length 2, and its value covers everything before it.
        var payload = new byte[body.Length + 4];
        body.CopyTo(payload, 0);
        payload[body.Length] = 1;
        payload[body.Length + 1] = 2;

        var packet = new byte[16 + Length(payload.Length).Length + payload.Length];
        Misb0601.Key.CopyTo(packet);
        Length(payload.Length).CopyTo(packet, 16);
        payload.CopyTo(packet, packet.Length - payload.Length);

        BinaryPrimitives.WriteUInt16BigEndian(
            packet.AsSpan(packet.Length - 2),
            Misb0601.Checksum(packet.AsSpan(0, packet.Length - 2)));

        return packet;
    }

    public static byte[] Items(params (int Tag, byte[] Value)[] items)
    {
        var bytes = new List<byte>();

        foreach (var (tag, value) in items)
        {
            bytes.Add((byte)tag);
            bytes.AddRange(Length(value.Length));
            bytes.AddRange(value);
        }

        return [.. bytes];
    }

    private static byte[] Length(int length)
        => length < 128 ? [(byte)length] : [0x82, (byte)(length >> 8), (byte)length];

    private static byte[] Text(string text) => Encoding.ASCII.GetBytes(text);

    private static byte[] U64(ulong value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
        return bytes;
    }

    private static byte[] U16(double value, double min, double max)
    {
        var bytes = new byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, (ushort)Math.Round((value - min) / (max - min) * ushort.MaxValue));
        return bytes;
    }

    private static byte[] U32(double value, double min, double max)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)Math.Round((value - min) / (max - min) * uint.MaxValue));
        return bytes;
    }

    private static byte[] S16(double value, double range)
    {
        var bytes = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(bytes, (short)Math.Round(value / range * short.MaxValue));
        return bytes;
    }

    private static byte[] S32(double value, double range)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, (int)Math.Round(value / range * int.MaxValue));
        return bytes;
    }

    /// <summary>
    /// Copies a video-only MPEG-TS into a new one that also carries the packet as a KLV stream at
    /// a fixed rate, in a private PES with a presentation timestamp.
    ///
    /// libav rather than the ffmpeg command line, because the command line cannot tag raw data as
    /// SMPTE KLV: <c>-f data</c> yields <c>bin_data</c>, which the mpegts muxer writes without the
    /// KLVA registration. The muxer here is the same libavformat, writing the same registration a
    /// real encoder does, and ffmpeg then pushes the file with <c>-c copy</c> exactly as it would
    /// any other KLV-carrying transport.
    ///
    /// Not written as stream type 0x15 (<c>AV_PROFILE_KLVA_SYNC</c>): libav's demuxer strips the
    /// five-byte ST 1402 metadata AU cell header from such a stream, as a real encoder's output
    /// requires, but libav's muxer does not write one, so its own round trip loses the first five
    /// bytes of every packet. That reasoning now governs production code as well - a synthetic
    /// track leaves <c>codecpar-&gt;profile</c> unset for it - and is stated where that happens, in
    /// <c>StreamLayout.From</c>'s synthetic-track overload.
    /// </summary>
    public static void WriteTransportStream(string videoPath, string outputPath, byte[] klv, double intervalSeconds)
    {
        FfmpegLibrary.EnsureLoaded();

        AVFormatContext* input = null;
        AVFormatContext* output = null;
        var packet = ffmpeg.av_packet_alloc();

        try
        {
            Check(ffmpeg.avformat_open_input(&input, videoPath, null, null), "open input");
            Check(ffmpeg.avformat_find_stream_info(input, null), "read input");
            Check(ffmpeg.avformat_alloc_output_context2(&output, null, "mpegts", outputPath), "allocate output");

            var inVideo = input->streams[0];
            var video = ffmpeg.avformat_new_stream(output, null);
            Check(ffmpeg.avcodec_parameters_copy(video->codecpar, inVideo->codecpar), "copy video parameters");
            video->codecpar->codec_tag = 0;
            video->time_base = inVideo->time_base;

            var data = ffmpeg.avformat_new_stream(output, null);
            data->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_DATA;
            data->codecpar->codec_id = AVCodecID.AV_CODEC_ID_SMPTE_KLV;
            data->time_base = new AVRational { num = 1, den = 90000 };

            Check(ffmpeg.avio_open(&output->pb, outputPath, ffmpeg.AVIO_FLAG_WRITE), "open output");
            Check(ffmpeg.avformat_write_header(output, null), "write header");

            var nextKlv = 0L;
            var interval = (long)(intervalSeconds * data->time_base.den);

            while (ffmpeg.av_read_frame(input, packet) >= 0)
            {
                if (packet->stream_index != 0)
                {
                    ffmpeg.av_packet_unref(packet);
                    continue;
                }

                var at = ffmpeg.av_rescale_q(packet->pts, inVideo->time_base, data->time_base);

                ffmpeg.av_packet_rescale_ts(packet, inVideo->time_base, video->time_base);
                Check(ffmpeg.av_interleaved_write_frame(output, packet), "write video");

                while (at >= nextKlv)
                {
                    Check(ffmpeg.av_new_packet(packet, klv.Length), "allocate klv");
                    Marshal.Copy(klv, 0, (IntPtr)packet->data, klv.Length);
                    packet->stream_index = data->index;
                    packet->pts = packet->dts = nextKlv;
                    packet->flags = ffmpeg.AV_PKT_FLAG_KEY;
                    Check(ffmpeg.av_interleaved_write_frame(output, packet), "write klv");

                    nextKlv += interval;
                }
            }

            Check(ffmpeg.av_write_trailer(output), "write trailer");
        }
        finally
        {
            ffmpeg.av_packet_free(&packet);

            if (output is not null)
            {
                ffmpeg.avio_closep(&output->pb);
                ffmpeg.avformat_free_context(output);
            }

            ffmpeg.avformat_close_input(&input);
        }
    }

    /// <summary>
    /// Every KLV packet in a transport stream, paired with its presentation timestamp or null when
    /// the carriage is asynchronous. Reading it through libav is what makes the answer meaningful:
    /// the demuxer strips the ST 1402 metadata AU cell header, so this is byte for byte what the
    /// KLV extractor hands the decoder in production.
    /// </summary>
    public static List<(long? Pts, byte[] Data)> ReadKlv(string path)
    {
        FfmpegLibrary.EnsureLoaded();

        AVFormatContext* input = null;
        var packet = ffmpeg.av_packet_alloc();
        var klv = new List<(long?, byte[])>();

        try
        {
            Check(ffmpeg.avformat_open_input(&input, path, null, null), "open input");
            Check(ffmpeg.avformat_find_stream_info(input, null), "read input");

            var index = -1;

            for (var i = 0; i < input->nb_streams; i++)
            {
                if (input->streams[i]->codecpar->codec_id == AVCodecID.AV_CODEC_ID_SMPTE_KLV)
                {
                    index = i;
                }
            }

            if (index < 0)
            {
                return klv;
            }

            while (ffmpeg.av_read_frame(input, packet) >= 0)
            {
                if (packet->stream_index == index)
                {
                    var data = new byte[packet->size];
                    Marshal.Copy((IntPtr)packet->data, data, 0, packet->size);
                    klv.Add((packet->pts == ffmpeg.AV_NOPTS_VALUE ? null : packet->pts, data));
                }

                ffmpeg.av_packet_unref(packet);
            }

            return klv;
        }
        finally
        {
            ffmpeg.av_packet_free(&packet);
            ffmpeg.avformat_close_input(&input);
        }
    }

    private static void Check(int result, string step)
    {
        if (result < 0)
        {
            throw new InvalidOperationException($"libav could not {step}: {result}");
        }
    }
}
