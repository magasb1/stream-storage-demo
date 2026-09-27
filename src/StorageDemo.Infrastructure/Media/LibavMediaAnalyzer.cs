using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Documents;

namespace StorageDemo.Infrastructure.Media;

/// <summary>
/// Probes media and renders a thumbnail by calling libav in this process, through the FFmpeg.AutoGen
/// bindings, against the libraries bundled with the application.
///
/// One open of the file answers both questions. The previous implementation launched ffprobe and
/// then ffmpeg, which meant two process starts and two reads of the same bytes.
///
/// Everything unmanaged is allocated and freed in the same method, in a finally, and no pointer
/// outlives the call. That discipline is the whole reason this file is as verbose as it is.
/// </summary>
public sealed class LibavMediaAnalyzer(
    IOptions<MediaOptions> options,
    ILogger<LibavMediaAnalyzer> logger) : IMediaAnalyzer
{
    private readonly MediaOptions _options = options.Value;

    /// <summary>
    /// The valve on the blocking section, <see cref="MediaOptions.MaxConcurrentDecodes"/> wide.
    ///
    /// Here rather than at the snapshot route, because this is the one place all of the cost passes
    /// through: a bound on the route would leave an upload's thumbnail outside it and would have to
    /// be repeated by whatever calls this next. It bounds the process only because this class is
    /// registered as a singleton; a transient one would hand every call its own gate and every gate
    /// would be open.
    ///
    /// Callers queue rather than being refused, and the queue is not bounded here: a snapshot that
    /// arrives late is still the evidence somebody asked for, and a refused one is not. Waiting
    /// costs a caller no thread, and how many callers there can be is a question for the surface in
    /// front of this rather than for a decoder.
    /// </summary>
    private readonly SemaphoreSlim _decodes = new(
        options.Value.MaxConcurrentDecodes,
        options.Value.MaxConcurrentDecodes);

    public bool CanAnalyze(string? contentType, string fileName)
    {
        if (!_options.Enabled)
        {
            return false;
        }

        var type = string.IsNullOrWhiteSpace(contentType) || contentType == ContentTypes.Unknown
            ? ContentTypes.Guess(fileName)
            : contentType;

        // SVG is markup, not a raster image, and libav cannot decode it.
        if (type.Contains("svg", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return type.StartsWith("image/", StringComparison.OrdinalIgnoreCase)
            || type.StartsWith("video/", StringComparison.OrdinalIgnoreCase)
            || type.StartsWith("audio/", StringComparison.OrdinalIgnoreCase);
    }

    public Task<MediaAnalysis> AnalyzeAsync(
        Stream content,
        string fileName,
        string? contentType,
        CancellationToken cancellationToken = default)
        => OnAFileAsync(content, fileName, Analyze, cancellationToken);

    public Task<byte[]?> LatestFrameAsync(
        Stream content,
        string fileName,
        CancellationToken cancellationToken = default)
        => OnAFileAsync(content, fileName, LatestFrame, cancellationToken);

    /// <summary>
    /// Spills the content to a temp file and runs libav over it off the request thread, with at
    /// most <see cref="MediaOptions.MaxConcurrentDecodes"/> calls doing either at once.
    ///
    /// The file is what makes the poster frame possible, rather than an optimisation to remove
    /// later: an upload is read back out of storage as a forward-only stream, and a seek into an
    /// input libav cannot seek fails instead of moving, which turns every poster frame into frame
    /// zero while reporting success. The snapshot path is the exception - it muxes into memory and
    /// then reads straight through to the end, and libav demultiplexes an unseekable MPEG-TS
    /// perfectly well, as every live stream in this service already does - so a custom AVIO could
    /// spare that one caller the write and the unlink. It would not remove the file from here.
    /// </summary>
    private async Task<T> OnAFileAsync<T>(
        Stream content,
        string fileName,
        Func<string, T> work,
        CancellationToken cancellationToken)
    {
        FfmpegLibrary.EnsureLoaded();

        var workingDirectory = Path.Combine(Path.GetTempPath(), "storagedemo-media");
        Directory.CreateDirectory(workingDirectory);

        var sourcePath = Path.Combine(
            workingDirectory,
            $"{Guid.NewGuid():N}{Path.GetExtension(fileName)}");

        // Held across the spill as well as the decode, so a burst is bounded in temp files and in
        // disk traffic and not only in threads: a call still waiting keeps its content wherever the
        // caller already had it, rather than in a second copy under the temp directory.
        //
        // That costs nothing today because no caller here is slow to read: a snapshot muxes into
        // memory first, and an upload is read back out of storage by the analysis worker one item at
        // a time. Widening that worker would put the read of an S3 object inside the permit, and a
        // slow object would then hold a decode slot for as long as it took to arrive.
        await _decodes.WaitAsync(cancellationToken);

        try
        {
            await using (var temp = File.Create(sourcePath))
            {
                await content.CopyToAsync(temp, cancellationToken);
            }

            return await Task.Run(() => work(sourcePath), cancellationToken);
        }
        finally
        {
            _decodes.Release();

            try
            {
                File.Delete(sourcePath);
            }
            catch (IOException)
            {
                // A leftover temp file is not worth failing an upload over.
            }
        }
    }

    /// <summary>
    /// The last picture in the media, at source resolution.
    ///
    /// Distinct from the thumbnail above, which is the picture a fixed number of seconds in. A
    /// preview and a snapshot ask "what does this look like now"; a poster frame asks "what does
    /// this piece of media look like". Answering both with one verb is what let a live preview
    /// serve a fixed frame for minutes while every component reported success.
    /// </summary>
    private unsafe byte[]? LatestFrame(string path)
    {
        AVFormatContext* format = null;

        if (ffmpeg.avformat_open_input(&format, path, null, null) < 0)
        {
            return null;
        }

        AVCodecContext* codec = null;
        AVFrame* frame = null;
        AVFrame* latest = null;
        AVPacket* packet = null;

        try
        {
            if (ffmpeg.avformat_find_stream_info(format, null) < 0)
            {
                return null;
            }

            AVCodec* decoder = null;
            var streamIndex = ffmpeg.av_find_best_stream(
                format,
                AVMediaType.AVMEDIA_TYPE_VIDEO,
                -1,
                -1,
                &decoder,
                0);

            if (streamIndex < 0 || decoder is null)
            {
                return null;
            }

            codec = ffmpeg.avcodec_alloc_context3(decoder);
            if (codec is null
                || ffmpeg.avcodec_parameters_to_context(codec, format->streams[streamIndex]->codecpar) < 0
                || ffmpeg.avcodec_open2(codec, decoder, null) < 0)
            {
                return null;
            }

            frame = ffmpeg.av_frame_alloc();
            latest = ffmpeg.av_frame_alloc();
            packet = ffmpeg.av_packet_alloc();

            if (frame is null || latest is null || packet is null)
            {
                return null;
            }

            while (ffmpeg.av_read_frame(format, packet) >= 0)
            {
                try
                {
                    if (packet->stream_index != streamIndex
                        || ffmpeg.avcodec_send_packet(codec, packet) < 0)
                    {
                        continue;
                    }

                    KeepFrames(codec, frame, latest);
                }
                finally
                {
                    ffmpeg.av_packet_unref(packet);
                }
            }

            // Whatever the decoder is still holding at end of input is the newest picture there is.
            ffmpeg.avcodec_send_packet(codec, null);
            KeepFrames(codec, frame, latest);

            return latest->width > 0
                ? JpegEncoder.Encode(latest, maxEdge: 0, _options.SnapshotQuality)
                : null;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Reading the latest frame failed");
            return null;
        }
        finally
        {
            if (packet is not null)
            {
                ffmpeg.av_packet_free(&packet);
            }

            if (latest is not null)
            {
                ffmpeg.av_frame_free(&latest);
            }

            if (frame is not null)
            {
                ffmpeg.av_frame_free(&frame);
            }

            if (codec is not null)
            {
                ffmpeg.avcodec_free_context(&codec);
            }

            ffmpeg.avformat_close_input(&format);
        }
    }

    /// <summary>Drains the decoder, leaving the newest picture in <paramref name="latest"/>.</summary>
    private static unsafe void KeepFrames(AVCodecContext* codec, AVFrame* frame, AVFrame* latest)
    {
        while (ffmpeg.avcodec_receive_frame(codec, frame) == 0)
        {
            // Moved rather than copied: this runs once per decoded frame of a whole segment.
            ffmpeg.av_frame_unref(latest);
            ffmpeg.av_frame_move_ref(latest, frame);
        }
    }

    private unsafe MediaAnalysis Analyze(string path)
    {
        AVFormatContext* format = null;

        var opened = ffmpeg.avformat_open_input(&format, path, null, null);
        if (opened < 0)
        {
            logger.LogDebug("libav could not open {Path}: {Error}", path, Describe(opened));
            return new MediaAnalysis(new Dictionary<string, string>(), null);
        }

        try
        {
            if (ffmpeg.avformat_find_stream_info(format, null) < 0)
            {
                logger.LogDebug("libav found no stream information in {Path}", path);
                return new MediaAnalysis(new Dictionary<string, string>(), null);
            }

            var metadata = DescribeFormat(format);
            var thumbnail = TryRenderThumbnail(format);

            return new MediaAnalysis(metadata, thumbnail);
        }
        finally
        {
            ffmpeg.avformat_close_input(&format);
        }
    }

    private static unsafe Dictionary<string, string> DescribeFormat(AVFormatContext* format)
    {
        var metadata = new Dictionary<string, string>();

        MediaMetadataFormat.Add(
            metadata,
            "Container",
            ToString(format->iformat->long_name) ?? ToString(format->iformat->name));

        if (format->duration != ffmpeg.AV_NOPTS_VALUE && format->duration > 0)
        {
            MediaMetadataFormat.Add(
                metadata,
                "Duration",
                MediaMetadataFormat.Duration(format->duration / (double)ffmpeg.AV_TIME_BASE));
        }

        if (format->bit_rate > 0)
        {
            MediaMetadataFormat.Add(
                metadata,
                "Overall bitrate",
                MediaMetadataFormat.Bitrate(format->bit_rate));
        }

        AddTags(metadata, format->metadata, prefix: string.Empty);

        var counts = new Dictionary<string, int>();

        for (var index = 0; index < format->nb_streams; index++)
        {
            var stream = format->streams[index];
            var type = ffmpeg.av_get_media_type_string(stream->codecpar->codec_type) ?? "data";

            counts[type] = counts.GetValueOrDefault(type) + 1;

            DescribeStream(metadata, stream, type, MediaMetadataFormat.StreamLabel(type, counts[type]));
        }

        return metadata;
    }

    private static unsafe void DescribeStream(
        Dictionary<string, string> metadata,
        AVStream* stream,
        string type,
        string label)
    {
        var parameters = stream->codecpar;
        var descriptor = ffmpeg.avcodec_descriptor_get(parameters->codec_id);

        MediaMetadataFormat.Add(
            metadata,
            $"{label} codec",
            descriptor is not null
                ? ToString(descriptor->long_name) ?? ToString(descriptor->name)
                : ffmpeg.avcodec_get_name(parameters->codec_id));

        switch (type)
        {
            case "video":
                // A corrupt file can still be opened with a codec guessed from the extension, and
                // then reports no dimensions. Showing "0 x 0" is worse than showing nothing.
                if (parameters->width > 0 && parameters->height > 0)
                {
                    MediaMetadataFormat.Add(
                        metadata,
                        $"{label} dimensions",
                        MediaMetadataFormat.Dimensions(parameters->width, parameters->height));

                    AddAspectRatio(metadata, label, parameters);
                }

                MediaMetadataFormat.Add(
                    metadata,
                    $"{label} pixel format",
                    ffmpeg.av_get_pix_fmt_name((AVPixelFormat)parameters->format));

                MediaMetadataFormat.Add(
                    metadata,
                    $"{label} frame rate",
                    MediaMetadataFormat.FrameRate(stream->avg_frame_rate.num, stream->avg_frame_rate.den));

                if (stream->nb_frames > 0)
                {
                    MediaMetadataFormat.Add(metadata, $"{label} frames", stream->nb_frames.ToString());
                }

                break;

            case "audio":
                if (parameters->ch_layout.nb_channels > 0)
                {
                    MediaMetadataFormat.Add(
                        metadata,
                        $"{label} channels",
                        parameters->ch_layout.nb_channels.ToString());
                }

                if (parameters->sample_rate > 0)
                {
                    MediaMetadataFormat.Add(
                        metadata,
                        $"{label} sample rate",
                        MediaMetadataFormat.SampleRate(parameters->sample_rate));
                }

                break;

            case "subtitle":
                MediaMetadataFormat.Add(metadata, $"{label} language", Tag(stream->metadata, "language"));
                break;
        }

        if (parameters->bit_rate > 0)
        {
            MediaMetadataFormat.Add(
                metadata,
                $"{label} bitrate",
                MediaMetadataFormat.Bitrate(parameters->bit_rate));
        }

        AddTags(metadata, stream->metadata, prefix: $"{label} ");
    }

    /// <summary>Display aspect is the stored size scaled by the pixel aspect, reduced.</summary>
    private static unsafe void AddAspectRatio(
        Dictionary<string, string> metadata,
        string label,
        AVCodecParameters* parameters)
    {
        var sample = parameters->sample_aspect_ratio;
        if (sample.num == 0 || sample.den == 0)
        {
            return;
        }

        int numerator;
        int denominator;

        ffmpeg.av_reduce(
            &numerator,
            &denominator,
            parameters->width * (long)sample.num,
            parameters->height * (long)sample.den,
            1024 * 1024);

        MediaMetadataFormat.Add(
            metadata,
            $"{label} aspect ratio",
            MediaMetadataFormat.AspectRatio(numerator, denominator));
    }

    /// <summary>Embedded tags: EXIF on images, creation time and encoder on video, ID3 on audio.</summary>
    private static unsafe void AddTags(
        Dictionary<string, string> metadata,
        AVDictionary* dictionary,
        string prefix)
    {
        if (dictionary is null)
        {
            return;
        }

        AVDictionaryEntry* entry = null;

        while ((entry = ffmpeg.av_dict_get(dictionary, string.Empty, entry, ffmpeg.AV_DICT_IGNORE_SUFFIX)) is not null)
        {
            var key = ToString(entry->key);
            if (string.IsNullOrEmpty(key))
            {
                continue;
            }

            // Language already has its own row on a subtitle stream.
            if (prefix.Length > 0 && key.Equals("language", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            MediaMetadataFormat.Add(
                metadata,
                $"{prefix}{MediaMetadataFormat.Capitalise(key.Replace('_', ' '))}",
                ToString(entry->value));
        }
    }

    private static unsafe string? Tag(AVDictionary* dictionary, string key)
    {
        if (dictionary is null)
        {
            return null;
        }

        var entry = ffmpeg.av_dict_get(dictionary, key, null, 0);

        return entry is null ? null : ToString(entry->value);
    }

    private unsafe byte[]? TryRenderThumbnail(AVFormatContext* format)
    {
        AVCodec* decoder = null;
        var streamIndex = ffmpeg.av_find_best_stream(format, AVMediaType.AVMEDIA_TYPE_VIDEO, -1, -1, &decoder, 0);

        if (streamIndex < 0 || decoder is null)
        {
            // Audio with no cover art, or a file with no picture in it at all.
            return null;
        }

        var stream = format->streams[streamIndex];
        AVCodecContext* codec = null;
        AVFrame* frame = null;
        AVPacket* packet = null;

        try
        {
            codec = ffmpeg.avcodec_alloc_context3(decoder);
            if (codec is null || ffmpeg.avcodec_parameters_to_context(codec, stream->codecpar) < 0)
            {
                return null;
            }

            // 0 lets libav pick a thread count; decoding one frame barely uses them either way.
            codec->thread_count = 0;

            if (ffmpeg.avcodec_open2(codec, decoder, null) < 0)
            {
                return null;
            }

            frame = ffmpeg.av_frame_alloc();
            packet = ffmpeg.av_packet_alloc();

            if (frame is null || packet is null)
            {
                return null;
            }

            // A still image has one frame, so seeking into it would land past the end.
            var seekable = format->duration != ffmpeg.AV_NOPTS_VALUE
                && format->duration > _options.VideoFrameSeconds * ffmpeg.AV_TIME_BASE;

            var moved = seekable && Seek(format, stream, streamIndex, codec);

            if (moved && Decode(format, codec, packet, frame, streamIndex))
            {
                return JpegEncoder.Encode(frame, _options.ThumbnailSize, _options.ThumbnailQuality);
            }

            if (moved)
            {
                // The seek landed somewhere undecodable, so start over from the beginning.
                // Only rewind if we actually moved: a single-image demuxer does not take kindly
                // to being seeked at all, and rewinding one left it with nothing to read.
                ffmpeg.av_seek_frame(format, streamIndex, 0, ffmpeg.AVSEEK_FLAG_BACKWARD);
                ffmpeg.avcodec_flush_buffers(codec);
            }

            if (Decode(format, codec, packet, frame, streamIndex))
            {
                return JpegEncoder.Encode(frame, _options.ThumbnailSize, _options.ThumbnailQuality);
            }

            logger.LogDebug("No frame could be decoded for a thumbnail");

            return null;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Thumbnail rendering failed");
            return null;
        }
        finally
        {
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
        }
    }

    /// <summary>
    /// Moves to the poster frame: <see cref="MediaOptions.VideoFrameSeconds"/> into the media,
    /// measured from where the media actually starts.
    ///
    /// Relative, not absolute. A recording cut from a rolling buffer begins at whatever timestamp
    /// the encoder had reached, which is a large number, and an absolute target below that clamps
    /// to the first frame. Every such document would then show its opening frame and report
    /// success, which is precisely the failure that hid the live preview freeze for months.
    /// </summary>
    private unsafe bool Seek(
        AVFormatContext* format,
        AVStream* stream,
        int streamIndex,
        AVCodecContext* codec)
    {
        var offset = (long)(_options.VideoFrameSeconds / ffmpeg.av_q2d(stream->time_base));
        var start = stream->start_time != ffmpeg.AV_NOPTS_VALUE ? stream->start_time : 0;

        if (ffmpeg.av_seek_frame(format, streamIndex, start + offset, ffmpeg.AVSEEK_FLAG_BACKWARD) < 0)
        {
            return false;
        }

        // Anything buffered belongs to the old position.
        ffmpeg.avcodec_flush_buffers(codec);

        return true;
    }

    /// <summary>Reads until the decoder hands back one frame from the stream we care about.</summary>
    private static unsafe bool Decode(
        AVFormatContext* format,
        AVCodecContext* codec,
        AVPacket* packet,
        AVFrame* frame,
        int streamIndex)
    {
        while (ffmpeg.av_read_frame(format, packet) >= 0)
        {
            try
            {
                if (packet->stream_index != streamIndex)
                {
                    continue;
                }

                if (ffmpeg.avcodec_send_packet(codec, packet) < 0)
                {
                    continue;
                }

                if (ffmpeg.avcodec_receive_frame(codec, frame) == 0)
                {
                    return true;
                }
            }
            finally
            {
                ffmpeg.av_packet_unref(packet);
            }
        }

        // Flush: the frame may still be sitting inside the decoder at end of file.
        ffmpeg.avcodec_send_packet(codec, null);

        return ffmpeg.avcodec_receive_frame(codec, frame) == 0;
    }

    private static unsafe string? ToString(byte* value)
        => value is null ? null : Marshal.PtrToStringAnsi((IntPtr)value);

    /// <summary>Turns a libav negative return code into its message.</summary>
    private static string Describe(int error) => FfmpegLibrary.Describe(error);
}
