using System.ComponentModel;
using System.IO;
using System.Windows;
using System.Windows.Media.Imaging;
using StorageDemo.Core.Documents;
using StorageDemo.Grpc;

namespace StorageDemo.Client;

/// <summary>One row of the metadata panel.</summary>
public sealed record MetadataRow(string Key, string Value);

/// <summary>
/// What the transport did over the last heartbeat, reduced to the four answers a tile shows.
/// </summary>
public enum StreamHealth
{
    None,
    Healthy,

    /// <summary>Packets arrived too late for the latency window, or the buffer is complaining.</summary>
    Dropped,

    /// <summary>Packets never arrived at all, which is the one worth interrupting someone for.</summary>
    Lost,

    Interrupted,
}

/// <summary>One tile in the explorer.</summary>
public sealed class DocumentItem : INotifyPropertyChanged
{
    private DocumentMessage? _document;
    private LiveStreamMessage? _live;
    private BitmapImage? _thumbnail;
    private string? _pendingName;
    private long _pendingSize;
    private int _lossBeats;

    private DocumentItem()
    {
    }

    public DocumentItem(DocumentMessage document) => _document = document;

    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>Null until the upload completes, and for a live session.</summary>
    public DocumentMessage? Document => _document;

    /// <summary>Set when this tile is a stream rather than a stored file.</summary>
    public LiveStreamMessage? Live => _live;

    public bool IsLive => _live is not null;

    public bool IsPending => _document is null && _live is null;

    /// <summary>A document's identifier, or a stream's name.</summary>
    public string Id => _document?.Id ?? _live?.Name ?? string.Empty;

    public string FileName => _document?.FileName ?? _live?.Name ?? _pendingName ?? string.Empty;

    public long Size => _document?.Size ?? _live?.Bytes ?? _pendingSize;

    public bool HasThumbnail => _document?.HasThumbnail ?? _live?.HasPreview ?? false;

    /// <summary>An interrupted stream is dimmed rather than removed, so the tile stays put.</summary>
    public bool IsInterrupted => _live?.State == "Interrupted";

    public bool IsRecording => _live?.Recording is not null;

    /// <summary>A tile that is still uploading is as new as it gets.</summary>
    public DateTimeOffset CreatedAt => _document?.CreatedAt?.ToDateTimeOffset() ?? DateTimeOffset.UtcNow;

    public DocumentKind Kind => IsLive
        ? DocumentKind.Video
        : ContentTypes.KindOf(_document?.ContentType, FileName);

    public string Details => IsLive
        ? $"{_live!.State}{(_live.Recording is null ? string.Empty : "  REC")}"
            + $"{(_live.DetectionEnabled ? "  DET" : string.Empty)}{HealthWord}  {HumanSize(Size)}"
        : IsPending
            ? "Uploading..."
            : $"{Icon}  {HumanSize(Size)}";

    /// <summary>
    /// The health bar's colour is not the only signal: this word says the same thing for anyone who
    /// cannot tell the two warning colours apart.
    /// </summary>
    private string HealthWord => Health switch
    {
        StreamHealth.Lost => "  LOST",
        StreamHealth.Dropped => "  DROPPED",
        _ => string.Empty,
    };

    /// <summary>
    /// Red is held for three beats, because the figures are per heartbeat: a single bad beat would
    /// otherwise blink once and be gone before anyone looked up.
    /// </summary>
    public StreamHealth Health => !IsLive
        ? StreamHealth.None
        : IsInterrupted
            ? StreamHealth.Interrupted
            : _lossBeats > 0
                ? StreamHealth.Lost
                : _live!.PacketsDropped > 0 || !_live.Startable || _live.CeilingBinding
                    ? StreamHealth.Dropped
                    : StreamHealth.Healthy;

    public Visibility HealthVisibility => IsLive ? Visibility.Visible : Visibility.Collapsed;

    /// <summary>Colour and position, read across a grid without reading anything.</summary>
    public string HealthBrush => Health switch
    {
        StreamHealth.Healthy => "#2E7D32",
        StreamHealth.Dropped => "#E08A00",
        StreamHealth.Lost => "#C62828",
        StreamHealth.Interrupted => "#9E9E9E",
        _ => "Transparent",
    };

    /// <summary>Where the figures live: in words, off the face of the tile.</summary>
    public string? HealthTooltip
    {
        get
        {
            if (!IsLive)
            {
                // Null rather than empty: a document's tile then has no tooltip at all.
                return null;
            }

            var text = Health switch
            {
                StreamHealth.Interrupted => "Interrupted: nothing is arriving.",
                StreamHealth.Lost => "Packets lost: part of this stream never arrived.",
                StreamHealth.Dropped => "Packets dropped: they arrived too late to be used.",
                _ => "Healthy.",
            };

            text += $"  {_live!.PacketsLost:N0} lost, {_live.PacketsDropped:N0} dropped in the last heartbeat.";

            if (!_live.Startable)
            {
                text += "  No keyframe recently enough to start from.";
            }

            if (_live.CeilingBinding)
            {
                text += "  The buffer's byte ceiling is binding.";
            }

            return text;
        }
    }

    /// <summary>The ST 0102 marking to print for a stream.</summary>
    public string Marking => !IsLive
        ? string.Empty
        : string.IsNullOrWhiteSpace(_live!.Classification)
            ? "UNMARKED"
            : _live.Classification.Trim();

    public Visibility MarkingVisibility => IsLive ? Visibility.Visible : Visibility.Collapsed;

    /// <summary>
    /// Marking colours are the deployment's convention rather than the standard's, so they are a
    /// table and not a rule.
    /// </summary>
    public string MarkingBrush => Marking switch
    {
        "UNMARKED" => "#B45309",
        var m when m.StartsWith("TOP SECRET", StringComparison.OrdinalIgnoreCase) => "#D97706",
        var m when m.StartsWith("SECRET", StringComparison.OrdinalIgnoreCase) => "#B91C1C",
        var m when m.StartsWith("CONFIDENTIAL", StringComparison.OrdinalIgnoreCase) => "#1D4ED8",
        var m when m.StartsWith("RESTRICTED", StringComparison.OrdinalIgnoreCase) => "#6D28D9",
        var m when m.StartsWith("UNCLASSIFIED", StringComparison.OrdinalIgnoreCase) => "#15803D",
        _ => "#374151",
    };

    public double Dimming => IsPending || IsInterrupted ? 0.45 : 1.0;

    /// <summary>A running stream is marked, because it is the one tile that changes on its own.</summary>
    public Visibility LiveBadgeVisibility => IsLive ? Visibility.Visible : Visibility.Collapsed;

    /// <summary>Everything the server's probe reported, in the order it reported it.</summary>
    public IReadOnlyList<MetadataRow> Metadata
    {
        get
        {
            if (_live is not null)
            {
                var rows = new List<MetadataRow>
                {
                    new("State", _live.State),
                    new("Source", _live.Manual ? "Created by request" : "Named itself on connect"),
                    new("Layout", _live.Layout),
                    new("Replica", _live.Owner),
                    new("Packets", _live.Packets.ToString("N0")),
                    new("Carried", HumanSize(_live.Bytes)),
                    new("Buffered", $"{_live.BufferedSeconds:0.#} s"),
                    new("Marking", Marking),
                    new("Lost (last beat)", _live.PacketsLost.ToString("N0")),
                    new("Dropped (last beat)", _live.PacketsDropped.ToString("N0")),
                };

                if (_live.Recording is { } recording)
                {
                    rows.Add(new MetadataRow(
                        "Recording",
                        $"since {recording.StartedAt.ToDateTimeOffset().LocalDateTime:HH:mm:ss}, "
                        + HumanSize(recording.Bytes)));
                }

                if (_live.DetectionEnabled)
                {
                    rows.Add(new MetadataRow(
                        "Detection",
                        $"{(_live.DetectionRate > 0 ? $"{_live.DetectionRate}/s" : "default rate")}, "
                        + (_live.HasDetectionWorker ? $"worker {_live.DetectionWorker}" : "no worker yet")));
                    rows.Add(new MetadataRow(
                        "Detection model",
                        _live.HasDetectionModel ? _live.DetectionModel : "worker default"));
                    rows.Add(new MetadataRow(
                        "Detection classes",
                        _live.DetectionLabels.Count == 0
                            ? "all COCO classes"
                            : string.Join(", ", _live.DetectionLabels)));
                }

                if (!_live.Startable)
                {
                    rows.Add(new MetadataRow("Warning", "No keyframe recently enough to start from."));
                }

                if (_live.CeilingBinding)
                {
                    rows.Add(new MetadataRow("Warning", "The buffer's byte ceiling is binding, so "
                        + "pre-rolls are shorter than the window promises."));
                }

                return rows;
            }

            return _document is null
                ? []
                : [.. _document.Metadata.Select(entry => new MetadataRow(entry.Key, entry.Value))];
        }
    }

    public static DocumentItem ForLive(LiveStreamMessage live)
    {
        // Through Apply, so a stream that is already losing packets when it is first seen is red on
        // its first tile rather than on its second.
        var item = new DocumentItem();
        item.Apply(live);

        return item;
    }

    /// <summary>The MISB ST 0902 minimum set as rows, in the order the standard lists it.</summary>
    public static IReadOnlyList<MetadataRow> KlvRows(LiveKlvMessage klv)
    {
        var rows = new List<MetadataRow>();

        void Add(string key, string? value)
        {
            if (value is { Length: > 0 })
            {
                rows.Add(new MetadataRow(key, value));
            }
        }

        var fields = klv.Fields;

        if (fields is null)
        {
            Add("Received", klv.ReceivedAt?.ToDateTimeOffset().LocalDateTime.ToString("HH:mm:ss.fff"));
            Add("Raw packet", $"{klv.Raw.Length:N0} bytes");

            return rows;
        }

        Add("Timestamp", fields.Timestamp?.ToDateTimeOffset().LocalDateTime.ToString("HH:mm:ss.fff"));
        Add("Mission", fields.HasMissionId ? fields.MissionId : null);
        Add("Platform", fields.HasPlatformDesignation ? fields.PlatformDesignation : null);
        Add("Sensor", fields.HasImageSourceSensor ? fields.ImageSourceSensor : null);
        Add("Coordinate system", fields.HasImageCoordinateSystem ? fields.ImageCoordinateSystem : null);
        Add("Platform heading", Degrees(fields.HasPlatformHeading, fields.PlatformHeading));
        Add("Platform pitch", Degrees(fields.HasPlatformPitch, fields.PlatformPitch));
        Add("Platform roll", Degrees(fields.HasPlatformRoll, fields.PlatformRoll));
        Add("Sensor position", Position(
            fields.HasSensorLatitude, fields.SensorLatitude,
            fields.HasSensorLongitude, fields.SensorLongitude));
        Add("Sensor altitude", Metres(fields.HasSensorTrueAltitude, fields.SensorTrueAltitude));
        Add("Field of view", Degrees(fields.HasSensorHorizontalFov, fields.SensorHorizontalFov) is { } h
            && Degrees(fields.HasSensorVerticalFov, fields.SensorVerticalFov) is { } v
                ? $"{h} x {v}"
                : null);
        Add("Relative azimuth", Degrees(fields.HasSensorRelativeAzimuth, fields.SensorRelativeAzimuth));
        Add("Relative elevation", Degrees(fields.HasSensorRelativeElevation, fields.SensorRelativeElevation));
        Add("Relative roll", Degrees(fields.HasSensorRelativeRoll, fields.SensorRelativeRoll));
        Add("Slant range", Metres(fields.HasSlantRange, fields.SlantRange));
        Add("Frame centre", Position(
            fields.HasFrameCenterLatitude, fields.FrameCenterLatitude,
            fields.HasFrameCenterLongitude, fields.FrameCenterLongitude));
        Add("Frame centre elevation", Metres(fields.HasFrameCenterElevation, fields.FrameCenterElevation));
        Add("Marking", fields.HasClassification && fields.Classification.Length > 0
            ? fields.Classification
            : "UNMARKED");
        Add("UAS LS version", fields.HasVersion ? fields.Version.ToString() : null);
        Add("Alignment", klv.Alignment switch
        {
            KlvAlignment.PresentationTimestamp => $"presentation timestamp {klv.ReferencePts}",
            KlvAlignment.Timestamp => "packet timestamp only",
            _ => null,
        });
        Add("Received", klv.ReceivedAt?.ToDateTimeOffset().LocalDateTime.ToString("HH:mm:ss.fff"));
        Add("Other items", fields.Unparsed.Count > 0 ? $"{fields.Unparsed.Count} outside the set" : null);
        Add("Raw packet", $"{klv.Raw.Length:N0} bytes");

        return rows;
    }

    private static string? Degrees(bool present, double value) => present ? $"{value:0.###}°" : null;

    private static string? Metres(bool present, double value) => present ? $"{value:N0} m" : null;

    private static string? Position(bool hasLatitude, double latitude, bool hasLongitude, double longitude)
        => hasLatitude && hasLongitude ? $"{latitude:0.######}, {longitude:0.######}" : null;

    /// <summary>Refreshes a live tile in place, so its stats move without the tile being rebuilt.</summary>
    public void Apply(LiveStreamMessage live)
    {
        _live = live;
        _lossBeats = live.PacketsLost > 0 ? LossBeats : Math.Max(0, _lossBeats - 1);

        Raise(
            nameof(Live),
            nameof(FileName),
            nameof(Size),
            nameof(HasThumbnail),
            nameof(Details),
            nameof(Dimming),
            nameof(IsInterrupted),
            nameof(IsRecording),
            nameof(Health),
            nameof(HealthBrush),
            nameof(HealthTooltip),
            nameof(Marking),
            nameof(MarkingBrush),
            nameof(Metadata));
    }

    private const int LossBeats = 3;

    public string Icon => Kind switch
    {
        DocumentKind.Text => "TXT",
        DocumentKind.Pdf => "PDF",
        DocumentKind.Image => "IMG",
        DocumentKind.Video => "VID",
        DocumentKind.Audio => "AUD",
        _ => "BIN",
    };

    /// <summary>
    /// Rendered by the server with ffmpeg, for images and video alike, so a tile costs one small
    /// JPEG rather than downloading and decoding the original.
    /// </summary>
    public BitmapImage? Thumbnail
    {
        get => _thumbnail;
        set
        {
            _thumbnail = value;
            Raise(nameof(Thumbnail), nameof(ThumbnailVisibility), nameof(IconVisibility), nameof(PlayBadgeVisibility));
        }
    }

    public Visibility ThumbnailVisibility => _thumbnail is null ? Visibility.Collapsed : Visibility.Visible;

    public Visibility IconVisibility => _thumbnail is null ? Visibility.Visible : Visibility.Collapsed;

    /// <summary>A play badge over the tile, so a video reads as a video and not as a photo.</summary>
    public Visibility PlayBadgeVisibility =>
        Kind == DocumentKind.Video && _thumbnail is not null && !IsLive
            ? Visibility.Visible
            : Visibility.Collapsed;

    /// <summary>A tile for a file that is still uploading.</summary>
    public static DocumentItem Pending(string path)
    {
        var item = new DocumentItem
        {
            _pendingName = Path.GetFileName(path),
            _pendingSize = new FileInfo(path).Length,
        };

        if (ContentTypes.KindOf(null, item.FileName) == DocumentKind.Image)
        {
            item.Thumbnail = TryLoadLocalThumbnail(path);
        }

        return item;
    }

    /// <summary>Swaps in the real document once the server has answered, keeping the same tile.</summary>
    public void Apply(DocumentMessage document)
    {
        _document = document;

        Raise(
            nameof(Document),
            nameof(IsPending),
            nameof(Id),
            nameof(FileName),
            nameof(Size),
            nameof(HasThumbnail),
            nameof(Kind),
            nameof(Details),
            nameof(Dimming),
            nameof(Metadata),
            nameof(Icon),
            nameof(IconVisibility),
            nameof(PlayBadgeVisibility));
    }

    public static string HumanSize(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => $"{bytes / 1024.0:0.#} KB",
        < 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024):0.#} MB",
        _ => $"{bytes / (1024.0 * 1024 * 1024):0.#} GB",
    };

    private static BitmapImage? TryLoadLocalThumbnail(string path)
    {
        try
        {
            var bitmap = new BitmapImage();
            bitmap.BeginInit();
            bitmap.CacheOption = BitmapCacheOption.OnLoad;
            bitmap.DecodePixelWidth = 160;
            bitmap.StreamSource = new MemoryStream(File.ReadAllBytes(path));
            bitmap.EndInit();
            bitmap.Freeze();

            return bitmap;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private void Raise(params string[] properties)
    {
        foreach (var property in properties)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(property));
        }
    }
}
