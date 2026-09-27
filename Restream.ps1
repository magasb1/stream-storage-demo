<#
.SYNOPSIS
    Pushes any source ffmpeg can read into the live ingest port, as a named stream.

.DESCRIPTION
    Stands in for a camera. Point it at a file, an RTSP camera, an HLS playlist or another SRT
    feed, give the stream a name, and the service picks it up exactly as it would a real encoder:
    nothing is requested first, the name arrives in the SRT stream identifier, and the stream is
    on air from the moment it connects.

    It remultiplexes rather than re-encodes, so even a large feed costs almost no processor. Pass
    -Transcode for a source whose codecs a transport stream cannot carry.

    It reconnects on its own, which is what a real encoder does and what the service counts on: a
    replica going away is absorbed by the sender retrying.

.PARAMETER Source
    Anything ffmpeg can open: a file path, http/https, rtsp://, srt://, udp://, an HLS .m3u8.

.PARAMETER Name
    What the stream is called. This is its identity for its whole life, so a feed that drops and
    reconnects under the same name resumes rather than becoming a second stream. Defaults to the
    last part of the source.

    Letters, digits, dot, dash, underscore and slash, up to 128 of them. The service rejects
    anything else rather than cleaning it up, because two names that normalised to one would let
    one encoder take over another's stream. This refuses it here, where the message is clearer.

.PARAMETER Ingest
    The ingest port. Defaults to the one the Compose stack publishes.

.PARAMETER Transcode
    Re-encode instead of copying. Needed for a source whose codecs will not go into a transport
    stream, and for one that emits keyframes too rarely to cut a buffer on.

.PARAMETER KeyframeSeconds
    While transcoding, how often to emit a keyframe. This decides how finely the service can cut
    its rolling buffer: the buffer is held as segments one keyframe apart, so a long interval
    means a viewer joins, rolls back and starts a recording in coarse steps.

    It is also the floor on how long a viewer waits to see a picture, whatever the SRT latency is
    set to, because a decoder can only start at a keyframe. One second is the reason the default
    is one second. It applies only while transcoding: a remultiplex copies the source's keyframes
    across as they are, so with a camera emitting one every ten seconds that is the wait, and the
    only ways out are the camera's own settings or -Transcode.

.PARAMETER Loop
    Play a finite source over and over, so a short clip behaves like a camera that never stops.

.PARAMETER Once
    Do not retry. Without this the sender reconnects for as long as the window is open.

.EXAMPLE
    .\Restream.ps1 .\clip.mp4 live/camera1 -Loop

.EXAMPLE
    .\Restream.ps1 -Source rtsp://192.168.1.50/stream1 -Name carpark/north

.EXAMPLE
    .\Restream.ps1 -Source https://example.com/feed.m3u8 -Name motorway -Ingest srt://10.0.0.5:9000
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory, Position = 0)]
    [string] $Source,

    [Parameter(Position = 1)]
    [string] $Name,

    [string] $Ingest = 'srt://127.0.0.1:9000',

    [string] $Watch = 'srt://127.0.0.1:9010',

    [switch] $Transcode,

    [ValidateRange(1, 60)]
    [int] $KeyframeSeconds = 1,

    [switch] $Loop,

    [switch] $Once,

    [string] $FfmpegPath
)

$ErrorActionPreference = 'Stop'

# ffmpeg leaves a non-zero exit behind whenever a connection drops, which is the normal case this
# script retries. On PowerShell 7.4 and later that would otherwise be a terminating error and the
# retry loop would never run.
$PSNativeCommandUseErrorActionPreference = $false

function Resolve-Ffmpeg {
    <#
        The build that ships with this repository is the one with libsrt in it. The machine's own
        ffmpeg is the last resort and usually has no SRT at all, which is why the next function
        checks rather than letting it fail at the first connection with an I/O error.
    #>
    param([string] $Explicit)

    $candidates = @()
    if ($Explicit) { $candidates += $Explicit }
    $candidates += (Join-Path $PSScriptRoot 'ffmpeg\win-x64\ffmpeg.exe')

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) { return (Resolve-Path $candidate).Path }
    }

    $onPath = Get-Command ffmpeg -ErrorAction SilentlyContinue
    if ($onPath) { return $onPath.Source }

    throw "No ffmpeg found. Run scripts/fetch-ffmpeg.sh to put one with SRT into ffmpeg\win-x64\, or pass -FfmpegPath."
}

function Assert-Srt {
    param([string] $Ffmpeg)

    $protocols = & $Ffmpeg -hide_banner -protocols 2>&1 | Out-String

    if ($protocols -notmatch '(?m)^\s*srt\s*$') {
        throw "'$Ffmpeg' has no SRT support, so it cannot reach the ingest port. Run scripts/fetch-ffmpeg.sh for a build that has it."
    }
}

function Resolve-StreamName {
    <#
        The same rule the service applies, checked here so a bad name fails with an explanation
        rather than as a connection the server accepts and then drops. A stream cannot be refused
        by name before its connection exists, so this is the only place it can be refused kindly.
    #>
    param([string] $Proposed, [string] $From)

    if (-not $Proposed) {
        # The last meaningful part of the source, without any query string or extension.
        $tail = ($From -split '[?#]')[0].TrimEnd('/', '\') -split '[\\/]' | Select-Object -Last 1
        $Proposed = [System.IO.Path]::GetFileNameWithoutExtension($tail)
    }

    if ($Proposed -notmatch '^[A-Za-z0-9._/-]{1,128}$' -or
        $Proposed.StartsWith('/') -or $Proposed.EndsWith('/') -or
        $Proposed.Contains('//') -or ($Proposed -split '/') -contains '..') {

        throw "'$Proposed' is not a usable stream name. Letters, digits, dot, dash, underscore and slash, up to 128 of them, with no empty segment. Pass -Name."
    }

    return $Proposed
}

function Get-FfmpegArguments {
    param([string] $StreamName, [bool] $SourceIsLive)

    $arguments = @('-hide_banner', '-loglevel', 'warning', '-stats')

    if ($Loop) { $arguments += @('-stream_loop', '-1') }

    # A file arrives as fast as the disk can read it, and a burst that ends at once is not a stream:
    # the far end sees a peer hang up mid-handshake.
    if (-not $SourceIsLive) { $arguments += '-re' }

    if ($Source -match '^https?://') {
        $arguments += @('-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5')
    }

    if ($Source -match '^rtsp://') {
        # Interleaved over TCP, because RTP over UDP through anything doing NAT is a coin toss.
        $arguments += @('-rtsp_transport', 'tcp')
    }

    $arguments += @('-i', $Source)

    if ($Transcode) {
        # Three flags for one keyframe interval, and all three are needed.
        $keyframeInterval = $KeyframeSeconds * 25

        $arguments += @(
            '-map', '0',
            '-c:v', 'mpeg2video', '-b:v', '4000k',
            '-g', "$keyframeInterval", '-keyint_min', "$keyframeInterval", '-sc_threshold', '0',
            '-c:a', 'mp2', '-b:a', '128k',
            '-c:d', 'copy')
    }
    else {
        # A remultiplex: encoded frames are copied across untouched, which is what keeps this cheap
        # enough to run several of at once.
        $arguments += @('-map', '0', '-c', 'copy')
    }

    # -map 0 on both paths, and it is load-bearing rather than tidy: without it ffmpeg's default
    # stream selection silently drops the KLV data stream, and every MISB feature here looks broken
    # rather than unfed.

    # Timestamps are generated where the source carries none, rather than the muxer refusing the
    # first packet it cannot place.
    $arguments += @('-fflags', '+genpts', '-f', 'mpegts')

    # A bare name, not the convention's #!::r=... envelope.
    $arguments += "$Ingest`?mode=caller&streamid=$StreamName"

    return $arguments
}

$ffmpeg = Resolve-Ffmpeg -Explicit $FfmpegPath
Assert-Srt -Ffmpeg $ffmpeg

$streamName = Resolve-StreamName -Proposed $Name -From $Source

# A source that is already a live transport paces itself; anything else has to be paced.
$sourceIsLive = $Source -match '^(rtsp|srt|udp|rtp|rtmp)://'

Write-Host "ffmpeg  $ffmpeg"
Write-Host "source  $Source"
Write-Host "name    $streamName"
Write-Host "ingest  $Ingest"
Write-Host ""
Write-Host "Watch it with:" -ForegroundColor DarkGray
Write-Host "  ffplay -analyzeduration 1000000 -probesize 1000000 -fflags nobuffer `"$Watch`?mode=caller&streamid=$streamName`"" -ForegroundColor DarkGray
Write-Host ""

$arguments = Get-FfmpegArguments -StreamName $streamName -SourceIsLive $sourceIsLive

while ($true) {
    & $ffmpeg @arguments

    if ($Once) { break }

    Write-Host "Sender stopped. Reconnecting in 2s; Ctrl+C to stop." -ForegroundColor DarkYellow
    Start-Sleep -Seconds 2
}
