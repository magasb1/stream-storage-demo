using StorageDemo.Core.Documents;

namespace StorageDemo.Tests.Application;

/// <summary>The extension table comes from MimeTypesMap rather than a hand-written list.</summary>
public sealed class ContentTypeTests
{
    [Theory]
    [InlineData("clip.mp4", "video/mp4")]
    [InlineData("clip.mkv", "video/x-matroska")]
    [InlineData("clip.webm", "video/webm")]
    [InlineData("clip.mov", "video/quicktime")]
    [InlineData("clip.avi", "video/x-msvideo")]
    public void Video_containers_are_recognised(string fileName, string expected)
        => Assert.Equal(expected, ContentTypes.Guess(fileName));

    [Theory]
    [InlineData("stream.ts")]
    [InlineData("stream.m2ts")]
    [InlineData("stream.mts")]
    public void Transport_streams_are_video_rather_than_typescript(string fileName)
        => Assert.Equal("video/mp2t", ContentTypes.Guess(fileName));

    [Theory]
    [InlineData("photo.jpg", "image/jpeg")]
    [InlineData("photo.png", "image/png")]
    [InlineData("photo.webp", "image/webp")]
    [InlineData("song.mp3", "audio/mpeg")]
    [InlineData("report.pdf", "application/pdf")]
    [InlineData("notes.txt", "text/plain")]
    public void Common_types_map_as_expected(string fileName, string expected)
        => Assert.Equal(expected, ContentTypes.Guess(fileName));

    [Theory]
    [InlineData("mystery", ContentTypes.Unknown)]
    [InlineData("mystery.zzzz", ContentTypes.Unknown)]
    public void An_unknown_extension_falls_back_to_binary(string fileName, string expected)
        => Assert.Equal(expected, ContentTypes.Guess(fileName));

    [Theory]
    [InlineData("clip.mp4", DocumentKind.Video)]
    [InlineData("stream.ts", DocumentKind.Video)]
    [InlineData("photo.jpg", DocumentKind.Image)]
    [InlineData("song.mp3", DocumentKind.Audio)]
    [InlineData("report.pdf", DocumentKind.Pdf)]
    [InlineData("notes.txt", DocumentKind.Text)]
    [InlineData("archive.zip", DocumentKind.Other)]
    public void Kind_is_derived_from_the_name_when_no_type_was_declared(string fileName, DocumentKind expected)
        => Assert.Equal(expected, ContentTypes.KindOf(null, fileName));

    [Fact]
    public void A_declared_type_wins_over_the_extension()
        => Assert.Equal(DocumentKind.Image, ContentTypes.KindOf("image/png", "mystery.bin"));

    [Fact]
    public void A_useless_declared_type_falls_back_to_the_extension()
        => Assert.Equal(DocumentKind.Video, ContentTypes.KindOf(ContentTypes.Unknown, "clip.mp4"));
}
