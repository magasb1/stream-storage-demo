using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The stream identifier is the only thing an operator types into an encoder, and the name it
/// yields is the stream's identity for its whole life.
/// </summary>
public sealed class StreamNameTests
{
    private static string Parse(string streamId)
    {
        Assert.True(StreamName.TryParse(streamId, out var name, out var rejection), rejection);

        return name;
    }

    private static string Reject(string? streamId)
    {
        Assert.False(StreamName.TryParse(streamId, out var name, out var rejection), $"accepted as '{name}'");
        Assert.NotEqual(string.Empty, rejection);

        return rejection;
    }

    [Theory]
    // A bare name, which is what OBS and Teradek produce and what SRT's own listener accepts.
    [InlineData("camera1", "camera1")]
    [InlineData("live/camera1", "live/camera1")]
    [InlineData("#!::r=camera1", "camera1")]
    [InlineData("#!::u=admin,r=live/camera1,m=publish", "live/camera1")]
    // A Makito in standard-keys mode omits m entirely, and Haivision wrote the format.
    [InlineData("#!::u=admin,r=ietf_107_srt_overview", "ietf_107_srt_overview")]
    [InlineData("#!::r=cam1,m=bidirectional", "cam1")]
    // A sender on FFmpeg 6.1 or older hands over the escape it typed into a URL.
    [InlineData("%23!::r=live/cam1,m=publish", "live/cam1")]
    // Vendor extensions are ignored rather than refused.
    [InlineData("#!::r=cam1,m=publish,volcTime=123,user_thing=x", "cam1")]
    // Handshake padding.
    [InlineData("camera1\0\0", "camera1")]
    [InlineData("  camera1  ", "camera1")]
    public void A_name_is_read_from_whatever_shape_it_arrives_in(string streamId, string expected)
        => Assert.Equal(expected, Parse(streamId));

    /// <summary>
    /// The mode is checked against the port the caller reached, and only an explicit mismatch is
    /// refused: absent means whatever that port is for, because the convention says it is optional
    /// and the vendor who wrote the convention leaves it out.
    /// </summary>
    [Fact]
    public void A_caller_asking_to_receive_is_refused_on_the_ingest_port()
        => Assert.Contains("ingest port", Reject("#!::r=live/cam1,m=request"));

    [Fact]
    public void A_caller_offering_to_send_is_refused_on_the_consumption_port()
    {
        Assert.False(
            StreamName.TryParse("#!::r=cam1,m=publish", out _, out var rejection, StreamIntent.Subscribe));

        Assert.Contains("consumption port", rejection);
    }

    [Fact]
    public void A_viewer_may_ask_to_receive_and_may_say_nothing_at_all()
    {
        Assert.True(StreamName.TryParse("#!::r=cam1,m=request", out var asked, out _, StreamIntent.Subscribe));
        Assert.Equal("cam1", asked);

        Assert.True(StreamName.TryParse("cam1", out var bare, out _, StreamIntent.Subscribe));
        Assert.Equal("cam1", bare);
    }

    /// <summary>
    /// The rollback position rides in a key the convention reserves for exactly this, so carrying
    /// one needs no extension to the format and no second field.
    /// </summary>
    [Theory]
    [InlineData("#!::r=cam1,user_from=20,m=request", 20.0)]
    [InlineData("#!::r=cam1,user_from=6.5", 6.5)]
    public void A_viewer_may_ask_to_start_further_back(string streamId, double expected)
        => Assert.Equal(expected, StreamName.Position(streamId));

    [Theory]
    [InlineData("#!::r=cam1,m=request")]
    [InlineData("#!::r=cam1,user_from=0")]
    [InlineData("#!::r=cam1,user_from=-5")]
    [InlineData("#!::r=cam1,user_from=soon")]
    [InlineData("cam1")]
    public void No_position_means_the_live_edge(string streamId)
        => Assert.Null(StreamName.Position(streamId));

    /// <summary>
    /// The identifier begins with '#', which starts a fragment in a URL, so a caller that puts it
    /// in one has to write %23 - and FFmpeg versions disagree about when they decode that.
    /// </summary>
    [Fact]
    public void An_identifier_truncated_at_the_fragment_is_refused_rather_than_guessed_at()
    {
        // What arrives when a URL parser treated the '#' as a fragment: the key is there and the
        // value is gone.
        Assert.Contains("empty", Reject(string.Empty));
    }

    [Fact]
    public void An_envelope_with_no_resource_key_has_no_name()
        => Assert.Contains("resource key", Reject("#!::u=admin"));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\0")]
    public void Nothing_at_all_is_refused(string? streamId) => Reject(streamId);

    [Theory]
    // Path traversal, which is why names are rejected rather than cleaned up: the name reaches a
    // registry key and a document name.
    [InlineData("../../etc/passwd")]
    [InlineData("live/../secret")]
    [InlineData("/leading")]
    [InlineData("trailing/")]
    [InlineData("double//slash")]
    // Structural in the convention, so a name carrying one could never round-trip.
    [InlineData("has,comma")]
    [InlineData("has=equals")]
    // Two names that would normalise onto one stream, which is the hijack this rule prevents.
    [InlineData("cam 1")]
    [InlineData("caméra")]
    public void An_unusable_name_is_rejected_rather_than_repaired(string streamId) => Reject(streamId);

    [Fact]
    public void An_identifier_longer_than_the_handshake_allows_is_refused()
        => Assert.Contains("512", Reject(new string('a', StreamName.MaxIdentifierBytes + 1)));

    [Fact]
    public void A_name_at_the_limit_of_the_allowed_length_is_accepted()
        => Assert.Equal(new string('a', 128), Parse(new string('a', 128)));

    [Fact]
    public void A_name_past_the_allowed_length_is_refused()
        => Reject(new string('a', 129));

    /// <summary>The slot a push token will occupy, so adding one later changes no format.</summary>
    [Fact]
    public void A_session_key_is_noticed_and_ignored()
    {
        Assert.Equal("cam1", Parse("#!::r=cam1,s=a-token,m=publish"));

        Assert.True(StreamName.CarriesSessionKey("#!::r=cam1,s=a-token"));
        Assert.False(StreamName.CarriesSessionKey("#!::r=cam1"));
        Assert.False(StreamName.CarriesSessionKey("cam1"));
    }

    /// <summary>
    /// Case-sensitively and byte-exactly, because a reconnect resuming the same stream is decided
    /// by comparing this string.
    /// </summary>
    [Fact]
    public void Two_names_differing_only_in_case_are_two_names()
        => Assert.NotEqual(Parse("Camera1"), Parse("camera1"));
}
