using System.IO;
using System.Text.Json;

namespace StorageDemo.Client;

public sealed record ServerEntry(string Name, string Address)
{
    /// <summary>
    /// The live token this server wants, carried as x-storage-token on the guarded live calls.
    /// </summary>
    public string Token { get; init; } = string.Empty;

    // The token is deliberately not shown: this label is what the server box lists.
    public override string ToString() => $"{Name}  —  {Address}";
}

/// <summary>
/// The switches that are the user's rather than any server's, beside servers.json in the same
/// profile folder and with the same attitude to a file it cannot read: a forgotten preference is a
/// nuisance, not a reason to fail startup.
/// </summary>
public static class ClientPreferences
{
    private static readonly string FilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "StorageDemoClient",
        "preferences.json");

    private sealed record Stored(bool Hud);

    private static bool _hud = Read();

    /// <summary>Whether the sensor heads-up display is drawn over a stream that carries KLV.</summary>
    public static bool Hud
    {
        get => _hud;
        set
        {
            _hud = value;

            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(FilePath)!);
                File.WriteAllText(FilePath, JsonSerializer.Serialize(new Stored(value)));
            }
            catch (Exception)
            {
            }
        }
    }

    private static bool Read()
    {
        try
        {
            return !File.Exists(FilePath)
                || JsonSerializer.Deserialize<Stored>(File.ReadAllText(FilePath))?.Hud != false;
        }
        catch (Exception)
        {
            return true;
        }
    }
}

/// <summary>
/// The endpoints the client can switch between, kept in the user's profile so the list survives a
/// rebuild.
/// </summary>
public sealed class ServerList
{
    private static readonly string FilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "StorageDemoClient",
        "servers.json");

    private static readonly ServerEntry[] Defaults =
    [
        new("Local (dotnet run)", "http://127.0.0.1:5080"),
        new("Docker Compose", "http://127.0.0.1:5081"),
        new("Kubernetes (port-forward)", "http://127.0.0.1:5082"),
        new("Talos lab", "http://10.10.10.122:5080"),
    ];

    public List<ServerEntry> Entries { get; private set; } = [.. Defaults];

    public static ServerList Load()
    {
        var list = new ServerList();

        try
        {
            if (File.Exists(FilePath))
            {
                var loaded = JsonSerializer.Deserialize<List<ServerEntry>>(File.ReadAllText(FilePath));
                if (loaded is { Count: > 0 })
                {
                    list.Entries = loaded;

                    // A saved list is the user's, but a default added since it was written would
                    // otherwise never appear: the file exists, so nothing reads the defaults again.
                    list.Entries.AddRange(Defaults.Where(d =>
                        !loaded.Any(e => string.Equals(e.Address, d.Address, StringComparison.OrdinalIgnoreCase))));
                }
            }
        }
        catch (Exception)
        {
            // A corrupt or unreadable file is not worth failing startup over; fall back to
            // defaults.
        }

        return list;
    }

    /// <summary>
    /// Remembers an address the user typed, so it is one click away next time, and the token that
    /// worked with it, so it does not have to be typed again either.
    /// </summary>
    public void Remember(string address, string token)
    {
        var index = Entries.FindIndex(e => string.Equals(e.Address, address, StringComparison.OrdinalIgnoreCase));

        if (index < 0)
        {
            Entries.Add(new ServerEntry(address, address) { Token = token });
        }
        else if (Entries[index].Token != token)
        {
            Entries[index] = Entries[index] with { Token = token };
        }
        else
        {
            return;
        }

        Save();
    }

    public string TokenFor(string address)
        => Entries.FirstOrDefault(e => string.Equals(e.Address, address, StringComparison.OrdinalIgnoreCase))
            ?.Token ?? string.Empty;

    public void Save()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(FilePath)!);
            File.WriteAllText(
                FilePath,
                JsonSerializer.Serialize(Entries, new JsonSerializerOptions { WriteIndented = true }));
        }
        catch (Exception)
        {
            // Losing the recents list is a nuisance, not a failure worth interrupting the user for.
        }
    }
}
