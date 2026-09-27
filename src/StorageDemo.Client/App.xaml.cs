using System.Windows;

namespace StorageDemo.Client;

public partial class App : Application
{
    private void OnStartup(object sender, StartupEventArgs e)
    {
        var address = e.Args.FirstOrDefault()
            ?? Environment.GetEnvironmentVariable("STORAGEDEMO_API")
            ?? "http://127.0.0.1:5080";

        new MainWindow(address).Show();
    }
}
