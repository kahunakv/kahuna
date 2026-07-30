
namespace Kahuna.Server.Tests;

/// <summary>A disposable that does nothing — used by test IRaft stubs to satisfy retention-hold calls.</summary>
internal sealed class NoopDisposable : IDisposable
{
    public static readonly NoopDisposable Instance = new();
    public void Dispose() { }
}
