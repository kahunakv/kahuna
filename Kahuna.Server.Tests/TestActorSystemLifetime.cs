using Nixie;

namespace Kahuna.Server.Tests;

internal static class TestActorSystemLifetime
{
    public static IDisposable Create(out ActorSystem actorSystem)
    {
        actorSystem = new();
        return new Lifetime(actorSystem);
    }

    private sealed class Lifetime(ActorSystem actorSystem) : IDisposable
    {
        public void Dispose()
        {
            actorSystem.GracefulShutdownAll(TimeSpan.FromSeconds(5)).GetAwaiter().GetResult();
            actorSystem.Dispose();
        }
    }
}
