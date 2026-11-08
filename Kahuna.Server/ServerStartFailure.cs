namespace Kahuna.Server;

/// <summary>
/// The first failure that stopped the server from starting. When one is recorded, the process exits with
/// <see cref="ExitCode"/>, so a supervisor sees a failed start and not a clean stop: systemd restarts a unit
/// with <c>Restart=on-failure</c> only on a non-zero exit, and a Kubernetes Job counts a zero exit as success.
/// <para>
/// A failure inside a hosted service never reaches <c>Program</c>. The host logs it, stops, and
/// <c>WaitForShutdownAsync</c> returns the same as for a requested stop, so the service records the failure here.
/// Only <c>Program</c> sets the process exit code, because the services also run inside test processes.
/// </para>
/// </summary>
public sealed class ServerStartFailure
{
    /// <summary>The exit code of a server that could not start or stopped because of an error.</summary>
    public const int ExitCode = 1;

    /// <summary>The exit code of a server started with a command line it cannot parse.</summary>
    public const int InvalidCommandLineExitCode = 2;

    private Exception? failure;

    /// <summary>The recorded failure, or null when the server started.</summary>
    public Exception? Failure => Volatile.Read(ref failure);

    /// <summary>
    /// Records <paramref name="ex"/> unless a failure is already recorded. The first failure is the cause; a
    /// later one is usually a consequence of the stop that the first one started.
    /// </summary>
    public void Record(Exception ex) => Interlocked.CompareExchange(ref failure, ex, null);
}
