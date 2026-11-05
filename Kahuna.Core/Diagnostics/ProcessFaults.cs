using System.Runtime.ExceptionServices;

namespace Kahuna.Server.Diagnostics;

/// <summary>
/// Process-wide policy for faults a node cannot survive in a well-defined state — today, running out of
/// managed heap.
///
/// <para>Why it exists: every replicated apply path, and the Raft WAL writer underneath it, catches broadly so
/// that one malformed entry cannot stop replication. An <see cref="OutOfMemoryException"/> raised inside one
/// of those catch-alls is logged like a bad entry and the process carries on — with a WAL pipeline that has
/// stopped writing, a queue pinned at its cap, appends refused, and a health endpoint that still answers 200.
/// That replica is a zombie: reachable, counted, and permanently behind. The correct outcome is for the process
/// to die so the orchestrator restarts it and it rejoins by snapshot with an empty heap.</para>
///
/// <para>Two layers. The <b>first-chance policy</b> (<see cref="InstallFirstChancePolicy"/>) observes every
/// exception at the throw site, before any catch runs, so an out-of-memory raised inside a dependency's own
/// catch-all (the WAL writer lives in Kommander) is still seen. The <b>apply-site filter</b>
/// (<see cref="Survivable"/>) is the belt for Kahuna's own catch-alls: it never lets a fatal fault be swallowed,
/// and when fail-fast is disabled it records the fault so the readiness probe can report the node unhealthy
/// instead of leaving it indistinguishable from an idle follower.</para>
/// </summary>
public static class ProcessFaults
{
    private static int firstChanceInstalled;

    private static volatile bool failFastEnabled = true;

    private static string? fatalFaultObserved;

    [ThreadStatic]
    private static bool escalating;

    private static OutOfMemoryExitRecorder? exitRecorder;

    /// <summary>
    /// The recorder that writes one line to the node's out-of-memory exit ledger right before a fail-fast, so
    /// the next start can tell a crash loop in recovery from a one-off. Armed by
    /// <see cref="OutOfMemoryRestartBreaker"/> once the node knows its storage path; null before that and for
    /// nodes without durable storage. Process-wide, like the policy itself.
    /// </summary>
    internal static OutOfMemoryExitRecorder? ExitRecorder
    {
        get => Volatile.Read(ref exitRecorder);
        set
        {
            // A process hosts one node in production; a test host that builds several arms the recorder once
            // per node, and the previous one releases its handle rather than leaking it.
            OutOfMemoryExitRecorder? previous = Interlocked.Exchange(ref exitRecorder, value);
            if (previous is not null && !ReferenceEquals(previous, value))
                previous.Dispose();
        }
    }

    /// <summary>Whether a fatal fault terminates the process (default) or is only recorded for the readiness
    /// probe. Set from <c>EmbeddedKahunaOptions.FailFastOnOutOfMemory</c> at node construction.</summary>
    public static bool FailFastEnabled
    {
        get => failFastEnabled;
        set => failFastEnabled = value;
    }

    /// <summary>A description of the first fatal fault observed while fail-fast was disabled, or null while none
    /// was. Once set it never clears: a node that ran out of heap once cannot vouch for its own state.</summary>
    public static string? FatalFaultObserved => Volatile.Read(ref fatalFaultObserved);

    /// <summary>Whether <paramref name="exception"/> is a fault the process must not try to survive.</summary>
    public static bool IsFatal(Exception exception) => exception is OutOfMemoryException;

    /// <summary>
    /// Installs the process-wide first-chance observer. Idempotent; installed once per process by the first
    /// node constructed in it. Observing at first chance is what makes an out-of-memory raised inside a
    /// dependency's catch-all (the WAL write path) fatal rather than a logged, swallowed line.
    /// </summary>
    public static void InstallFirstChancePolicy()
    {
        if (Interlocked.Exchange(ref firstChanceInstalled, 1) == 1)
            return;

        AppDomain.CurrentDomain.FirstChanceException += OnFirstChanceException;
    }

    private static void OnFirstChanceException(object? sender, FirstChanceExceptionEventArgs e)
    {
        if (!IsFatal(e.Exception) || escalating)
            return;

        // Re-entrancy guard: the escalation itself allocates (a message), and under real heap exhaustion that
        // allocation can throw again on this thread, which would re-enter this handler recursively.
        escalating = true;
        try
        {
            Escalate(e.Exception, "first-chance exception");
        }
        finally
        {
            escalating = false;
        }
    }

    /// <summary>
    /// Exception-filter helper for a catch-all on an apply or replication path: <c>catch (Exception ex) when
    /// (ProcessFaults.Survivable(ex, "site"))</c>. Returns true for an ordinary exception so the catch handles
    /// it as before. For a fatal one it terminates the process when fail-fast is enabled (never returning), and
    /// otherwise records the fault and returns false so the exception propagates instead of being swallowed.
    /// Running inside the filter, it fires before the stack unwinds, so a crash dump keeps the throw site.
    /// </summary>
    public static bool Survivable(Exception exception, string site)
    {
        if (!IsFatal(exception))
            return true;

        Escalate(exception, site);
        return false;
    }

    /// <summary>The pure decision behind <see cref="Survivable"/>, for tests that must not throw a real
    /// <see cref="OutOfMemoryException"/> inside a process with the first-chance policy installed.</summary>
    internal static ProcessFaultDisposition Classify(Exception exception, bool failFast)
    {
        if (!IsFatal(exception))
            return ProcessFaultDisposition.Survivable;

        return failFast ? ProcessFaultDisposition.FailFast : ProcessFaultDisposition.PropagateAndMarkUnhealthy;
    }

    private static void Escalate(Exception exception, string site)
    {
        if (failFastEnabled)
        {
            // The exit is recorded before the process dies, with no allocation on the way (the heap is what
            // just ran out). The recorder's own failure is irrelevant here — the fail-fast follows either way —
            // and the thread-static guard keeps an out-of-memory raised inside the recorder from re-entering
            // this policy through the first-chance observer.
            bool wasEscalating = escalating;
            escalating = true;
            try
            {
                Volatile.Read(ref exitRecorder)?.RecordExit(site);
            }
            catch (Exception)
            {
                // Best effort only.
            }
            finally
            {
                escalating = wasEscalating;
            }

            Environment.FailFast(
                $"Kahuna: {exception.GetType().Name} at {site}. Terminating so the orchestrator restarts this node with an empty heap instead of leaving a reachable replica whose WAL/apply pipeline is dead.",
                exception);
        }

        Interlocked.CompareExchange(
            ref fatalFaultObserved,
            $"{exception.GetType().Name} at {site} ({DateTimeOffset.UtcNow:O})",
            null);
    }

    /// <summary>Test seam: forgets a recorded fault. Never called by production code.</summary>
    internal static void ResetForTests() => Volatile.Write(ref fatalFaultObserved, null);
}

/// <summary>What <see cref="ProcessFaults"/> does with an exception.</summary>
internal enum ProcessFaultDisposition
{
    /// <summary>An ordinary exception; the catch handles it.</summary>
    Survivable = 0,

    /// <summary>A fatal fault with fail-fast enabled: the process terminates.</summary>
    FailFast = 1,

    /// <summary>A fatal fault with fail-fast disabled: it propagates, and the node reports unhealthy.</summary>
    PropagateAndMarkUnhealthy = 2
}
