
namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Represents the deferred stage-3 logic for a resumable backend read.
///
/// The resumable-read mechanism decomposes every persistent cache miss into three phases:
///   Stage 1 (actor thread, sync) — in-memory pre-checks; produces a ReadContinuation when
///            a disk round-trip is required and defers the reply via the Promise.
///   Stage 2 (scheduler thread, off-actor) — pure backend read; mutates no actor-owned state.
///   Stage 3 (actor thread, via ResumeRead message) — Execute() below; reconciles the disk
///            result against current resident store, updates cache, and resolves all waiters —
///            or, for multi-page scans, dispatches the next page and defers resolution again.
///
/// Concrete subtypes own the per-shape reconciliation rules (point read, by-revision, scan).
/// All actor-owned state mutations (InsertStoreEntry, TouchEntry, byte accounting) happen
/// inside Execute — never in stage 2.
///
/// Single-flight coalescing: when N requests for the same key arrive while a backend read is
/// already in flight, each additional caller's Promise is attached via AddWaiter. Execute
/// resolves every attached Promise with the same result, so exactly one backend read serves
/// all concurrent callers.
/// </summary>
internal abstract class ReadContinuation
{
    // Primary promise plus any coalesced waiters. Mutated only on the actor thread (stage 1
    // adds waiters, stage 3 broadcasts). No synchronisation needed.
    private readonly List<TaskCompletionSource<KeyValueResponse?>> waiters;

    /// <summary>The primary caller's completion source (the one that initiated the read).</summary>
    internal TaskCompletionSource<KeyValueResponse?> Promise => waiters[0];

    /// <summary>
    /// The raw result from the backend read (stage 2). Set by the stage-2 callback before
    /// the ResumeRead message is dispatched; null means the key was not found on disk.
    /// </summary>
    internal KeyValueEntry? DiskResult { get; set; }

    /// <summary>
    /// The raw result list from a scan backend read (stage 2). Used by bucket and prefix-from-disk
    /// scan continuations instead of DiskResult (which holds a single KeyValueEntry).
    /// Set by the stage-2 callback before the ResumeRead message is dispatched.
    /// </summary>
    internal List<(string, ReadOnlyKeyValueEntry)>? ScanDiskResult { get; set; }

    /// <summary>
    /// True when the backend read failed (faulted or was cancelled). Set by the stage-2
    /// callback before the ResumeRead message is dispatched; read by Execute on the actor thread
    /// (stage 3). The Send() establishes the happens-before edge that makes this visible.
    /// </summary>
    internal bool Faulted { get; private set; }

    /// <summary>Marks this continuation as faulted. Call only from the stage-2 callback.</summary>
    internal void SetFaulted() => Faulted = true;

    protected ReadContinuation(TaskCompletionSource<KeyValueResponse?> promise)
    {
        waiters = new(1) { promise };
    }

    /// <summary>
    /// Attaches a coalesced caller to this in-flight read. The caller's Promise will be
    /// resolved with the same result as the primary at stage-3 resume time.
    /// Must be called only on the actor thread (stage 1).
    /// </summary>
    internal void AddWaiter(TaskCompletionSource<KeyValueResponse?> promise)
    {
        waiters.Add(promise);
    }

    /// <summary>
    /// Resolves every attached Promise (primary + coalesced waiters) with the given response.
    /// Must be called only on the actor thread (stage 1 catch paths or stage 3).
    /// </summary>
    internal void Resolve(KeyValueResponse? response)
    {
        foreach (TaskCompletionSource<KeyValueResponse?> w in waiters)
            w.TrySetResult(response);
    }

    /// <summary>
    /// Removes this continuation's single-flight registration from the actor's in-flight read
    /// map, if it registered one. The default is a no-op (used by continuations that never
    /// coalesce, e.g. range scans); coalescing subtypes override it to remove their own key.
    /// Idempotent: removing an already-removed or never-registered key is safe.
    /// </summary>
    internal virtual void RemovePendingKey(KeyValueContext context) { }

    /// <summary>
    /// Terminal failure path for stage 3. Removes any in-flight registration and resolves every
    /// waiter with the given retryable/errored response. Invoked by the ResumeRead handler when
    /// Execute throws, so a continuation bug can never strand coalesced callers or leak an
    /// in-flight entry that later arrivals would attach to. Idempotent with respect to both
    /// RemovePendingKey and Resolve (TrySetResult).
    /// </summary>
    internal void Fail(KeyValueContext context, KeyValueResponse? response)
    {
        RemovePendingKey(context);
        Resolve(response);
    }

    /// <summary>
    /// Runs on the actor thread (stage 3). Reconciles DiskResult against the current
    /// resident store, updates actor-owned state, and resolves all Promises via Resolve().
    /// For multi-page scans, may instead dispatch the next backend page (stage 2) and defer
    /// resolution; in that case Resolve is called only after the final page's Execute returns.
    /// </summary>
    internal abstract void Execute(KeyValueContext context);
}
