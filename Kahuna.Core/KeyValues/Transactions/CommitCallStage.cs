namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The consecutive parts of one commit call; the <c>stage</c> tag of <see cref="CommitCallTelemetry"/>. A call is
/// in exactly one stage at any moment, so the stages of a call add up to the call. A stage that ends early, by a
/// refusal or an exception, keeps the time it took.
/// </summary>
internal enum CommitCallStage
{
    /// <summary>From the entry of the call on this node to the coordinator: the check for the session's owner and,
    /// for a session this node does not hold, the leadership check, the probe of the peers and the forward.</summary>
    Route,

    /// <summary>The session lookup and the finalize slot. A call that does not own the finalize spends its time
    /// here: it waits for the owner's outcome, or reads the retained outcome or the durable record.</summary>
    Session,

    /// <summary>The wait for the operations of the session that are still in flight. After it the working set is
    /// frozen.</summary>
    Drain,

    /// <summary>The checks before the commit, the pin of a yielding transaction's intents, the split of the working
    /// set, the commit timestamp, the build of the finalize input, the admission gates and the one-phase
    /// eligibility.</summary>
    Freeze,

    /// <summary>The staged-base validation before the first proposal.</summary>
    Preflight,

    /// <summary>The serialization of the record init and of the prepares, and the one-phase gate.</summary>
    Encode,

    /// <summary>The one-phase attempt, from its entry to the reply of its bundle or to its exit before the bundle.
    /// It covers the same span as <see cref="DurableTransactionMetrics.OnePhasePreBundleMs"/> plus
    /// <see cref="DurableTransactionMetrics.OnePhaseBundleMs"/>.</summary>
    Attempt,

    /// <summary>After the reply of the bundle: the verdict, and the apply of the committed values on the leader.</summary>
    Apply,

    /// <summary>The two-phase claim of the staged intents, the prepare barrier and the prepare retries.</summary>
    Prepare,

    /// <summary>The commit-time checks: the validate stage of the two-phase path, the read checks of a read-only
    /// transaction, and the read-set validation of an ephemeral one. The one-phase path runs its checks inside
    /// <see cref="Attempt"/>.</summary>
    Validate,

    /// <summary>A terminal transition of the record on its own round: the two-phase decision, an abort driven
    /// through the record, or the fence of an earlier attempt.</summary>
    Decision,

    /// <summary>The hand-off of the resolution to the background, or the whole resolution when deferred settlement
    /// is off.</summary>
    Resolution,

    /// <summary>The prepare, conflict probe, commit and rollback of ephemeral keys, all in memory.</summary>
    Ephemeral,

    /// <summary>The release of the transaction's locks and read snapshots. A session with
    /// <c>AsyncRelease</c> leaves the part after the first wait out of the call.</summary>
    Release,

    /// <summary>The outcome on the session, the retained outcome, the end of the session and the publication to the
    /// callers that wait for it.</summary>
    Answer
}

/// <summary>How a commit call was served; the <c>path</c> tag of <see cref="CommitCallTelemetry"/>.</summary>
internal enum CommitCallPath
{
    /// <summary>The call ended before a path was chosen: a drain that timed out, a refusal by a check or an
    /// admission gate, a fence of an earlier attempt, or a session that is being reaped.</summary>
    None,

    /// <summary>The one-phase attempt gave the outcome.</summary>
    OnePhase,

    /// <summary>The two-phase flow gave the outcome, with or without a one-phase attempt before it.</summary>
    TwoPhase,

    /// <summary>Every written key is ephemeral: the two-phase commit in memory.</summary>
    Ephemeral,

    /// <summary>The transaction wrote nothing.</summary>
    ReadOnly,

    /// <summary>Another call owned the finalize and this one answered its outcome.</summary>
    Mirrored,

    /// <summary>The session was gone: the answer came from the retained outcome or the durable record.</summary>
    Replayed,

    /// <summary>Another node served the call. That node records it too, under the path it took there.</summary>
    Forwarded
}
