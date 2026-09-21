
using System.Runtime.ExceptionServices;

using Kommander.Time;

using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Raised inside an actor turn when the script turns out to need something the turn cannot give it — a key
/// other than the one the turn was started for. The turn stops, releases what it holds, and the script is run
/// again from the start on the general path, with a fresh transaction. Nothing the turn staged was committed,
/// so the second run is the only one anybody observes.
/// </summary>
internal sealed class ActorTurnEscapeException : Exception
{
    public ActorTurnEscapeException(string reason) : base(reason)
    {
    }
}

/// <summary>
/// The requests a finalize sends when it runs inside an actor turn. Inside a turn every request for the key
/// must be served by the running actor itself; one sent through the mailbox would wait for a turn that cannot
/// start until this one ends.
/// </summary>
internal interface IActorTurnFinalizer
{
    Task<(KeyValueResponseType, KeyValueFinalizeStage)> TryFinalize(HLCTimestamp transactionId, HLCTimestamp commitId, string key, string? recordAnchorKey);

    Task<KeyValueResponseType> TryRollback(HLCTimestamp transactionId, string key);
}

/// <summary>
/// Runs a whole script transaction over one ephemeral key inside a single turn of the actor that owns the key.
///
/// <para>The turn issues exactly the requests the general path issues, to exactly the same handlers, in the
/// same order: acquire the key's exclusive lock, run the script's reads and writes, finalize, release. The
/// only difference is how each request travels. On the general path each one is a mailbox round trip with a
/// routing step in front of it; here each one is a direct call, because the actor is already ours. So the
/// turn adds no rule of its own about what a script may observe or when a transaction conflicts — and because
/// the actor is single threaded, nothing can interleave with the transaction while the turn runs.</para>
///
/// <para>The script's statements reach the turn through <see cref="ScriptTransactionContext.ActorTurn"/>. A
/// statement that names any other key, or any other durability, ends the turn with
/// <see cref="ActorTurnEscapeException"/>; static analysis picks the scripts worth trying, this check is what
/// makes a wrong pick harmless.</para>
///
/// <para>Every outcome of the script — a result, a script error, an abort — is captured and handed back, and
/// the release runs in the turn's <c>finally</c>, so the actor is never left holding this transaction's lock
/// or staged write when the turn ends.</para>
/// </summary>
internal sealed class ScriptActorTurn : IKeyValueActorTurn, IActorTurnFinalizer
{
    private const KeyValueDurability Durability = KeyValueDurability.Ephemeral;

    private readonly KeyValuesManager manager;

    private readonly TransactionCoordinator coordinator;

    private readonly Func<ScriptTransactionContext, NodeAst, CancellationToken, Task> runScript;

    private readonly ScriptTransactionContext context;

    private readonly NodeAst ast;

    private readonly string key;

    private readonly int lockExpiresMs;

    private readonly CancellationToken cancellationToken;

    private IKeyValueInlineDispatcher? actor;

    public ScriptActorTurn(
        KeyValuesManager manager,
        TransactionCoordinator coordinator,
        Func<ScriptTransactionContext, NodeAst, CancellationToken, Task> runScript,
        ScriptTransactionContext context,
        NodeAst ast,
        string key,
        int lockExpiresMs,
        CancellationToken cancellationToken
    )
    {
        this.manager = manager;
        this.coordinator = coordinator;
        this.runScript = runScript;
        this.context = context;
        this.ast = ast;
        this.key = key;
        this.lockExpiresMs = lockExpiresMs;
        this.cancellationToken = cancellationToken;
    }

    /// <summary>True once the actor has started the turn. False means the mailbox never took the message.</summary>
    public bool Started { get; private set; }

    /// <summary>True when the script needed more than the turn could give and must run again on the general path.</summary>
    public bool Escaped { get; private set; }

    /// <summary>What the script or its finalize threw, to be rethrown where the general path would have thrown it.</summary>
    public ExceptionDispatchInfo? Failure { get; private set; }

    public async ValueTask RunAsync(IKeyValueInlineDispatcher actor)
    {
        Started = true;
        this.actor = actor;

        try
        {
            context.LocksAcquired = new(1);

            (KeyValueResponseType acquired, string keyName, KeyValueDurability durability, _) = await AcquireLock();

            if (acquired != KeyValueResponseType.Locked)
                throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);

            context.LocksAcquired.Add((key, Durability));
            context.ActorTurn = this;

            await runScript(context, ast, cancellationToken);

            if (context.Action == KeyValueTransactionAction.Commit)
                await coordinator.FinalizeInActorTurn(context, key, this, cancellationToken);
        }
        catch (ActorTurnEscapeException)
        {
            Escaped = true;
        }
        catch (Exception ex)
        {
            Failure = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            context.ActorTurn = null;

            try
            {
                context.PerKeyWorkingSetReleased = await ReleaseWhatTheGeneralPathWouldRelease();
            }
            catch (Exception)
            {
                // Left false: the ordinary release runs after the turn and tries again through the mailbox.
            }

            this.actor = null;
        }
    }

    /// <summary>
    /// Releases the key unless the commit already did. This is the per-key part of the coordinator's working-set
    /// release, for a working set that is by construction this one key: a committed write was unlocked by the
    /// commit itself; anything else — a lock with no write, a staged write that never committed, a read — is
    /// cleaned by the one exclusive-lock release, which also removes the transaction's staged state.
    /// </summary>
    private async Task<bool> ReleaseWhatTheGeneralPathWouldRelease()
    {
        context.MarkRenewalExcluded();

        bool holdsLock = context.LocksAcquired is { Count: > 0 };
        bool touchedKey = holdsLock || context.ModifiedKeys is { Count: > 0 } || context.ReadKeys is { Count: > 0 };

        if (!touchedKey)
            return true;

        bool committedWrite = context.State == KeyValueTransactionState.Committed
            && context.ModifiedKeys is not null
            && context.ModifiedKeys.Contains((key, Durability));

        if (committedWrite)
            return true;

        Task<(KeyValueResponseType, string)> release;

        KeyValueInlineScope.Current = actor;
        try { release = manager.TryReleaseExclusiveLock(context.TransactionId, key, Durability); }
        finally { KeyValueInlineScope.Current = null; }

        (KeyValueResponseType type, _) = await release;

        return TransactionCoordinator.IsReleaseAcked(type);
    }

    private void RequireOwnKey(string requestedKey, KeyValueDurability durability)
    {
        if (durability != Durability || !string.Equals(requestedKey, key, StringComparison.Ordinal))
            throw new ActorTurnEscapeException("The script touched a key the actor turn does not own");

        if (actor is null)
            throw new ActorTurnEscapeException("The actor turn is over");
    }

    // Each operation below starts the local operation with the inline scope set and clears the scope before it
    // awaits. The local operation rents its request synchronously, which is when the scope is read, so the
    // request is stamped for inline dispatch and nothing that runs later can inherit the scope.

    private Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> AcquireLock()
    {
        KeyValueInlineScope.Current = actor;
        try { return manager.TryAcquireExclusiveLock(context.TransactionId, key, lockExpiresMs, Durability); }
        finally { KeyValueInlineScope.Current = null; }
    }

    public ValueTask<(KeyValueResponseType, long, HLCTimestamp)> TrySet(
        string requestedKey, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability)
    {
        RequireOwnKey(requestedKey, durability);

        KeyValueInlineScope.Current = actor;
        try { return manager.TrySetKeyValue(context.TransactionId, key, value, compareValue, compareRevision, flags, expiresMs, Durability); }
        finally { KeyValueInlineScope.Current = null; }
    }

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGet(string requestedKey, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability)
    {
        RequireOwnKey(requestedKey, durability);

        KeyValueInlineScope.Current = actor;
        try { return manager.TryGetValue(context.TransactionId, key, revision, readTimestamp, Durability); }
        finally { KeyValueInlineScope.Current = null; }
    }

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExists(string requestedKey, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability)
    {
        RequireOwnKey(requestedKey, durability);

        KeyValueInlineScope.Current = actor;
        try { return manager.TryExistsValue(context.TransactionId, key, revision, readTimestamp, Durability); }
        finally { KeyValueInlineScope.Current = null; }
    }

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDelete(string requestedKey, KeyValueDurability durability)
    {
        RequireOwnKey(requestedKey, durability);

        KeyValueInlineScope.Current = actor;
        try { return manager.TryDeleteKeyValue(context.TransactionId, key, Durability); }
        finally { KeyValueInlineScope.Current = null; }
    }

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtend(string requestedKey, int expiresMs, KeyValueDurability durability)
    {
        RequireOwnKey(requestedKey, durability);

        KeyValueInlineScope.Current = actor;
        try { return manager.TryExtendKeyValue(context.TransactionId, key, expiresMs, Durability); }
        finally { KeyValueInlineScope.Current = null; }
    }

    public Task<(KeyValueResponseType, KeyValueFinalizeStage)> TryFinalize(HLCTimestamp transactionId, HLCTimestamp commitId, string requestedKey, string? recordAnchorKey)
    {
        RequireOwnKey(requestedKey, Durability);

        KeyValueInlineScope.Current = actor;
        try { return manager.TryFinalizeMutation(transactionId, commitId, key, Durability, recordAnchorKey); }
        finally { KeyValueInlineScope.Current = null; }
    }

    public async Task<KeyValueResponseType> TryRollback(HLCTimestamp transactionId, string requestedKey)
    {
        RequireOwnKey(requestedKey, Durability);

        Task<(KeyValueResponseType, long)> rollback;

        KeyValueInlineScope.Current = actor;
        try { rollback = manager.TryRollbackMutations(transactionId, key, HLCTimestamp.Zero, Durability); }
        finally { KeyValueInlineScope.Current = null; }

        (KeyValueResponseType type, _) = await rollback;

        return type;
    }
}
