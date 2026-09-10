
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions.Commands;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Operators;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Parses and executes Kahuna script transactions. Composes <see cref="TransactionCoordinator"/>
/// for all 2PC and lock-release operations. AST traversal and script options live exclusively here.
/// </summary>
internal sealed class ScriptTransactionExecutor
{
    private const int ExtraLockingDelay = 10;

    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly TransactionCoordinator coordinator;

    private readonly ScriptParserProcessor scriptParserProcessor;

    /// <summary>Admission gate for script transactions. Separate from the interactive-session gate because a
    /// script transaction holds its slot only for its own bounded execution.</summary>
    private readonly TransactionPriorityOrderer orderer;

    public ScriptTransactionExecutor(
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        IRaft raft,
        ILogger<IKahuna> logger,
        TransactionCoordinator coordinator,
        TransactionPriorityOrderer orderer
    )
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
        this.coordinator = coordinator;
        this.orderer = orderer;
        this.scriptParserProcessor = new(this.configuration, logger);
    }

    /// <summary>
    /// Executes a single or multi-command transaction in an atomic manner.
    /// </summary>
    /// <param name="priority">
    /// Admission priority, honoured only for scripts that actually open a transaction — an explicit
    /// <c>BEGIN</c> block or a multi-statement script, which is what the admission gate governs. A script
    /// consisting of one standalone command (<c>SET</c>, <c>GET</c>, <c>DELETE</c>, <c>EXTEND</c>, a bucket or
    /// prefix read, or their ephemeral forms) runs directly against the store and is deliberately not gated:
    /// it holds no transaction, and putting single-key operations behind a concurrency ceiling would throttle
    /// ordinary reads and writes, which is well outside what this gate is for. Priority is accepted and
    /// ignored for those shapes rather than rejected, so a caller can set it once for a mixed workload.
    /// Note that parsing necessarily happens before admission, since the script is what says whether a
    /// transaction is being opened at all.
    /// </param>
    public async Task<KeyValueTransactionResult> TryExecuteTx(ReadOnlyMemory<byte> script, string? hash, List<KeyValueParameter>? parameters, TransactionPriority priority = TransactionPriority.Normal)
    {
        // A priority can arrive here as a raw number from a REST payload or as a cast enum from the embedded
        // API, neither of which is validated by the gRPC wire conversion. Normalize before it can influence
        // admission, so an out-of-range ordinal cannot be treated as Critical and jump the queue.
        priority = TransactionPriorityOrderer.Normalize(priority);

        // Refused before the parse rather than after it: the tree a long script builds is what the walkers
        // then descend, and a body large enough to build a dangerous tree must never reach the parser.
        if (script.Length > configuration.MaxScriptLength)
            return new()
            {
                Type = KeyValueResponseType.InvalidInput,
                Reason = $"Script is too long: {script.Length} bytes exceeds the limit of {configuration.MaxScriptLength}"
            };

        try
        {
            // Parse synchronously before the first await; the AST owns everything needed
            // afterwards, so the script memory does not have to survive later command awaits.
            NodeAst ast = scriptParserProcessor.Parse(script.Span, hash);

            switch (ast.nodeType)
            {
                case NodeType.Set:
                    return await SetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Get:
                    return await GetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.GetByBucket:
                    return await GetByBucketCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.ScanByPrefix:
                    return await ScanByPrefixCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Exists:
                    return await ExistsCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Delete:
                    return await DeleteCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Extend:
                    return await ExtendCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Eset:
                    return await SetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Eget:
                    return await GetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Eexists:
                    return await ExistsCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Edelete:
                    return await DeleteCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Eextend:
                    return await ExtendCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.EGetByBucket:
                    return await GetByBucketCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.EscanByPrefix:
                    return await ScanByPrefixCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Begin:
                    return await ExecuteTransaction(ast.leftAst!, ast.rightAst, parameters, false, priority);

                case NodeType.StmtList:
                case NodeType.Let:
                case NodeType.NullType:
                case NodeType.IntegerType:
                case NodeType.StringType:
                case NodeType.FloatType:
                case NodeType.BooleanType:
                case NodeType.Identifier:
                case NodeType.If:
                case NodeType.For:
                case NodeType.Equals:
                case NodeType.NotEquals:
                case NodeType.LessThan:
                case NodeType.GreaterThan:
                case NodeType.LessThanEquals:
                case NodeType.GreaterThanEquals:
                case NodeType.And:
                case NodeType.Or:
                case NodeType.Not:
                case NodeType.Negate:
                case NodeType.Add:
                case NodeType.Subtract:
                case NodeType.Mult:
                case NodeType.Div:
                case NodeType.Range:
                case NodeType.ArrayIndex:
                case NodeType.FuncCall:
                case NodeType.ArgumentList:
                case NodeType.NotFound:
                case NodeType.NotSet:
                case NodeType.Return:
                case NodeType.Sleep:
                case NodeType.Throw:
                case NodeType.Placeholder:
                case NodeType.BeginOptionList:
                case NodeType.BeginOption:
                    return await ExecuteTransaction(ast, null, parameters, true, priority);

                case NodeType.Rollback:
                case NodeType.Commit:
                    throw new KahunaScriptException("Invalid transaction", ast.yyline);

                // The set-flag nodes are deliberately absent: they only ever appear inside the flag list of a
                // SET, never as a script root, so they reach the same unknown-command error as anything else
                // the grammar cannot put here.
                default:
                    throw new KahunaScriptException("Unknown command: " + ast.nodeType, ast.yyline);
            }
        }
        catch (KahunaScriptException ex)
        {
            logger.LogKahunaScriptException(ex);

            return new() { Type = KeyValueResponseType.Errored, Reason = DescribeScriptError(ex) };
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogKahunaAbortedException(ex);

            return new() { Type = KeyValueResponseType.Aborted, Reason = ex.Message };
        }
        catch (TaskCanceledException ex)
        {
            logger.LogTaskCanceledException(ex);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (OperationCanceledException ex)
        {
            logger.LogTaskCanceledException(ex);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (Exception ex)
        {
            logger.LogTryExecuteTxError(ex);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message };
        }
    }

    /// <summary>
    /// Orders a lock set for acquisition. Ordinal, matching the ordering used for keys and range bounds
    /// everywhere else in the store.
    /// </summary>
    private static List<string> SortedOrdinal(HashSet<string> keys)
    {
        List<string> sorted = new(keys);

        sorted.Sort(StringComparer.Ordinal);

        return sorted;
    }

    /// <summary>
    /// Formats a script error for the client. A parse error already carries "at line X, column Y near 'tok'",
    /// built where the token is still known, so the location is appended here only for the runtime errors that
    /// carry a line alone. Appending unconditionally produced messages that named the same line twice.
    /// </summary>
    private static string DescribeScriptError(KahunaScriptException ex)
    {
        if (ex.Column > 0 || ex.Line <= 0)
            return ex.Message;

        return ex.Message + " at line " + ex.Line;
    }

    /// <summary>
    /// Returns a temporary script transaction context for executing a single non-transactional command.
    /// </summary>
    private static ScriptTransactionContext GetTempTransactionContext(List<KeyValueParameter>? parameters)
    {
        return new()
        {
            TransactionId = HLCTimestamp.Zero,
            Locking = KeyValueTransactionLocking.Pessimistic,
            Action = KeyValueTransactionAction.Commit,
            AsyncRelease = true,
            Parameters = parameters
        };
    }

    /// <summary>
    /// Executes a script transaction using the two-phase commit protocol.
    /// The autoCommit flag selects automatic commit on success vs. explicit commit/rollback.
    /// </summary>
    private async Task<KeyValueTransactionResult> ExecuteTransaction(NodeAst ast, NodeAst? optionsAst, List<KeyValueParameter>? parameters, bool autoCommit, TransactionPriority priority)
    {
        bool asyncRelease = false;
        int timeout = configuration.DefaultTransactionTimeout;
        int? admissionWaitMs = null;
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic;
        HLCTimestamp readTimestamp = HLCTimestamp.Zero;

        if (optionsAst?.nodeType is NodeType.BeginOptionList or NodeType.BeginOption)
        {
            Dictionary<string, string> options = new();

            GetTransactionOptions(optionsAst, options);

            if (options.TryGetValue("locking", out string? optionValue))
            {
                locking = optionValue switch
                {
                    "pessimistic" => KeyValueTransactionLocking.Pessimistic,
                    "optimistic" => KeyValueTransactionLocking.Optimistic,
                    _ => throw new KahunaScriptException("Unsupported locking option: " + optionValue, optionsAst.yyline)
                };
            }

            if (options.TryGetValue("autoCommit", out optionValue))
            {
                autoCommit = optionValue switch
                {
                    "true" => true,
                    "false" => false,
                    "yes" => true,
                    "no" => false,
                    _ => throw new KahunaScriptException("Unsupported autoCommit option: " + optionValue, optionsAst.yyline)
                };
            }

            if (options.TryGetValue("asyncRelease", out optionValue))
            {
                asyncRelease = optionValue switch
                {
                    "true" => true,
                    "false" => false,
                    "yes" => true,
                    "no" => false,
                    _ => throw new KahunaScriptException("Unsupported asyncRelease option: " + optionValue, optionsAst.yyline)
                };
            }

            if (options.TryGetValue("timeout", out optionValue))
            {
                if (!int.TryParse(optionValue, out timeout))
                    throw new KahunaScriptException("Invalid timeout option: " + optionValue, optionsAst.yyline);

                // Zero is refused rather than read as "no limit". A transaction holds locks and an admission
                // slot for as long as it runs, so an unbounded script is a way to stall a node, and the
                // deadline is the only thing that ends one that never completes. A caller who wants a very
                // long transaction must say how long.
                if (timeout <= 0)
                    throw new KahunaScriptException("timeout must be greater than zero: " + optionValue, optionsAst.yyline);
            }

            // How long this script will queue for an admission slot, as distinct from the timeout above, which
            // is how long it may run once started. A script that expects to do a lot of work is not thereby
            // willing to wait a long time for its turn.
            if (options.TryGetValue("admissionWait", out optionValue))
            {
                if (!int.TryParse(optionValue, out int parsedAdmissionWaitMs))
                    throw new KahunaScriptException("Invalid admissionWait option: " + optionValue, optionsAst.yyline);

                // An option value is a literal and a sign is an operator, so the grammar cannot express a
                // negative budget today. The check stands so that a later grammar change cannot turn one
                // into a silent "do not wait" through the clamp below.
                if (parsedAdmissionWaitMs < 0)
                    throw new KahunaScriptException("admissionWait cannot be negative: " + optionValue, optionsAst.yyline);

                admissionWaitMs = parsedAdmissionWaitMs;
            }

            if (options.TryGetValue("snapshot", out optionValue))
            {
                if (!long.TryParse(optionValue, out long snapshotMs) || snapshotMs == 0)
                    throw new KahunaScriptException("snapshot must be a non-zero Unix epoch millisecond value", optionsAst.yyline);
                readTimestamp = new HLCTimestamp(0, snapshotMs, uint.MaxValue);
            }

            // An inline priority overrides whatever the transport carried, so a script can express its own
            // importance without the caller having to set it out of band.
            if (options.TryGetValue("priority", out optionValue))
            {
                priority = optionValue switch
                {
                    "background" => TransactionPriority.Background,
                    "low" => TransactionPriority.Low,
                    "normal" => TransactionPriority.Normal,
                    "high" => TransactionPriority.High,
                    "critical" => TransactionPriority.Critical,
                    _ => throw new KahunaScriptException("Unsupported priority option: " + optionValue, optionsAst.yyline)
                };
            }
        }

        // The door-wait, deliberately separate from the execution timeout below. Clamped so no script can hold
        // a queue slot longer than the operator allows.
        // An explicit zero means "run only if a slot is free right now" and must be distinguishable from an
        // absent option, which takes the operator's default. Collapsing the two would make it impossible for a
        // latency-sensitive caller to opt out of queueing at all.
        int admissionWait = Math.Min(
            admissionWaitMs ?? configuration.DefaultAdmissionWaitMs,
            configuration.MaxAdmissionWaitMs);

        // Wait for a slot before minting the transaction's identity. A transaction that queued behind a
        // saturated node must carry the HLC of when it actually started, not of when it was submitted, or its
        // reads would be anchored to a snapshot taken before it ran. Below the ceiling this completes
        // synchronously and costs nothing.
        AdmissionLease? lease;

        using (CancellationTokenSource admissionCts = new())
        {
            if (admissionWait > 0)
                admissionCts.CancelAfter(TimeSpan.FromMilliseconds(admissionWait));

            // A zero budget cannot be expressed as a zero-millisecond deadline: the orderer refuses an
            // already-cancelled token before it looks for a free slot, so a timer that happens to fire first
            // would turn "do not queue" into "do not run". Admission instead completes synchronously exactly
            // when it did not have to queue, so cancelling an incomplete result abandons the waiter without
            // ever waiting on it.
            ValueTask<AdmissionLease?> admission = orderer.AdmitAsync(priority, admissionCts.Token);

            if (admissionWait == 0 && !admission.IsCompleted)
                await admissionCts.CancelAsync().ConfigureAwait(false);

            try
            {
                lease = await admission.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The admission budget expired while queued. Nothing was minted, locked, or written, so this is
                // not an aborted transaction — it is a transaction that never started, and the node is shedding
                // load, so the caller should back off rather than resubmit immediately.
                return new() { Type = KeyValueResponseType.AdmissionRefused, Reason = "Timed out waiting for an admission slot" };
            }
        }

        // Refused outright because the admission queue is already full. Same contract as a budget expiry:
        // nothing was started, so resubmitting is safe, but it should follow a back-off.
        if (lease is null)
            return new() { Type = KeyValueResponseType.AdmissionRefused, Reason = "Node is at its transaction admission limit" };

        // The execution deadline starts here rather than at submission, so a script that queued behind a
        // saturated node still gets the full time it asked for to do its work. It is measured from the same
        // point as the transaction's identity below, for the same reason.
        using CancellationTokenSource cts = new();

        cts.CancelAfter(TimeSpan.FromMilliseconds(timeout));

        HLCTimestamp transactionId = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId());

        ScriptTransactionContext context = new()
        {
            TransactionId = transactionId,
            Priority = priority,
            Locking = locking,
            ReadTimestamp = readTimestamp,
            Action = autoCommit ? KeyValueTransactionAction.Commit : KeyValueTransactionAction.Abort,
            AsyncRelease = asyncRelease,
            Result = new() { Type = KeyValueResponseType.Aborted },
            Parameters = parameters
        };

        HashSet<string> ephemeralLocksToAcquire = [];
        HashSet<string> persistentLocksToAcquire = [];
        HashSet<string> ephemeralPrefixLocksToAcquire = [];
        HashSet<string> persistentPrefixLocksToAcquire = [];

        try
        {
            // Inside the try so that a malformed script surfacing here still runs the finally that returns
            // the admission slot — a slot lost to an early throw would shrink node capacity permanently.
            if (locking == KeyValueTransactionLocking.Pessimistic)
                KeyValueLockHelper.GetLocksToAcquire(
                    context,
                    ast,
                    ephemeralLocksToAcquire,
                    persistentLocksToAcquire,
                    ephemeralPrefixLocksToAcquire,
                    persistentPrefixLocksToAcquire
                );

            if (locking == KeyValueTransactionLocking.Pessimistic)
                await AcquireLocksPessimistically(
                    context,
                    ephemeralLocksToAcquire,
                    persistentLocksToAcquire,
                    ephemeralPrefixLocksToAcquire,
                    persistentPrefixLocksToAcquire,
                    timeout,
                    cts.Token
                );

            await ExecuteTransactionInternal(context, ast, cts.Token);

            if (context.Action == KeyValueTransactionAction.Commit)
            {
                await coordinator.TwoPhaseCommit(context, cts.Token);

                if (context.Result?.Type == KeyValueResponseType.Aborted)
                    return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted" };

                return context.Result ?? new() { Type = KeyValueResponseType.Errored };
            }

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted" };
        }
        catch (KahunaScriptException ex)
        {
            logger.LogKahunaScriptException(ex);

            return new() { Type = KeyValueResponseType.Errored, Reason = DescribeScriptError(ex) };
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogKahunaAbortedException(ex);

            return new() { Type = KeyValueResponseType.Aborted, Reason = ex.Message };
        }
        catch (TaskCanceledException ex)
        {
            logger.LogTaskCanceledException(ex);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (OperationCanceledException ex)
        {
            logger.LogOperationCanceledException(ex);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (Exception ex)
        {
            logger.LogTryExecuteTxError(ex);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.GetType().Name + ": " + ex.Message };
        }
        finally
        {
            // First, and unconditionally: returning the slot must not sit behind anything that can throw,
            // or a failure here would cost the node a slot for the rest of its life.
            lease.Dispose();

            // Release every confirmed lock shape not finalized by two-phase commit and clean the
            // transaction's read MVCC. Safe to run on a committed transaction: its modified keys were already
            // finalized and are skipped internally. Best-effort — no terminal promise rides on completion.
            if (context.AsyncRelease)
                _ = coordinator.ReleaseWorkingSet(context);
            else
                await coordinator.ReleaseWorkingSet(context);
        }
    }

    /// <summary>
    /// Acquires all locks required by a pessimistic script transaction before execution begins.
    ///
    /// <para>The acquisition order is fixed, because the sets arrive here as hash sets and hash enumeration
    /// order depends on what else the set holds. Two transactions over overlapping keys would otherwise
    /// attempt them in different orders, each take part of the overlap, and both abort — a live holder is
    /// reported immediately, so neither waits, but neither makes progress either. The order is: ephemeral
    /// prefix locks, then persistent prefix locks, then the point locks, each ordinal ascending by key.</para>
    ///
    /// <para>This removes the mutual abort between two transactions whose shared keys land on one leader. It
    /// does not remove it entirely: the point-lock batch fans out to one request per leader in parallel, so
    /// two transactions can still each win a different leader's share. Making that impossible would mean
    /// acquiring leader by leader in sequence, which costs a round trip per partition on every transaction
    /// start, and the failure it would prevent is already a clean immediate abort.</para>
    /// </summary>
    private async Task AcquireLocksPessimistically(
        ScriptTransactionContext context,
        HashSet<string> ephemeralLocksToAcquire,
        HashSet<string> persistentLocksToAcquire,
        HashSet<string> ephemeralPrefixLocksToAcquire,
        HashSet<string> persistentPrefixLocksToAcquire,
        int timeout,
        CancellationToken ctsToken
    )
    {
        int numberLocks = ephemeralPrefixLocksToAcquire.Count + persistentPrefixLocksToAcquire.Count;


        if (numberLocks > 0)
        {
            context.PrefixLocksAcquired = new(numberLocks);

            foreach (string prefixKey in SortedOrdinal(ephemeralPrefixLocksToAcquire))
            {
                KeyValueResponseType acquirePrefixResponse = await manager.LocateAndTryAcquireExclusivePrefixLock(
                    context.TransactionId,
                    prefixKey,
                    timeout + ExtraLockingDelay,
                    KeyValueDurability.Ephemeral,
                    ctsToken
                );

                if (acquirePrefixResponse != KeyValueResponseType.Locked)
                    throw new KahunaAbortedException("Failed to acquire prefix lock: " + prefixKey + " " + KeyValueDurability.Ephemeral);

                context.PrefixLocksAcquired.Add((prefixKey, KeyValueDurability.Ephemeral));
            }

            foreach (string prefixKey in SortedOrdinal(persistentPrefixLocksToAcquire))
            {
                KeyValueResponseType acquirePrefixResponse = await manager.LocateAndTryAcquireExclusivePrefixLock(
                    context.TransactionId,
                    prefixKey,
                    timeout + ExtraLockingDelay,
                    KeyValueDurability.Persistent,
                    ctsToken
                );

                if (acquirePrefixResponse != KeyValueResponseType.Locked)
                    throw new KahunaAbortedException("Failed to acquire prefix lock: " + prefixKey + " " + KeyValueDurability.Persistent);

                context.PrefixLocksAcquired.Add((prefixKey, KeyValueDurability.Persistent));
            }
        }

        numberLocks = ephemeralLocksToAcquire.Count + persistentLocksToAcquire.Count;

        if (numberLocks > 0)
        {
            context.LocksAcquired = new(numberLocks);

            if (numberLocks == 1)
            {
                if (ephemeralLocksToAcquire.Count > 0)
                {
                    (KeyValueResponseType acquireResponse, string keyName, KeyValueDurability durability, _) = await manager.LocateAndTryAcquireExclusiveLock(
                        context.TransactionId,
                        ephemeralLocksToAcquire.First(),
                        timeout + ExtraLockingDelay,
                        KeyValueDurability.Ephemeral,
                        ctsToken
                    );

                    if (acquireResponse != KeyValueResponseType.Locked)
                        throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);

                    context.LocksAcquired.Add((keyName, durability));
                    return;
                }

                if (persistentLocksToAcquire.Count > 0)
                {
                    (KeyValueResponseType acquireResponse, string keyName, KeyValueDurability durability, _) =
                        await manager.LocateAndTryAcquireExclusiveLock(context.TransactionId, persistentLocksToAcquire.First(), timeout + ExtraLockingDelay, KeyValueDurability.Persistent, ctsToken);

                    if (acquireResponse != KeyValueResponseType.Locked)
                        throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);

                    context.LocksAcquired.Add((keyName, durability));
                    return;
                }
            }

            List<(string Key, int ExpiresMs, KeyValueDurability Durability)> keysToLock = new(numberLocks);

            foreach (string key in ephemeralLocksToAcquire)
                keysToLock.Add((key, timeout + ExtraLockingDelay, KeyValueDurability.Ephemeral));

            foreach (string key in persistentLocksToAcquire)
                keysToLock.Add((key, timeout + ExtraLockingDelay, KeyValueDurability.Persistent));

            // Sorted for the reason described on the acquisition-order comment above: two transactions that
            // reach the same leader must attempt their shared keys in the same relative order, and the source
            // sets are hash sets whose enumeration order depends on what else they contain.
            keysToLock.Sort(static (left, right) =>
            {
                int byKey = string.CompareOrdinal(left.Key, right.Key);

                return byKey != 0 ? byKey : left.Durability.CompareTo(right.Durability);
            });

            List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> lockResponses = await manager.LocateAndTryAcquireManyExclusiveLocks(context.TransactionId, keysToLock, ctsToken);

            foreach ((KeyValueResponseType response, string keyName, KeyValueDurability durability, _) in lockResponses)
            {
                if (response == KeyValueResponseType.Locked)
                    context.LocksAcquired.Add((keyName, durability));
            }

            foreach ((KeyValueResponseType response, string keyName, KeyValueDurability durability, _) in lockResponses)
            {
                if (response != KeyValueResponseType.Locked)
                    throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);
            }
        }
    }

    /// <summary>
    /// Reads all transaction BEGIN options into a dictionary.
    /// </summary>
    private static void GetTransactionOptions(NodeAst ast, Dictionary<string, string> options)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.BeginOptionList:
                {
                    if (ast.leftAst is not null)
                        GetTransactionOptions(ast.leftAst, options);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }

                case NodeType.BeginOption:
                    if (ast.leftAst?.yytext is null || ast.rightAst?.yytext is null)
                        throw new KahunaScriptException("Invalid BEGIN option", ast.yyline);

                    // Reject a repeated option rather than letting one value quietly win. Which of the two
                    // the author meant is unknowable, and a silently discarded option is the exact failure
                    // this option list is meant to stop.
                    if (!options.TryAdd(ast.leftAst.yytext, ast.rightAst.yytext))
                        throw new KahunaScriptException("Duplicated BEGIN option: " + ast.leftAst.yytext, ast.yyline);

                    break;
            }

            break;
        }
    }

    /// <summary>
    /// Recursively executes the script AST.
    /// </summary>
    /// <param name="spineProbed">
    /// True when this call descends the left spine of a statement list whose batch probe already
    /// ran at the top. The probe is O(statements) and the spine has one level per statement, so
    /// re-probing every level would make batch detection quadratic in script length.
    /// </param>
    private async Task ExecuteTransactionInternal(ScriptTransactionContext context, NodeAst ast, CancellationToken cancellationToken, bool spineProbed = false)
    {
        if (ast.nodeType == NodeType.StmtList)
        {
            if (!spineProbed)
                ProbeBatchablePrefix(context, ast);

            // The probe stashed the largest batchable prefix subtree; the descent executes nothing
            // until it reaches that exact node, so batching here is equivalent to the per-level
            // detection it replaces.
            if (ReferenceEquals(context.BatchBoundary, ast))
            {
                context.BatchBoundary = null;

                context.Result = context.BatchBoundaryIsSetMany
                    ? await SetManyCommand.Execute(manager, context, ast, cancellationToken)
                    : await DeleteManyCommand.Execute(manager, context, ast, cancellationToken);
                return;
            }
        }

        while (true)
        {
            if (context.Status == KeyValueExecutionStatus.Stop)
                break;

            cancellationToken.ThrowIfCancellationRequested();

            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null)
                        await ExecuteTransactionInternal(context, ast.leftAst, cancellationToken, spineProbed: true);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }

                case NodeType.If:
                    await ExecuteIf(context, ast, cancellationToken);
                    break;

                case NodeType.For:
                    await ExecuteFor(context, ast, cancellationToken);
                    break;

                case NodeType.Let:
                {
                    context.Result = LetCommand.Execute(context, ast);
                    break;
                }

                case NodeType.Set:
                    if (!context.ReadTimestamp.IsNull())
                        throw new KahunaAbortedException("writes are not allowed in a snapshot (AS OF) transaction");
                    context.Result = await SetCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.Delete:
                    if (!context.ReadTimestamp.IsNull())
                        throw new KahunaAbortedException("writes are not allowed in a snapshot (AS OF) transaction");
                    context.Result = await DeleteCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.Extend:
                    if (!context.ReadTimestamp.IsNull())
                        throw new KahunaAbortedException("writes are not allowed in a snapshot (AS OF) transaction");
                    context.Result = await ExtendCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.Get:
                {
                    context.Result = await GetCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                }

                case NodeType.Exists:
                {
                    context.Result = await ExistsCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                }

                case NodeType.Eset:
                    if (!context.ReadTimestamp.IsNull())
                        throw new KahunaAbortedException("writes are not allowed in a snapshot (AS OF) transaction");
                    context.Result = await SetCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                case NodeType.Eget:
                {
                    context.Result = await GetCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;
                }

                case NodeType.Eexists:
                {
                    context.Result = await ExistsCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;
                }

                case NodeType.Edelete:
                    if (!context.ReadTimestamp.IsNull())
                        throw new KahunaAbortedException("writes are not allowed in a snapshot (AS OF) transaction");
                    context.Result = await DeleteCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                case NodeType.Eextend:
                    if (!context.ReadTimestamp.IsNull())
                        throw new KahunaAbortedException("writes are not allowed in a snapshot (AS OF) transaction");
                    context.Result = await ExtendCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                case NodeType.GetByBucket:
                    context.Result = await GetByBucketCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.EGetByBucket:
                    context.Result = await GetByBucketCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                // A prefix scan fans out over every partition and carries no transaction identity, so inside a
                // transaction it would read a snapshot blind to the transaction's own uncommitted writes and
                // would take no prefix lock to make the read repeatable. Refusing is the honest answer until a
                // transaction-carrying scan exists. GET BY BUCKET is the prefix read that does work here.
                case NodeType.ScanByPrefix:
                case NodeType.EscanByPrefix:
                    throw new KahunaScriptException("SCAN BY PREFIX is not supported inside transactions, use GET BY BUCKET", ast.yyline);

                case NodeType.Commit:
                    context.Action = KeyValueTransactionAction.Commit;
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;

                case NodeType.Rollback:
                    context.Action = KeyValueTransactionAction.Abort;
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;

                case NodeType.Return:
                    KeyValueTransactionResult? result = ReturnCommand.Execute(context, ast);
                    if (result is not null)
                        context.Result = result;
                    break;

                case NodeType.Sleep:
                    await SleepCommand.Execute(ast, cancellationToken);
                    break;

                case NodeType.Throw:
                    ThrowCommand.Execute(context, ast, cancellationToken);
                    break;

                case NodeType.Begin:
                    throw new KahunaScriptException("Nested transactions are not supported", ast.yyline);

                case NodeType.IntegerType:
                case NodeType.StringType:
                case NodeType.FloatType:
                case NodeType.BooleanType:
                case NodeType.Identifier:
                case NodeType.Equals:
                case NodeType.NotEquals:
                case NodeType.LessThan:
                case NodeType.GreaterThan:
                case NodeType.LessThanEquals:
                case NodeType.GreaterThanEquals:
                case NodeType.And:
                case NodeType.Or:
                case NodeType.Not:
                case NodeType.Negate:
                case NodeType.Add:
                case NodeType.Subtract:
                case NodeType.Mult:
                case NodeType.Div:
                case NodeType.Range:
                case NodeType.ArrayIndex:
                case NodeType.FuncCall:
                case NodeType.ArgumentList:
                case NodeType.NullType:
                case NodeType.Placeholder:
                    KeyValueExpressionResult evalResult = KeyValueTransactionExpression.Eval(context, ast);
                    context.Result = evalResult.ToTransactionResult();
                    break;

                case NodeType.SetNotExists:
                case NodeType.SetExists:
                case NodeType.SetCmp:
                case NodeType.SetCmpRev:
                    break;

                case NodeType.NotSet:
                case NodeType.NotFound:
                case NodeType.BeginOptionList:
                case NodeType.BeginOption:
                default:
                    throw new KahunaScriptException("Invalid statement: " + ast.nodeType, ast.yyline);
            }

            break;
        }
    }

    private async Task ExecuteIf(ScriptTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid IF expression", ast.yyline);

        // The condition must be a boolean, the same rule the logical operators follow. A number or a string
        // here used to take the ELSE branch without a word, so a mistyped guard read as a working one.
        if (BooleanOperand.Require(context, ast.leftAst, ast, "IF"))
        {
            if (ast.rightAst is not null)
                await ExecuteTransactionInternal(context, ast.rightAst, cancellationToken);

            return;
        }

        if (ast.extendedOne is not null)
            await ExecuteTransactionInternal(context, ast.extendedOne, cancellationToken);
    }

    private async Task ExecuteFor(ScriptTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid FOR variable", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid FOR expression", ast.yyline);

        KeyValueExpressionResult expressionResult = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        if (expressionResult.Type != KeyValueExpressionType.ArrayType || expressionResult.ArrayValue is null)
            throw new KahunaScriptException("FOR expression is not iterable", ast.yyline);

        foreach (KeyValueExpressionResult iter in expressionResult.ArrayValue)
        {
            context.SetVariable(ast.leftAst, ast.leftAst.yytext!, iter);

            if (ast.extendedOne is not null)
                await ExecuteTransactionInternal(context, ast.extendedOne, cancellationToken);
        }
    }

    /// <summary>
    /// Locates the largest batchable prefix of a statement list in one pass and stashes its
    /// subtree on the context. Statements execute in left-spine order, so the subtree covering
    /// statements [0..k-1] is the spine node at depth n-k; when the first k statements are
    /// homogeneous set/eset (or delete/edelete) commands over distinct keys, that node runs as a
    /// single set-many/delete-many. Probing once at the spine's top replaces re-scanning the
    /// prefix at every recursion level, which was quadratic in script length. The descent
    /// executes nothing before reaching the boundary, so context state (variables, parameters)
    /// at probe time matches what per-level detection observed.
    /// </summary>
    private static void ProbeBatchablePrefix(ScriptTransactionContext context, NodeAst ast)
    {
        context.BatchBoundary = null;

        // Collect the left spine top-down: spine[i] covers statements [0 .. (n-1) - i], where
        // n = spine.Count + 1 statements. The deepest left leaf is statement 0.
        List<NodeAst> spine = [];

        NodeAst node = ast;
        while (node.nodeType == NodeType.StmtList)
        {
            spine.Add(node);

            if (node.leftAst is null)
                return;

            node = node.leftAst;
        }

        // The first statement fixes the batch kind; anything else is not batchable.
        bool isSetMany;
        switch (node.nodeType)
        {
            case NodeType.Set or NodeType.Eset:
                isSetMany = true;
                break;

            case NodeType.Delete or NodeType.Edelete:
                isSetMany = false;
                break;

            default:
                return;
        }

        int n = spine.Count + 1;
        HashSet<(string, KeyValueDurability)> keys = [];
        int k = 0;

        // Statements in execution order: the deepest left leaf, then each spine node's right
        // statement bottom-up. Stop at the first statement that breaks the batch shape.
        for (int i = 0; i < n; i++)
        {
            NodeAst? stmt = i == 0 ? node : spine[n - 1 - i].rightAst;

            if (stmt is null || !IsBatchableStatement(context, stmt, isSetMany, keys))
                break;

            k++;
        }

        // A batch needs at least two statements; the subtree covering [0..k-1] is spine[n - k].
        if (k < 2)
            return;

        context.BatchBoundary = spine[n - k];
        context.BatchBoundaryIsSetMany = isSetMany;
    }

    /// <summary>
    /// True when <paramref name="stmt"/> fits the batch being probed: a set/eset (or
    /// delete/edelete) over a key not already claimed by an earlier statement of the batch.
    /// Mirrors the per-statement rules of the previous per-level detectors, including delete's
    /// parameter resolution (an unresolvable key simply ends the batchable prefix).
    /// </summary>
    private static bool IsBatchableStatement(
        ScriptTransactionContext context,
        NodeAst stmt,
        bool isSetMany,
        HashSet<(string, KeyValueDurability)> keys)
    {
        KeyValueDurability durability;

        if (isSetMany)
        {
            if (stmt.nodeType is not (NodeType.Set or NodeType.Eset))
                return false;

            durability = stmt.nodeType == NodeType.Set
                ? KeyValueDurability.Persistent
                : KeyValueDurability.Ephemeral;
        }
        else
        {
            if (stmt.nodeType is not (NodeType.Delete or NodeType.Edelete))
                return false;

            durability = stmt.nodeType == NodeType.Delete
                ? KeyValueDurability.Persistent
                : KeyValueDurability.Ephemeral;
        }

        if (stmt.leftAst is null)
            return false;

        // The key is resolved, not read as written. Two placeholders that resolve to the same key are one
        // key, and batching them applies both writes together instead of in statement order, which commits a
        // different revision than the same script run one statement at a time.
        string keyName;

        try
        {
            keyName = BaseCommand.GetKeyName(context, stmt.leftAst);
        }
        catch (KahunaScriptException)
        {
            // An unresolvable key simply ends the batchable run; the statement itself reports the error when
            // it executes.
            return false;
        }

        return keys.Add((keyName, durability));
    }

}
