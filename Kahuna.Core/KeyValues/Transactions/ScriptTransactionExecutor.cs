
using System.Buffers;

using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions.Commands;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Functions;
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

    /// <summary>
    /// Every function scripts on this node can call: the built-ins merged with whatever the host
    /// registered. It is built once here and never changes, which is what lets a call resolve with a
    /// lock-free probe and lets the parsed-script cache stay independent of it.
    /// </summary>
    private readonly ScriptFunctionTable functionTable;

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

        // Freezing the registry here is what makes a later Register throw: a host must finish
        // registering before it builds the node, because a table that changed under a running script
        // would let one transaction see a different language than the next.
        this.functionTable = new(configuration.Functions, raft.GetLocalNodeName(), configuration.FunctionSlowWarnMs, logger);

        if (functionTable.CustomCount > 0)
            logger.LogUserFunctionsRegistered(functionTable.CustomCount, functionTable.NodeName, functionTable.Fingerprint);
    }

    /// <summary>The node's function table, for the metrics surface and for diagnostics.</summary>
    internal ScriptFunctionTable FunctionTable => functionTable;

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
    /// The outcome of a script that reached its end without committing.
    ///
    /// <para>A statement whose response stopped the script reports that response as the script's own outcome.
    /// Nothing durable happened: the script never reached two-phase commit, and the working set is released on
    /// the way out. So a retryable answer (a leader change, a foreign intent still settling, a fenced route)
    /// stays <see cref="KeyValueResponseType.MustRetry"/> and the client layer retries it, as the outcome
    /// contract requires; a conflict stays <see cref="KeyValueResponseType.Aborted"/>; a malformed statement
    /// stays <see cref="KeyValueResponseType.Errored"/>. Flattening all three into a generic abort told clients
    /// to restart transactions that a retry would have completed, and left no trace of which key refused.</para>
    ///
    /// <para>A script that ended by its own control flow (ROLLBACK, RETURN, or no COMMIT) aborted by design and
    /// keeps the plain reason.</para>
    /// </summary>
    private static KeyValueTransactionResult UncommittedResult(ScriptTransactionContext context)
    {
        if (context.StatementFailure is not { } failure)
            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted" };

        return new() { Type = failure.Type, Reason = failure.Describe() };
    }

    /// <summary>
    /// Copies a lock set into <paramref name="buffer"/> in ordinal order and returns how many keys it
    /// wrote. Ordinal, matching the ordering used for keys and range bounds everywhere else in the store.
    ///
    /// <para>The caller supplies the buffer so the two prefix sets can share one, which is what they did
    /// not do before: each was copied into a list of its own to be sorted, and the list was thrown away
    /// straight after the loop that read it. What acquisition needs is the order, not the container.</para>
    /// </summary>
    private static int SortOrdinalInto(HashSet<string> keys, string[] buffer)
    {
        int count = 0;

        foreach (string key in keys)
            buffer[count++] = key;

        buffer.AsSpan(0, count).Sort(StringComparer.Ordinal);

        return count;
    }

    /// <summary>
    /// The one key in a single-element lock set.
    ///
    /// <para>Reads it through the set's own enumerator. <c>First()</c> cannot see a hash set as a list, so
    /// it reached the enumerator through the interface and boxed it — one allocation on the shape that is
    /// by far the most common, a transaction that locks exactly one key.</para>
    /// </summary>
    private static string OnlyKey(HashSet<string> keys)
    {
        foreach (string key in keys)
            return key;

        // Unreachable: the caller checks the count first. Kept as the same exception First() raised, so an
        // impossible state reports as an internal error rather than as a transaction conflict.
        throw new InvalidOperationException("A lock set counted as non-empty held no key");
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
    ///
    /// <para>It is an instance method because it must attach the node's function table, the same as
    /// the transaction path below. A single command such as <c>SET k acme_f(1)</c> runs here, so a
    /// static context would leave the most common shape of this feature without a table.</para>
    /// </summary>
    private ScriptTransactionContext GetTempTransactionContext(List<KeyValueParameter>? parameters)
    {
        return new()
        {
            TransactionId = HLCTimestamp.Zero,
            Locking = KeyValueTransactionLocking.Pessimistic,
            Action = KeyValueTransactionAction.Commit,
            AsyncRelease = true,
            Parameters = parameters,
            FunctionTable = functionTable,
            HybridLogicalClock = raft.HybridLogicalClock,
            LocalNodeId = raft.GetLocalNodeId()
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
        ReadValidation readValidation = ReadValidation.None;
        DecisionDurability decisionDurability = DecisionDurability.BestEffort;

        if (optionsAst?.nodeType is NodeType.BeginOptionList or NodeType.BeginOption)
        {
            Dictionary<string, string> options = new();

            GetTransactionOptions(optionsAst, options);

            // Reject a name no option answers to. A misspelled or miscased option would otherwise run the
            // transaction on its default, which is the silent failure the duplicate check in GetTransactionOptions also guards
            // against: the script says one thing and the transaction does another.
            foreach (string optionName in options.Keys)
            {
                if (!IsKnownTransactionOption(optionName))
                    throw new KahunaScriptException("Unknown BEGIN option: " + optionName, optionsAst.yyline);
            }

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

            if (options.TryGetValue("readValidation", out optionValue))
            {
                readValidation = optionValue switch
                {
                    "none" => ReadValidation.None,
                    "trackAndValidate" => ReadValidation.TrackAndValidate,
                    _ => throw new KahunaScriptException("Unsupported readValidation option: " + optionValue, optionsAst.yyline)
                };
            }

            if (options.TryGetValue("decisionDurability", out optionValue))
            {
                decisionDurability = optionValue switch
                {
                    "bestEffort" => DecisionDurability.BestEffort,
                    "durable" => DecisionDurability.Durable,
                    _ => throw new KahunaScriptException("Unsupported decisionDurability option: " + optionValue, optionsAst.yyline)
                };
            }

            // A read pinned to a past timestamp cannot see writes that land after it, so validating those reads
            // for write skew would promise a guarantee the engine cannot keep. Interactive transactions refuse
            // the same combination.
            if (!readTimestamp.IsNull() && readValidation == ReadValidation.TrackAndValidate)
                throw new KahunaScriptException("snapshot cannot be combined with readValidation=trackAndValidate", optionsAst.yyline);
        }

        // Clamp to the server's hard maximum, as interactive transactions do. The limit bounds how long any
        // transaction can hold locks and an admission slot, and how old a transaction's read snapshot can get
        // before age-based reclamation treats it as orphaned.
        timeout = Math.Min(timeout, configuration.MaxTransactionTimeout);

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

        using (PooledCancellationSource pooledAdmission = CancellationSourcePool.Rent())
        {
            CancellationTokenSource admissionCts = pooledAdmission.Source;

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
        using PooledCancellationSource pooledExecution = CancellationSourcePool.Rent();

        CancellationTokenSource cts = pooledExecution.Source;

        cts.CancelAfter(TimeSpan.FromMilliseconds(timeout));

        // A factory rather than a single instance: a script that started inside an actor turn and had to leave it
        // runs again from the start, and that second run is a new transaction with an identity of its own.
        ScriptTransactionContext NewContext() => new()
        {
            TransactionId = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId()),
            Priority = priority,
            Locking = locking,
            ReadTimestamp = readTimestamp,
            ReadValidation = readValidation,
            DecisionDurability = decisionDurability,
            Action = autoCommit ? KeyValueTransactionAction.Commit : KeyValueTransactionAction.Abort,
            AsyncRelease = asyncRelease,
            Result = new() { Type = KeyValueResponseType.Aborted },
            Parameters = parameters,
            FunctionTable = functionTable,
            HybridLogicalClock = raft.HybridLogicalClock,
            LocalNodeId = raft.GetLocalNodeId()
        };

        ScriptTransactionContext context = NewContext();

        try
        {
            bool ranInActorTurn = false;

            // Inside the try so that a malformed script surfacing here still runs the finally that returns
            // the admission slot — a slot lost to an early throw would shrink node capacity permanently.
            //
            // The four lock sets live inside this branch because only a pessimistic transaction has any use
            // for them. Declared outside, an optimistic script — and a pessimistic one that locks nothing —
            // allocated four sets that were never read.
            if (locking == KeyValueTransactionLocking.Pessimistic)
            {
                HashSet<string> ephemeralLocksToAcquire = [];
                HashSet<string> persistentLocksToAcquire = [];
                HashSet<string> ephemeralPrefixLocksToAcquire = [];
                HashSet<string> persistentPrefixLocksToAcquire = [];

                KeyValueLockHelper.GetLocksToAcquire(
                    context,
                    ast,
                    ephemeralLocksToAcquire,
                    persistentLocksToAcquire,
                    ephemeralPrefixLocksToAcquire,
                    persistentPrefixLocksToAcquire
                );

                // An auto-commit script whose whole lock set is one ephemeral key led by this node runs inside a
                // single turn of the actor that owns the key. A script that started there and had to leave — it
                // reached for something the turn cannot serve — committed nothing and released its key, so it
                // starts over below as a new transaction on the general path.
                if (autoCommit
                    && ephemeralLocksToAcquire.Count == 1
                    && persistentLocksToAcquire.Count == 0
                    && ephemeralPrefixLocksToAcquire.Count == 0
                    && persistentPrefixLocksToAcquire.Count == 0
                    && HasActorTurnShape(ast)
                    && coordinator.CanRunInActorTurn(context))
                {
                    ActorTurnOutcome outcome = await TryRunInActorTurn(context, ast, OnlyKey(ephemeralLocksToAcquire), timeout, cts.Token);

                    if (outcome == ActorTurnOutcome.Escaped)
                    {
                        if (!context.PerKeyWorkingSetReleased)
                            _ = coordinator.ReleaseWorkingSet(context);

                        context = NewContext();

                        ephemeralLocksToAcquire.Clear();
                        persistentLocksToAcquire.Clear();
                        ephemeralPrefixLocksToAcquire.Clear();
                        persistentPrefixLocksToAcquire.Clear();

                        KeyValueLockHelper.GetLocksToAcquire(
                            context,
                            ast,
                            ephemeralLocksToAcquire,
                            persistentLocksToAcquire,
                            ephemeralPrefixLocksToAcquire,
                            persistentPrefixLocksToAcquire
                        );
                    }

                    ranInActorTurn = outcome == ActorTurnOutcome.Completed;
                }

                if (!ranInActorTurn)
                {
                    await AcquireLocksPessimistically(
                        context,
                        ephemeralLocksToAcquire,
                        persistentLocksToAcquire,
                        ephemeralPrefixLocksToAcquire,
                        persistentPrefixLocksToAcquire,
                        timeout,
                        cts.Token
                    );
                }
            }

            if (!ranInActorTurn)
                await ExecuteTransactionInternal(context, ast, cts.Token);

            if (context.Action == KeyValueTransactionAction.Commit)
            {
                // A script that ran inside an actor turn finalized there, before it let the actor go.
                if (!ranInActorTurn)
                    await coordinator.TwoPhaseCommit(context, cts.Token);

                // A durable finalize that ended unresolved is abandoned by this script: nothing retries its
                // identity. Fence it now so its installed intents free their keys for the caller's re-run, and
                // so a stalled commit that already won is reported as committed instead of retried twice.
                if (context.Result?.Type == KeyValueResponseType.MustRetry)
                    await coordinator.FenceAbandonedFinalize(context);

                // The coordinator names why it aborted (a conflict, a moved base, a refused prepare). That reason
                // is part of the outcome the client acts on, so it is carried through rather than flattened.
                if (context.Result?.Type == KeyValueResponseType.Aborted)
                    return new() { Type = KeyValueResponseType.Aborted, Reason = context.Result.Reason ?? "Transaction aborted" };

                return context.Result ?? new() { Type = KeyValueResponseType.Errored };
            }

            return UncommittedResult(context);
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

            // A finalize left unresolved by an exception path above is abandoned like any other: fence it
            // before the working set goes, so its prepared intents do not outlive the script. A no-op when
            // the commit path already fenced it, or when no durable finalize ran.
            if (context.UnresolvedDurableFinalize is not null)
                await coordinator.FenceAbandonedFinalize(context);

            // Release every confirmed lock shape not finalized by two-phase commit and clean the
            // transaction's read MVCC. Safe to run on a committed transaction: its modified keys were already
            // finalized and are skipped internally. Best-effort — no terminal promise rides on completion.
            if (context.AsyncRelease)
                _ = coordinator.ReleaseWorkingSet(context);
            else
                await coordinator.ReleaseWorkingSet(context);
        }
    }

    private enum ActorTurnOutcome
    {
        /// <summary>The turn did not start: the key is led elsewhere, or the actor did not take the message.</summary>
        NotRun,

        /// <summary>The script ran, finalized and released inside the turn. Its outcome is on the context.</summary>
        Completed,

        /// <summary>The script left the turn half way. Nothing was committed and the key was released.</summary>
        Escaped
    }

    /// <summary>
    /// Runs the script inside one turn of the actor that owns <paramref name="key"/>, if that actor is on this
    /// node. Whatever the script or its finalize threw inside the turn is rethrown here, so it reaches the same
    /// handlers, and becomes the same result, as if the general path had thrown it.
    /// </summary>
    private async Task<ActorTurnOutcome> TryRunInActorTurn(
        ScriptTransactionContext context, NodeAst ast, string key, int timeout, CancellationToken cancellationToken)
    {
        if (!await manager.IsLocallyLedHashKey(key, cancellationToken))
            return ActorTurnOutcome.NotRun;

        ScriptActorTurn turn = new(
            manager, coordinator, ExecuteTransactionInternal, context, ast, key, timeout + ExtraLockingDelay, cancellationToken);

        await manager.RunActorTurn(key, turn);

        if (!turn.Started)
            return ActorTurnOutcome.NotRun;

        if (turn.Escaped)
        {
            DurableTransactionMetrics.ScriptActorTurnEscape();

            return ActorTurnOutcome.Escaped;
        }

        DurableTransactionMetrics.ScriptActorTurn();

        turn.Failure?.Throw();

        return ActorTurnOutcome.Completed;
    }

    /// <summary>
    /// Whether every node of the script is one a single actor turn can run: expressions, LET, IF, RETURN, THROW,
    /// and the point operations over the ephemeral key space. A turn holds its actor for as long as it runs, so
    /// anything that waits (SLEEP), loops (FOR), reaches other partitions (bucket reads, scans), touches the
    /// persistent key space, or controls a transaction by hand (BEGIN, COMMIT, ROLLBACK) keeps the script on
    /// the general path. The list names what is allowed, so a node type added later is left out until someone
    /// decides it belongs. The answer is remembered on the root of the cached tree.
    /// </summary>
    private static bool HasActorTurnShape(NodeAst root)
    {
        int memo = Volatile.Read(ref root.actorTurnShapeMemo);

        if (memo == 0)
        {
            memo = EveryNodeFitsAnActorTurn(root) ? 1 : 2;
            Volatile.Write(ref root.actorTurnShapeMemo, memo);
        }

        return memo == 1;
    }

    private static bool EveryNodeFitsAnActorTurn(NodeAst? node)
    {
        if (node is null)
            return true;

        switch (node.nodeType)
        {
            case NodeType.NullType:
            case NodeType.IntegerType:
            case NodeType.StringType:
            case NodeType.FloatType:
            case NodeType.BooleanType:
            case NodeType.Identifier:
            case NodeType.Placeholder:
            case NodeType.StmtList:
            case NodeType.Let:
            case NodeType.If:
            case NodeType.Return:
            case NodeType.Throw:
            case NodeType.Eset:
            case NodeType.Eget:
            case NodeType.Eexists:
            case NodeType.Edelete:
            case NodeType.Eextend:
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
            case NodeType.NotSet:
            case NodeType.NotFound:
            case NodeType.SetFlagsList:
            case NodeType.SetEx:
            case NodeType.SetNotExists:
            case NodeType.SetExists:
            case NodeType.SetCmp:
            case NodeType.SetCmpRev:
            case NodeType.SetNoRev:
                break;

            default:
                return false;
        }

        return EveryNodeFitsAnActorTurn(node.leftAst)
            && EveryNodeFitsAnActorTurn(node.rightAst)
            && EveryNodeFitsAnActorTurn(node.extendedOne)
            && EveryNodeFitsAnActorTurn(node.extendedTwo)
            && EveryNodeFitsAnActorTurn(node.extendedThree)
            && EveryNodeFitsAnActorTurn(node.extendedFour);
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

            // One buffer for both sets, sized for the larger. The ephemeral pass finishes before the
            // persistent one starts, so the second reuse cannot read the first pass's keys.
            string[] buffer = ArrayPool<string>.Shared.Rent(
                Math.Max(ephemeralPrefixLocksToAcquire.Count, persistentPrefixLocksToAcquire.Count));

            try
            {
                await AcquirePrefixLocks(context, ephemeralPrefixLocksToAcquire, buffer, KeyValueDurability.Ephemeral, timeout, ctsToken);
                await AcquirePrefixLocks(context, persistentPrefixLocksToAcquire, buffer, KeyValueDurability.Persistent, timeout, ctsToken);
            }
            finally
            {
                // Cleared on the way back: the buffer holds key strings, and a pooled array that keeps
                // them alive pins them until the slot is next used.
                ArrayPool<string>.Shared.Return(buffer, clearArray: true);
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
                        OnlyKey(ephemeralLocksToAcquire),
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
                        await manager.LocateAndTryAcquireExclusiveLock(context.TransactionId, OnlyKey(persistentLocksToAcquire), timeout + ExtraLockingDelay, KeyValueDurability.Persistent, ctsToken);

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
    /// Acquires one durability's prefix locks, in ordinal order, recording each one on the context.
    ///
    /// <para>The order is the deadlock-avoidance contract described on the caller: two transactions whose
    /// prefixes overlap must attempt them in the same relative order, and a hash set's enumeration order
    /// depends on what else the set holds. <paramref name="buffer"/> is the caller's, and is large enough
    /// for this set.</para>
    /// </summary>
    private async Task AcquirePrefixLocks(
        ScriptTransactionContext context,
        HashSet<string> prefixKeys,
        string[] buffer,
        KeyValueDurability durability,
        int timeout,
        CancellationToken ctsToken
    )
    {
        int count = SortOrdinalInto(prefixKeys, buffer);

        for (int i = 0; i < count; i++)
        {
            string prefixKey = buffer[i];

            KeyValueResponseType acquirePrefixResponse = await manager.LocateAndTryAcquireExclusivePrefixLock(
                context.TransactionId,
                prefixKey,
                timeout + ExtraLockingDelay,
                durability,
                ctsToken
            );

            if (acquirePrefixResponse != KeyValueResponseType.Locked)
                throw new KahunaAbortedException("Failed to acquire prefix lock: " + prefixKey + " " + durability);

            context.PrefixLocksAcquired!.Add((prefixKey, durability));
        }
    }

    /// <summary>
    /// Whether a BEGIN option name is one the executor reads. Names are case-sensitive, like their values.
    /// </summary>
    private static bool IsKnownTransactionOption(string name)
    {
        return name switch
        {
            "locking" or "autoCommit" or "asyncRelease" or "timeout" or "admissionWait" or "snapshot" or
                "priority" or "readValidation" or "decisionDurability" => true,
            _ => false
        };
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
    /// Executes a script AST node. A statement list runs as a flat loop over its statements.
    ///
    /// <para>The grammar builds a statement list as a left-leaning spine with one level per statement.
    /// Descending that spine through recursive awaits put one suspended call per statement under the
    /// first statement that went asynchronous, and every one of them boxed its state machine. The loop
    /// keeps a whole list inside this one call, so a list costs one box however long it is. Only a
    /// genuinely nested list (an IF or FOR body) is another call.</para>
    /// </summary>
    private async Task ExecuteTransactionInternal(ScriptTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        NodeAst[]? statements = null;
        int next = 0;

        if (ast.nodeType == NodeType.StmtList)
        {
            if (!context.TryRestoreBatchProbe(ast))
            {
                ProbeBatchablePrefix(context, ast);
                context.RecordBatchProbe(ast);
            }

            statements = GetStatements(ast);

            // The probe stashed the subtree covering the largest batchable prefix. That prefix holds the
            // first statements of the list, so it runs before anything else and the loop resumes after it.
            NodeAst? boundary = context.BatchBoundary;

            if (boundary is not null)
            {
                // A batched write fans out through the locator, which a turn must never do.
                RefuseInsideActorTurn(context);

                context.BatchBoundary = null;

                context.Result = context.BatchBoundaryIsSetMany
                    ? await SetManyCommand.Execute(manager, context, boundary, cancellationToken)
                    : await DeleteManyCommand.Execute(manager, context, boundary, cancellationToken);

                next = IndexAfterBatch(statements, boundary);
            }
        }

        while (true)
        {
            if (statements is not null)
            {
                if (next >= statements.Length)
                    break;

                ast = statements[next++];
            }

            if (context.Status == KeyValueExecutionStatus.Stop)
                break;

            cancellationToken.ThrowIfCancellationRequested();

            switch (ast.nodeType)
            {
                // The grammar never puts a list in statement position; a tree built by hand can.
                case NodeType.StmtList:
                    await ExecuteTransactionInternal(context, ast, cancellationToken);
                    break;

                case NodeType.If:
                    await ExecuteIf(context, ast, cancellationToken);
                    break;

                case NodeType.For:
                    RefuseInsideActorTurn(context);
                    await ExecuteFor(context, ast, cancellationToken);
                    break;

                case NodeType.Let:
                {
                    LetCommand.Execute(context, ast);
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
                    RefuseInsideActorTurn(context);
                    context.Result = await GetByBucketCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.EGetByBucket:
                    RefuseInsideActorTurn(context);
                    context.Result = await GetByBucketCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                // A prefix scan fans out over every partition and carries no transaction identity, so inside a
                // transaction it would read a snapshot blind to the transaction's own uncommitted writes and
                // would take no prefix lock to make the read repeatable. Refusing is the honest answer until a
                // transaction-carrying scan exists. GET BY BUCKET is the prefix read that does work here.
                case NodeType.ScanByPrefix:
                case NodeType.EscanByPrefix:
                    RefuseInsideActorTurn(context);
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
                    ReturnCommand.Execute(context, ast);
                    break;

                case NodeType.Sleep:
                    RefuseInsideActorTurn(context);
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
                    context.DeferResult(KeyValueTransactionExpression.Eval(context, ast));
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

            if (statements is null)
                break;
        }
    }

    /// <summary>
    /// Stops a script that runs inside an actor turn from doing what a turn must never do: wait, loop without a
    /// bound, or send work through the locator. Static analysis keeps such scripts out of a turn in the first
    /// place; this is the check that holds if that analysis is ever wrong. The turn ends, releases its key, and
    /// the script runs again on the general path.
    /// </summary>
    private static void RefuseInsideActorTurn(ScriptTransactionContext context)
    {
        if (context.ActorTurn is not null)
            throw new ActorTurnEscapeException("The statement cannot run inside an actor turn");
    }

    /// <summary>
    /// The statements of a list in execution order: the deepest left leaf first, then the right
    /// statement of each spine node from the bottom up.
    ///
    /// <para>Kept on the list's root node, because a parsed tree is cached and shared by every later
    /// execution of the same script and its shape never changes. Two concurrent executions can both build
    /// the array; both build the same one from the same tree, and the store is a single reference write,
    /// so the race is harmless. Nothing writes to the array after it is published.</para>
    /// </summary>
    private static NodeAst[] GetStatements(NodeAst list)
    {
        NodeAst[]? statements = Volatile.Read(ref list.statementsMemo);

        if (statements is not null)
            return statements;

        int count = 0;
        NodeAst? node = list;

        while (node is not null && node.nodeType == NodeType.StmtList)
        {
            if (node.rightAst is not null)
                count++;

            node = node.leftAst;
        }

        if (node is not null)
            count++;

        statements = new NodeAst[count];

        int index = count;
        node = list;

        while (node is not null && node.nodeType == NodeType.StmtList)
        {
            if (node.rightAst is not null)
                statements[--index] = node.rightAst;

            node = node.leftAst;
        }

        if (node is not null)
            statements[--index] = node;

        Volatile.Write(ref list.statementsMemo, statements);

        return statements;
    }

    /// <summary>
    /// The index of the first statement after a batched prefix. The batch subtree is a spine node, and
    /// its right statement is the last statement the batch covers.
    /// </summary>
    private static int IndexAfterBatch(NodeAst[] statements, NodeAst boundary)
    {
        for (int i = 0; i < statements.Length; i++)
        {
            if (ReferenceEquals(statements[i], boundary.rightAst))
                return i + 1;
        }

        throw new KahunaScriptException("Batched statements are not part of the statement list", boundary.yyline);
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
    /// single set-many/delete-many. The probe runs once when the list is entered, and the batch
    /// runs before any other statement of the list, so context state (variables, parameters) at
    /// probe time is the state the batched statements execute against.
    /// </summary>
    private static void ProbeBatchablePrefix(ScriptTransactionContext context, NodeAst ast)
    {
        context.BatchBoundary = null;

        // The spine depth is counted first so the array below is exact. Both walks only follow
        // already-built references, and the second replaces a list that grew by doubling and was
        // discarded a few lines later.
        int depth = 0;
        NodeAst node = ast;

        while (node.nodeType == NodeType.StmtList)
        {
            depth++;

            if (node.leftAst is null)
                return;

            node = node.leftAst;
        }

        // The first statement fixes the batch kind; anything else is not batchable. Checked before any
        // buffer is taken, because this is where most probes end.
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

        // Collect the left spine top-down: spine[i] covers statements [0 .. (n-1) - i], where
        // n = depth + 1 statements. The deepest left leaf is statement 0.
        NodeAst[] spine = ArrayPool<NodeAst>.Shared.Rent(depth);

        try
        {
            node = ast;

            for (int i = 0; i < depth; i++)
            {
                spine[i] = node;
                node = node.leftAst!;
            }

            int n = depth + 1;

            // Reused across probes on this thread. The probe is synchronous from end to end, so the set
            // cannot be observed by another script while this one is using it.
            HashSet<(string, KeyValueDurability)> keys = probeKeys ??= new(n);
            keys.Clear();

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
        finally
        {
            // Cleared on the way back: the buffer holds syntax-tree references, and a pooled array that
            // keeps them alive pins a whole parsed script until the slot is next used.
            ArrayPool<NodeAst>.Shared.Return(spine, clearArray: true);
        }
    }

    /// <summary>
    /// The duplicate-key set the batch probe fills, reused between probes on the same thread.
    ///
    /// <para>The probe allocated one set per call, and a statement list is probed once per entry — which
    /// for a loop body is once per iteration. The set is only ever live inside one synchronous probe, so
    /// one instance per thread is enough and no two scripts can share it.</para>
    /// </summary>
    [ThreadStatic]
    private static HashSet<(string, KeyValueDurability)>? probeKeys;

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
