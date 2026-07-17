
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions.Commands;
using Kahuna.Server.KeyValues.Transactions.Data;
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

    public ScriptTransactionExecutor(
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        IRaft raft,
        ILogger<IKahuna> logger,
        TransactionCoordinator coordinator
    )
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
        this.coordinator = coordinator;
        this.scriptParserProcessor = new(this.configuration, logger);
    }

    /// <summary>
    /// Executes a single or multi-command transaction in an atomic manner.
    /// </summary>
    public async Task<KeyValueTransactionResult> TryExecuteTx(ReadOnlyMemory<byte> script, string? hash, List<KeyValueParameter>? parameters)
    {
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
                    return await ExecuteTransaction(ast.leftAst!, ast.rightAst, parameters, false);

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
                    return await ExecuteTransaction(ast, null, parameters, true);

                case NodeType.SetCmp:
                case NodeType.SetCmpRev:
                case NodeType.SetNotExists:
                case NodeType.SetExists:
                    break;

                case NodeType.Rollback:
                case NodeType.Commit:
                    throw new KahunaScriptException("Invalid transaction", ast.yyline);

                default:
                    throw new KahunaScriptException("Unknown command: " + ast.nodeType, ast.yyline);
            }

            return new() { Type = KeyValueResponseType.Errored };
        }
        catch (KahunaScriptException ex)
        {
            logger.LogKahunaScriptException(ex);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message + " at line " + ex.Line };
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
    private async Task<KeyValueTransactionResult> ExecuteTransaction(NodeAst ast, NodeAst? optionsAst, List<KeyValueParameter>? parameters, bool autoCommit)
    {
        bool asyncRelease = false;
        int timeout = configuration.DefaultTransactionTimeout;
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
                    throw new KahunaScriptException("Invalid timeout option: " + timeout, optionsAst.yyline);
            }

            if (options.TryGetValue("snapshot", out optionValue))
            {
                if (!long.TryParse(optionValue, out long snapshotMs) || snapshotMs == 0)
                    throw new KahunaScriptException("snapshot must be a non-zero Unix epoch millisecond value", optionsAst.yyline);
                readTimestamp = new HLCTimestamp(0, snapshotMs, uint.MaxValue);
            }
        }

        using CancellationTokenSource cts = new();

        cts.CancelAfter(TimeSpan.FromMilliseconds(timeout));

        HLCTimestamp transactionId = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId());

        ScriptTransactionContext context = new()
        {
            TransactionId = transactionId,
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

        if (locking == KeyValueTransactionLocking.Pessimistic)
            KeyValueLockHelper.GetLocksToAcquire(
                context,
                ast,
                ephemeralLocksToAcquire,
                persistentLocksToAcquire,
                ephemeralPrefixLocksToAcquire,
                persistentPrefixLocksToAcquire
            );

        try
        {
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

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message + " at line " + ex.Line };
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
            logger.LogOperationCanceledException(ex);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.GetType().Name + ": " + ex.Message };
        }
        finally
        {
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

            foreach (string prefixKey in ephemeralPrefixLocksToAcquire)
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

            foreach (string prefixKey in persistentPrefixLocksToAcquire)
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
                        await manager.LocateAndTryAcquireExclusiveLock(context.TransactionId, persistentLocksToAcquire.First(), timeout + 10, KeyValueDurability.Persistent, ctsToken);

                    if (acquireResponse != KeyValueResponseType.Locked)
                        throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);

                    context.LocksAcquired.Add((keyName, durability));
                    return;
                }
            }

            List<(string, int, KeyValueDurability)> keysToLock = new(numberLocks);

            foreach (string key in ephemeralLocksToAcquire)
                keysToLock.Add((key, timeout + ExtraLockingDelay, KeyValueDurability.Ephemeral));

            foreach (string key in persistentLocksToAcquire)
                keysToLock.Add((key, timeout + ExtraLockingDelay, KeyValueDurability.Persistent));

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

                    options.Add(ast.leftAst.yytext, ast.rightAst.yytext);
                    break;
            }

            break;
        }
    }

    /// <summary>
    /// Recursively executes the script AST.
    /// </summary>
    private async Task ExecuteTransactionInternal(ScriptTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        if (ast.nodeType == NodeType.StmtList)
        {
            HashSet<(string, KeyValueDurability)> keys = [];

            if (CanBatchBeSetMany(context, ast, keys))
            {
                context.Result = await SetManyCommand.Execute(manager, context, ast, cancellationToken);
                return;
            }

            keys.Clear();

            if (CanBatchBeDeleteMany(context, ast, keys))
            {
                context.Result = await DeleteManyCommand.Execute(manager, context, ast, cancellationToken);
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
                        await ExecuteTransactionInternal(context, ast.leftAst, cancellationToken);

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
                    throw new NotImplementedException();
            }

            break;
        }
    }

    private async Task ExecuteIf(ScriptTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid IF expression", ast.yyline);

        KeyValueExpressionResult expressionResult = KeyValueTransactionExpression.Eval(context, ast.leftAst);

        if (expressionResult is { Type: KeyValueExpressionType.BoolType, BoolValue: true })
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

    private static bool CanBatchBeSetMany(ScriptTransactionContext context, NodeAst ast, HashSet<(string, KeyValueDurability)> keys)
    {
        bool areSets = false;

        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null)
                    {
                        if (!CanBatchBeSetMany(context, ast.leftAst, keys))
                            return false;
                    }

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }

                case NodeType.Set:
                    if (ast.leftAst?.yytext is null)
                        return false;

                    if (!keys.Add((ast.leftAst.yytext!, KeyValueDurability.Persistent)))
                        return false;

                    areSets = true;
                    break;

                case NodeType.Eset:
                    if (ast.leftAst?.yytext is null)
                        return false;

                    if (!keys.Add((ast.leftAst.yytext!, KeyValueDurability.Ephemeral)))
                        return false;

                    areSets = true;
                    break;

                default:
                    return false;
            }

            break;
        }

        return areSets;
    }

    private static bool CanBatchBeDeleteMany(ScriptTransactionContext context, NodeAst ast, HashSet<(string, KeyValueDurability)> keys)
    {
        bool areDeletes = false;

        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null)
                    {
                        if (!CanBatchBeDeleteMany(context, ast.leftAst, keys))
                            return false;
                    }

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }

                case NodeType.Delete:
                {
                    if (ast.leftAst is null)
                        return false;

                    string keyName;

                    try
                    {
                        keyName = BaseCommand.GetKeyName(context, ast.leftAst);
                    }
                    catch (KahunaScriptException)
                    {
                        return false;
                    }

                    if (!keys.Add((keyName, KeyValueDurability.Persistent)))
                        return false;

                    areDeletes = true;
                    break;
                }

                case NodeType.Edelete:
                {
                    if (ast.leftAst is null)
                        return false;

                    string keyName;

                    try
                    {
                        keyName = BaseCommand.GetKeyName(context, ast.leftAst);
                    }
                    catch (KahunaScriptException)
                    {
                        return false;
                    }

                    if (!keys.Add((keyName, KeyValueDurability.Ephemeral)))
                        return false;

                    areDeletes = true;
                    break;
                }

                default:
                    return false;
            }

            break;
        }

        return areDeletes;
    }
}
