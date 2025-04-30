
using System.Collections.Concurrent;
using DotNext;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions.Commands;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// This class is responsible for coordinating multi-key transactions across multiple nodes in a distributed manner.
/// 
/// If it is determined that a transaction is not required for simple commands, the request is simply redirected to the
/// basic API, avoiding unnecessary coordination costs.
///
/// Kahuna orchestrates distributed transactions by combining multi-version concurrency control (MVCC),
/// a two-phase commit protocol, and the Raft consensus algorithm to ensure consistency
/// and high availability across a distributed key–value store.
///
/// Key Components and Mechanisms:
///
/// Multi-Version Concurrency Control (MVCC):
/// Kahuna uses MVCC to maintain multiple versions of data items. This allows transactions to read a
/// consistent snapshot of the data without interfering with concurrent writes, ensuring isolation.
///
/// Two-Phase Commit (2PC) Protocol:
///
/// - Prewrite Phase: When a transaction starts, Kahuna’s transaction coordinator selects a lock for the transaction.
///  All writes are first “prewritten.” In this phase, locks are set on the involved keys and tentative data versions are written.
/// This ensures that the transaction can later be committed atomically.
/// - Commit Phase: Once all prewrites succeed, the coordinator commits the transaction by first committing the primary key
/// and then informing other nodes to commit their corresponding secondary keys. If any node reports an issue during prewrite,
/// the transaction is rolled back to maintain atomicity.
///
/// Raft Consensus for Replication and Fault Tolerance:
/// Each Kahuna node is part of a Raft group that replicates data across several nodes. This means that even if some nodes fail,
/// the transaction’s state and data changes are preserved across replicas. Raft ensures that all nodes agree on the order of operations,
/// which is crucial for maintaining a consistent distributed state.
///
/// Handling Concurrency and Failures:
/// Lock Management: Locks on keys prevent conflicting operations, ensuring that only one transaction can modify a
/// given data item at a time.
/// Rollback Mechanisms: In case a transaction cannot proceed (e.g., due to conflicts or node failures), Kahuna has
/// mechanisms to clean up by rolling back any prewritten changes, thereby ensuring the system remains in a consistent state.
/// </summary>
internal sealed class KeyValueTransactionCoordinator
{
    private const int ExtraLockingDelay = 10;
    
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly ScriptParserProcessor scriptParserProcessor;

    private readonly ConcurrentDictionary<HLCTimestamp, KeyValueTransactionContext> sessions = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="configuration"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public KeyValueTransactionCoordinator(KeyValuesManager manager, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;

        this.scriptParserProcessor = new(this.configuration, logger);
    }

    /// <summary>
    /// Executes a single or multi-command transaction in an atomic manner
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    /// <exception cref="KahunaScriptException"></exception>
    public async Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash, List<KeyValueParameter>? parameters)
    {
        try
        {
            NodeAst ast = scriptParserProcessor.Parse(script, hash);

            switch (ast.nodeType)
            {
                case NodeType.Set:
                    return await SetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Get:
                    return await GetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.GetByPrefix:
                    return await GetByPrefixCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);
                
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

                case NodeType.EgetByPrefix:
                    return await GetByPrefixCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);
                
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
            logger.LogDebug("KahunaScriptException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message + " at line " + ex.Line };
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogDebug("KahunaAbortedException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Aborted, Reason = ex.Message };
        }
        catch (TaskCanceledException ex)
        {
            logger.LogDebug("TaskCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (OperationCanceledException ex)
        {
            logger.LogDebug("TaskCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (Exception ex)
        {
            logger.LogError("TryExecuteTx: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message };
        }
    }

    /// <summary>
    /// Starts a new key-value transaction and returns the unique transaction identifier.
    /// </summary>
    /// <param name="options">The options used to configure the transaction, such as locking, timeout, and release behavior.</param>
    /// <returns>Returns the unique transaction ID of type <see cref="HLCTimestamp"/>.</returns>
    public async Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options)
    {
        bool result;
        HLCTimestamp transactionId;

        do
        {
            transactionId = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId());

            KeyValueTransactionContext context = new()
            {
                TransactionId = transactionId,
                Locking = options.Locking,
                Action = KeyValueTransactionAction.Commit,
                AsyncRelease = options.AsyncRelease,
                Timeout = options.Timeout <= 0 ? configuration.DefaultTransactionTimeout : options.Timeout
            };

            result = sessions.TryAdd(transactionId, context);

        } while (!result);

        logger.LogDebug("Started interactive transaction {TransactionId}", transactionId);

        await Task.CompletedTask;

        return (KeyValueResponseType.Set, transactionId);
    }

    /// <summary>
    /// Commits the transaction with the given transaction ID.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="acquiredLocks"></param>
    /// <param name="modifiedKeys"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> CommitTransaction(
        HLCTimestamp transactionId,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        if (!sessions.TryGetValue(transactionId, out KeyValueTransactionContext? context))
        {
            logger.LogWarning("Trying to commit unknown transaction {TransactionId}", transactionId);

            return KeyValueResponseType.Errored;
        }

        try
        {
            context.Result = new() { Type = KeyValueResponseType.Set, Reason = null };

            foreach (KeyValueTransactionModifiedKey acquiredLock in acquiredLocks)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((acquiredLock.Key ?? "", acquiredLock.Durability));
            }

            foreach (KeyValueTransactionModifiedKey modifiedKey in modifiedKeys)
            {
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((modifiedKey.Key ?? "", modifiedKey.Durability));
            }

            await TwoPhaseCommit(context, CancellationToken.None);

            if (context.Result is null)
                return KeyValueResponseType.Errored;

            if (context.Result.Type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored)
                return KeyValueResponseType.Aborted;

            logger.LogDebug("Committed interactive transaction {TransactionId}", transactionId);

            sessions.TryRemove(transactionId, out _);

            return KeyValueResponseType.Committed;
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogDebug("KahunaAbortedException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            //return new() { Type = KeyValueResponseType.Aborted, Reason = ex.Message };
            return KeyValueResponseType.Aborted;
        }
        catch (TaskCanceledException ex)
        {
            logger.LogDebug("TaskCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            //return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };

            return KeyValueResponseType.Aborted;
        }
        catch (OperationCanceledException ex)
        {
            logger.LogDebug("OperationCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            //return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };

            return KeyValueResponseType.Aborted;
        }
        catch (Exception ex)
        {
            logger.LogDebug("OperationCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            //return new() { Type = KeyValueResponseType.Errored, Reason = ex.GetType().Name + ": " + ex.Message };

            return KeyValueResponseType.Aborted;
        }
        finally
        {
            if (context.Locking == KeyValueTransactionLocking.Pessimistic || (context.State != KeyValueTransactionState.Committed && context.State != KeyValueTransactionState.RolledBack))
            {
                // Final Step: Release locks
                if (context.AsyncRelease)
                    _ = ReleaseAcquiredLocks(context);
                else
                    await ReleaseAcquiredLocks(context);
            }
        }
    }

    /// <summary>
    /// Rollbacks the transaction with the given transaction ID.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="acquiredLocks"></param>
    /// <param name="modifiedKeys"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> RollbackTransaction(
        HLCTimestamp transactionId,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        if (!sessions.TryGetValue(transactionId, out KeyValueTransactionContext? context))
        {
            logger.LogWarning("Trying to rollback unknown transaction {TransactionId}", transactionId);

            return KeyValueResponseType.Errored;
        }

        try
        {
            foreach (KeyValueTransactionModifiedKey acquiredLock in acquiredLocks)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((acquiredLock.Key ?? "", acquiredLock.Durability));
            }

            foreach (KeyValueTransactionModifiedKey modifiedKey in modifiedKeys)
            {
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((modifiedKey.Key ?? "", modifiedKey.Durability));
            }

            context.Action = KeyValueTransactionAction.Abort;

            logger.LogDebug("Rolled back interactive transaction {TransactionId}", transactionId);

            sessions.TryRemove(transactionId, out _);

            return KeyValueResponseType.RolledBack;
        }
        finally
        {
            if (context.Locking == KeyValueTransactionLocking.Pessimistic || context.State != KeyValueTransactionState.Committed && context.State != KeyValueTransactionState.RolledBack)
            {
                // Final Step: Release locks
                if (context.AsyncRelease)
                    _ = ReleaseAcquiredLocks(context);
                else
                    await ReleaseAcquiredLocks(context);
            }
        }
    }

    /// <summary>
    /// Returns a temporary transaction context for executing a single command.
    /// </summary>
    /// <param name="parameters"></param>
    /// <returns></returns>
    private static KeyValueTransactionContext GetTempTransactionContext(List<KeyValueParameter>? parameters)
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
    /// Executes a transaction using Two-Phase commit protocol (2PC).
    /// The autoCommit flag offers an specific commit/rollback instruction or an automatic commit behavior on success. 
    /// </summary>
    /// <param name="ast"></param>
    /// <param name="optionsAst"></param>
    /// <param name="parameters"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="KahunaScriptException"></exception>
    private async Task<KeyValueTransactionResult> ExecuteTransaction(NodeAst ast, NodeAst? optionsAst, List<KeyValueParameter>? parameters, bool autoCommit)
    {
        bool asyncRelease = false;
        int timeout = configuration.DefaultTransactionTimeout;
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic;

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
        }

        using CancellationTokenSource cts = new();

        cts.CancelAfter(TimeSpan.FromMilliseconds(timeout));

        // Need HLC timestamp for the transaction id
        HLCTimestamp transactionId = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueTransactionContext context = new()
        {
            TransactionId = transactionId,
            Locking = locking,
            Action = autoCommit ? KeyValueTransactionAction.Commit : KeyValueTransactionAction.Abort,
            AsyncRelease = asyncRelease,
            Result = new() { Type = KeyValueResponseType.Aborted },
            Parameters = parameters
        };

        HashSet<string> ephemeralLocksToAcquire = [];
        HashSet<string> persistentLocksToAcquire = [];
        HashSet<string> ephemeralPrefixLocksToAcquire = [];
        HashSet<string> persistentPrefixLocksToAcquire = [];

        // Acquire all locks in advance for pessimistic locking
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
            // Step 1: Acquire locks in advance for pessimistic locking
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

            // Step 2: Execute transaction
            await ExecuteTransactionInternal(context, ast, cts.Token);

            if (context.Action == KeyValueTransactionAction.Commit)
            {
                await TwoPhaseCommit(context, cts.Token);

                if (context.Result?.Type == KeyValueResponseType.Aborted)
                    return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted" };

                return context.Result ?? new() { Type = KeyValueResponseType.Errored };
            }

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted" };
        }
        catch (KahunaScriptException ex)
        {
            logger.LogDebug("KahunaScriptException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message + " at line " + ex.Line };
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogDebug("KahunaAbortedException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Aborted, Reason = ex.Message };
        }
        catch (TaskCanceledException ex)
        {
            logger.LogDebug("TaskCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (OperationCanceledException ex)
        {
            logger.LogDebug("OperationCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (Exception ex)
        {
            logger.LogDebug("OperationCanceledException: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return new() { Type = KeyValueResponseType.Errored, Reason = ex.GetType().Name + ": " + ex.Message };
        }
        finally
        {
            if (context.Locking == KeyValueTransactionLocking.Pessimistic || context.State != KeyValueTransactionState.Committed && context.State != KeyValueTransactionState.RolledBack)
            {
                // Final Step: Release locks
                if (context.AsyncRelease)
                    _ = ReleaseAcquiredLocks(context);
                else
                    await ReleaseAcquiredLocks(context);
            }
        }
    }

    /// <summary>
    /// Generates a plan to acquire all locks needed for the transaction.
    /// Pessimistic transactions acquire all locks in advance to avoid deadlocks and prevent any external modifications to the keys
    /// </summary>
    /// <param name="context"></param>
    /// <param name="ephemeralLocksToAcquire"></param>
    /// <param name="persistentLocksToAcquire"></param>
    /// <param name="timeout"></param>
    /// <param name="ctsToken"></param>
    /// <exception cref="KahunaAbortedException"></exception>
    private async Task AcquireLocksPessimistically(
        KeyValueTransactionContext context, 
        HashSet<string> ephemeralLocksToAcquire, 
        HashSet<string> persistentLocksToAcquire, 
        HashSet<string> ephemeralPrefixLocksToAcquire,
        HashSet<string> persistentPrefixLocksToAcquire,
        int timeout, 
        CancellationToken ctsToken
    )
    {
        
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
        }
        
        int numberLocks = ephemeralLocksToAcquire.Count + persistentLocksToAcquire.Count;

        if (numberLocks == 0)
            return;

        context.LocksAcquired = [];

        if (numberLocks == 1)
        {
            if (ephemeralLocksToAcquire.Count > 0)
            {
                (KeyValueResponseType acquireResponse, string keyName, KeyValueDurability durability) = await manager.LocateAndTryAcquireExclusiveLock(
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
                (KeyValueResponseType acquireResponse, string keyName, KeyValueDurability durability) =
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

        List<(KeyValueResponseType, string, KeyValueDurability)> lockResponses = await manager.LocateAndTryAcquireManyExclusiveLocks(context.TransactionId, keysToLock, ctsToken);

        foreach ((KeyValueResponseType response, string keyName, KeyValueDurability durability) in lockResponses)
        {
            if (response == KeyValueResponseType.Locked)
                context.LocksAcquired.Add((keyName, durability));
        }

        foreach ((KeyValueResponseType response, string keyName, KeyValueDurability durability) in lockResponses)
        {
            if (response != KeyValueResponseType.Locked)
                throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);
        }
    }

    /// <summary>
    /// Releases any acquired locks during the transaction execution.
    /// </summary>
    /// <param name="context"></param>
    private async Task ReleaseAcquiredLocks(KeyValueTransactionContext context)
    {
        try
        {
            if (context.LocksAcquired is null || context.LocksAcquired.Count == 0)
                return;

            List<(string, KeyValueDurability)> locksToRelease;

            // Commit or rollback will release locks, we just need the ones that haven't been released yet
            if (context.ModifiedKeys is null || (context.State != KeyValueTransactionState.Committed && context.State != KeyValueTransactionState.RolledBack))
                locksToRelease = context.LocksAcquired.ToList();
            else
            {
                locksToRelease = [];

                foreach ((string, KeyValueDurability) lockKey in context.LocksAcquired)
                {
                    if (!context.ModifiedKeys.Contains(lockKey))
                        locksToRelease.Add(lockKey);
                }
            }

            if (locksToRelease.Count == 1)
            {
                (string lockKey, KeyValueDurability durability) = locksToRelease.First();

                await manager.LocateAndTryReleaseExclusiveLock(context.TransactionId, lockKey, durability, CancellationToken.None);
                return;
            }

            await manager.LocateAndTryReleaseManyExclusiveLocks(context.TransactionId, locksToRelease, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError("ReleaseAcquiredLocks: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }

    /// <summary>
    /// Reads all passed transaction options as a dictionary
    /// </summary>
    /// <param name="ast"></param>
    /// <param name="options"></param>
    /// <exception cref="KahunaScriptException"></exception>
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
    /// Executes the 2PC protocol for the transaction.
    /// </summary>
    /// <param name="context"></param>
    /// <param name="cancellationToken"></param>
    private async Task TwoPhaseCommit(KeyValueTransactionContext context, CancellationToken cancellationToken)
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return;

        // Step 3: Prepare mutations
        (bool success, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? mutationsPrepared) = await PrepareMutations(
            context,
            cancellationToken
        );

        if (mutationsPrepared is null)
            return;

        if (!success)
        {
            // Step 4.a: Rollback mutations in the case of failures
            if (context.AsyncRelease)
                _ = RollbackMutations(context, mutationsPrepared);
            else
                await RollbackMutations(context, mutationsPrepared);
            return;
        }

        // Step 4.b: Prepare mutations of successful preparations
        if (context.AsyncRelease)
            _ = CommitMutations(context, mutationsPrepared);
        else
            await CommitMutations(context, mutationsPrepared);
    }

    /// <summary>
    /// Executes a plan to prepare all the mutations for the modified keys in the transaction.
    /// If any of the modified keys cannot be "prepared", the transaction is aborted.
    /// </summary>
    /// <param name="context"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<(bool, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>?)> PrepareMutations(
        KeyValueTransactionContext context,
        CancellationToken cancellationToken
    )
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return (false, null);

        if (!context.SetState(KeyValueTransactionState.Preparing, KeyValueTransactionState.Pending))
            throw new KahunaAbortedException("Failed to set transaction state to Preparing");

        HLCTimestamp highestModifiedTime = context.TransactionId;

        if (context.ModifiedResult?.Type is KeyValueResponseType.Set or KeyValueResponseType.Extended or KeyValueResponseType.Deleted)
        {
            if (context.ModifiedResult.Values is not null)
            {
                foreach (KeyValueTransactionResultValue result in context.ModifiedResult.Values)
                {
                    if (result.LastModified != HLCTimestamp.Zero && result.LastModified > highestModifiedTime)
                        highestModifiedTime = result.LastModified;
                }
            }
        }

        // Request a new unique timestamp for the transaction
        HLCTimestamp commitId = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), highestModifiedTime);

        if (context.ModifiedKeys.Count == 1)
        {
            (string key, KeyValueDurability durability) = context.ModifiedKeys.First();

            (KeyValueResponseType type, HLCTimestamp ticketId, string _, KeyValueDurability _) = await manager.LocateAndTryPrepareMutations(
                context.TransactionId,
                commitId,
                key, durability,
                cancellationToken
            );

            if (!context.SetState(KeyValueTransactionState.Prepared, KeyValueTransactionState.Preparing))
                throw new KahunaAbortedException("Failed to set transaction state to Prepared");

            if (type != KeyValueResponseType.Prepared)
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };

                logger.LogWarning("Couldn't propose {Key} {Response}", key, type);

                return (false, null);
            }

            return (true, [(key, ticketId, durability)]);
        }

        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> proposalResponses = await manager.LocateAndTryPrepareManyMutations(
            context.TransactionId,
            commitId,
            context.ModifiedKeys.ToList(),
            cancellationToken
        );

        if (!context.SetState(KeyValueTransactionState.Prepared, KeyValueTransactionState.Preparing))
            throw new KahunaAbortedException("Failed to set transaction state to Prepared");

        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            List<(string, HLCTimestamp, KeyValueDurability)> preparedMutations = [];

            foreach ((KeyValueResponseType, HLCTimestamp, string, KeyValueDurability) proposalResponse in proposalResponses)
            {
                if (proposalResponse.Item1 == KeyValueResponseType.Prepared)
                    preparedMutations.Add((proposalResponse.Item3, proposalResponse.Item2, proposalResponse.Item4));

                logger.LogWarning("Couldn't propose {Key} {Response}", proposalResponse.Item3, proposalResponse.Item1);
            }

            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };

            return (false, preparedMutations);
        }

        return (true, proposalResponses.Select(r => (r.Item3, r.Item2, r.Item4)).ToList());
    }

    /// <summary>
    /// Commits the prepared mutations to the key-value store.
    /// </summary>
    /// <param name="context">The transaction context containing transaction-specific information.</param>
    /// <param name="mutationsPrepared">A list of tuples representing the prepared mutations, including the key, timestamp, and durability level.</param>
    /// <returns>An asynchronous task representing the commit operation.</returns>
    private async Task CommitMutations(KeyValueTransactionContext context, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared)
    {
        if (mutationsPrepared.Count == 0)
            return;

        if (!context.SetState(KeyValueTransactionState.Committing, KeyValueTransactionState.Prepared))
            throw new KahunaAbortedException("Failed to set transaction state to Committing");

        if (mutationsPrepared.Count == 1)
        {
            (string key, HLCTimestamp ticketId, KeyValueDurability durability) = mutationsPrepared.First();

            (KeyValueResponseType response, long _) = await manager.LocateAndTryCommitMutations(
                context.TransactionId,
                key,
                ticketId,
                durability,
                CancellationToken.None
            );

            if (response != KeyValueResponseType.Committed)
                logger.LogWarning("CommitMutations: {Type} {Key} {TicketId}", response, key, ticketId);

            if (!context.SetState(KeyValueTransactionState.Committed, KeyValueTransactionState.Committing))
                throw new KahunaAbortedException("Failed to set transaction state to Committed");

            return;
        }

        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = await manager.LocateAndTryCommitManyMutations(
            context.TransactionId,
            mutationsPrepared,
            CancellationToken.None
        );

        foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
        {
            if (response != KeyValueResponseType.Committed)
                logger.LogWarning("CommitMutations {Type} {Key} {TicketId} {Durability}", response, key, commitIndex, durability);
        }

        if (!context.SetState(KeyValueTransactionState.Committed, KeyValueTransactionState.Committing))
            throw new KahunaAbortedException("Failed to set transaction state to Committed");
    }

    /// <summary>
    /// Rolls back a collection of mutations associated with a given transaction context.
    /// </summary>
    /// <param name="context">The transaction context containing the details of the transaction.</param>
    /// <param name="mutationsPrepared">The list of mutations prepared for rollback, containing key, ticket identifier, and durability details.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task RollbackMutations(KeyValueTransactionContext context, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared)
    {
        if (mutationsPrepared.Count == 0)
            return;

        if (!context.SetState(KeyValueTransactionState.RollingBack, KeyValueTransactionState.Prepared))
            throw new KahunaAbortedException("Failed to set transaction state to RollingBack");

        if (mutationsPrepared.Count == 1)
        {
            (string key, HLCTimestamp ticketId, KeyValueDurability durability) = mutationsPrepared.First();

            (KeyValueResponseType response, long _) = await manager.LocateAndTryRollbackMutations(
                context.TransactionId,
                key,
                ticketId,
                durability,
                CancellationToken.None
            );

            if (response != KeyValueResponseType.RolledBack)
                logger.LogWarning("RollbackMutations: {Type} {Key} {TicketId}", response, key, ticketId);

            if (!context.SetState(KeyValueTransactionState.RolledBack, KeyValueTransactionState.RollingBack))
                throw new KahunaAbortedException("Failed to set transaction state to RolledBack");

            return;
        }

        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = await manager.LocateAndTryRollbackManyMutations(
            context.TransactionId,
            mutationsPrepared,
            CancellationToken.None
        );

        foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
        {
            if (response != KeyValueResponseType.RolledBack)
                logger.LogWarning("RollbackMutations {Type} {Key} {TicketId} {Durability}", response, key, commitIndex, durability);
        }

        if (!context.SetState(KeyValueTransactionState.RolledBack, KeyValueTransactionState.RollingBack))
            throw new KahunaAbortedException("Failed to set transaction state to RolledBack");
    }

    /// <summary>
    /// Internal method to execute the transaction AST recursively.
    /// </summary>
    /// <param name="context"></param>
    /// <param name="ast"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaScriptException"></exception>
    /// <exception cref="NotImplementedException"></exception>
    private async Task ExecuteTransactionInternal(KeyValueTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        // Multiple sets in a row can be optimized
        if (ast.nodeType == NodeType.StmtList)
        {            
            HashSet<(string, KeyValueDurability)> keys = [];
            
            if (CanBatchBeSetMany(context, ast, keys))
            {
                context.Result = await SetManyCommand.Execute(manager, context, ast, cancellationToken);
                return;
            }
        }
        
        while (true)
        {
            //Console.WriteLine("AST={0} {1}", ast.nodeType, ast.yyline);

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
                    context.Result = await SetCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.Delete:
                    context.Result = await DeleteCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.Extend:
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
                    context.Result = await DeleteCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                case NodeType.Eextend:
                    context.Result = await ExtendCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                case NodeType.GetByPrefix:
                    context.Result = await GetByPrefixCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.EgetByPrefix:
                    context.Result = await GetByPrefixCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
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

    /// <summary>
    /// Executes an "if" stmt
    /// </summary>
    /// <param name="context"></param>
    /// <param name="ast"></param>
    /// <exception cref="Exception"></exception>
    private async Task ExecuteIf(KeyValueTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
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

    /// <summary>
    /// Executes a "for" stmt
    /// </summary>
    /// <param name="context"></param>
    /// <param name="ast"></param>
    /// <exception cref="Exception"></exception>
    private async Task ExecuteFor(KeyValueTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
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
    /// Determines whether a batch of operations can involve multiple set operations.
    /// </summary>
    /// <param name="context">The transaction context containing data related to the current key-value transaction.</param>
    /// <param name="ast">The abstract syntax tree (AST) node representing the operations to be analyzed.</param>
    /// <param name="keys"></param>
    /// <returns>True if a batch can include multiple set operations; otherwise, false.</returns>
    private static bool CanBatchBeSetMany(KeyValueTransactionContext context, NodeAst ast, HashSet<(string, KeyValueDurability)> keys)
    {
        bool areSets = false;        

        while (true)
        {
            //Console.Error.WriteLine("AST={0}", ast.nodeType);

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
}