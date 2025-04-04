
using Kommander;
using Kommander.Time;
//using Kommander.Support.Parallelization;

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
    private const int LockMaxDegreeOfParallelism = 5;
    
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;
    
    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;
    
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
            NodeAst ast = ScriptParserProcessor.Parse(script, hash, logger);

            switch (ast.nodeType)
            {
                case NodeType.Set:
                    return await SetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Get:
                    return await GetCommand.Execute(manager, GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);
                
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

                case NodeType.Begin:
                    return await ExecuteTransaction(ast.leftAst!, ast.rightAst, parameters, false);

                case NodeType.StmtList:
                case NodeType.Let:
                case NodeType.IntegerType:
                case NodeType.StringType:
                case NodeType.FloatType:
                case NodeType.BooleanType:
                case NodeType.Identifier:
                case NodeType.If:
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
                case NodeType.FuncCall:
                case NodeType.ArgumentList:
                case NodeType.Return:
                case NodeType.Sleep:
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
            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message + " at line " + ex.Line };
        }
        catch (KahunaAbortedException ex)
        {
            return new() { Type = KeyValueResponseType.Aborted, Reason = ex.Message };
        }
        catch (TaskCanceledException)
        {
            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (OperationCanceledException)
        {
            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (Exception ex)
        {
            logger.LogError("TryExecuteTx: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
            
            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message };
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
        bool asyncRelease = true;
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
        HLCTimestamp transactionId = raft.HybridLogicalClock.SendOrLocalEvent();
        
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

        // Acquire all locks in advance for pessimistic locking
        if (locking == KeyValueTransactionLocking.Pessimistic)
            KeyValueLockHelper.GetLocksToAcquire(context, ast, ephemeralLocksToAcquire, persistentLocksToAcquire);

        try
        {
            // Step 1: Acquire locks in advance for pessimistic locking
            if (locking == KeyValueTransactionLocking.Pessimistic)
                await AcquireLocksPessimistically(context, ephemeralLocksToAcquire, persistentLocksToAcquire, timeout, cts.Token);

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
            return new() { Type = KeyValueResponseType.Errored, Reason = ex.Message + " at line " + ex.Line };
        }
        catch (KahunaAbortedException ex)
        {
            return new() { Type = KeyValueResponseType.Aborted, Reason = ex.Message };
        }
        catch (TaskCanceledException)
        {
            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (OperationCanceledException)
        {
            return new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction aborted by timeout" };
        }
        catch (Exception ex)
        {
            return new() { Type = KeyValueResponseType.Errored, Reason = ex.GetType().Name + ": " + ex.Message };
        }
        finally
        {
            // Final Step: Release locks
            if (context.AsyncRelease)
                _ = ReleaseAcquiredLocks(context);
            else
                await ReleaseAcquiredLocks(context);
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
    private async Task AcquireLocksPessimistically(KeyValueTransactionContext context, HashSet<string> ephemeralLocksToAcquire, HashSet<string> persistentLocksToAcquire, int timeout, CancellationToken ctsToken)
    {
        int numberLocks = ephemeralLocksToAcquire.Count + persistentLocksToAcquire.Count;

        if (numberLocks == 0)
            return;
        
        context.LocksAcquired = [];

        if (numberLocks == 1)
        {
            if (ephemeralLocksToAcquire.Count > 0)
            {
                (KeyValueResponseType acquireResponse, string keyName, KeyValueDurability durability) = await manager.LocateAndTryAcquireExclusiveLock(context.TransactionId, ephemeralLocksToAcquire.First(), timeout + 10, KeyValueDurability.Ephemeral, ctsToken);
                
                if (acquireResponse != KeyValueResponseType.Locked) 
                    throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);

                context.LocksAcquired.Add((keyName, durability));
                return;
            }
            
            if (persistentLocksToAcquire.Count > 0)
            {
                (KeyValueResponseType acquireResponse, string keyName, KeyValueDurability durability) = await manager.LocateAndTryAcquireExclusiveLock(context.TransactionId, persistentLocksToAcquire.First(), timeout + 10, KeyValueDurability.Persistent, ctsToken);
                
                if (acquireResponse != KeyValueResponseType.Locked) 
                    throw new KahunaAbortedException("Failed to acquire lock: " + keyName + " " + durability);

                context.LocksAcquired.Add((keyName, durability));
                return;
            }
        }
                
        List<(string, int, KeyValueDurability)> keysToLock = new(numberLocks);

        foreach (string key in ephemeralLocksToAcquire)
            keysToLock.Add((key, timeout + 10, KeyValueDurability.Ephemeral));

        foreach (string key in persistentLocksToAcquire)
            keysToLock.Add((key, timeout + 10, KeyValueDurability.Persistent));

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
            
            if (context.LocksAcquired.Count == 1)
            {
                (string lockKey, KeyValueDurability durability) = context.LocksAcquired.First();
                
                await manager.LocateAndTryReleaseExclusiveLock(context.TransactionId, lockKey, durability, CancellationToken.None);
                return;
            }
            
            await manager.LocateAndTryReleaseManyExclusiveLocks(context.TransactionId, context.LocksAcquired, CancellationToken.None);
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
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid BEGIN option", ast.yyline);
                    
                    if (ast.leftAst.yytext is null)
                        throw new KahunaScriptException("Invalid BEGIN option", ast.yyline);
                    
                    if (ast.rightAst is null)
                        throw new KahunaScriptException("Invalid BEGIN option", ast.yyline);
                    
                    if (ast.rightAst.yytext is null)
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
        (bool success, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? mutationsPrepared) = await PrepareMutations(context, cancellationToken);
        
        if (!success)
            return;

        if (mutationsPrepared is null)
            return;

        await CommitMutations(context, mutationsPrepared);
    }

    /// <summary>
    /// Executes a plan to prepare all the mutations for the modified keys in the transaction.
    /// If any of the modified keys cannot be "prepared", the transaction is aborted.
    /// </summary>
    /// <param name="context"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<(bool, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>?)> PrepareMutations(KeyValueTransactionContext context, CancellationToken cancellationToken)
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return (false, null);
        
        if (context.ModifiedKeys.Count == 1)
        {
            (string key, KeyValueDurability durability) = context.ModifiedKeys.First();
            
            (KeyValueResponseType type, HLCTimestamp ticketId, string _, KeyValueDurability _) = await manager.LocateAndTryPrepareMutations(context.TransactionId, key, durability, cancellationToken);

            if (type != KeyValueResponseType.Prepared)
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };
                return (false, null);
            }

            return (true, [(key, ticketId, durability)]);
        }
        
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> proposalResponses = await manager.LocateAndTryPrepareManyMutations(context.TransactionId, context.ModifiedKeys, cancellationToken);
    
        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            foreach ((KeyValueResponseType, HLCTimestamp, string, KeyValueDurability) proposalResponse in proposalResponses)
            {
                if (proposalResponse.Item1 == KeyValueResponseType.Prepared)
                    await manager.LocateAndTryRollbackMutations(context.TransactionId, proposalResponse.Item3, proposalResponse.Item2, proposalResponse.Item4, cancellationToken);

                Console.WriteLine("{0} {1}", proposalResponse.Item3, proposalResponse.Item1);
            }

            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };
            return (false, null);
        }

        return (true, proposalResponses.Select(r => (r.Item3, r.Item2, r.Item4)).ToList());
    }
    
    /// <summary>
    /// Send the commit request to all involved nodes.
    /// </summary>
    /// <param name="context"></param>
    /// <param name="mutationsPrepared"></param>
    private async Task CommitMutations(KeyValueTransactionContext context, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared)
    {
        if (mutationsPrepared.Count == 0)
            return;

        if (mutationsPrepared.Count == 1)
        {
            (string key, HLCTimestamp ticketId, KeyValueDurability durability) = mutationsPrepared.First();
            
            (KeyValueResponseType response, long commitIndex) = await manager.LocateAndTryCommitMutations(context.TransactionId, key, ticketId, durability, CancellationToken.None);
            
            if (response != KeyValueResponseType.Committed)
                logger.LogWarning("CommitMutations: {Type} {Key} {TicketId}", response, key, ticketId);
            
            return;
        }
        
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = await manager.LocateAndTryCommitManyMutations(context.TransactionId, mutationsPrepared, CancellationToken.None);
        
        foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
        {
            if (response != KeyValueResponseType.Committed)
                logger.LogWarning("CommitMutations {Type} {Key} {TicketId} {Durability}", response, key, commitIndex, durability);
        }
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
        while (true)
        {
            //Console.WriteLine("AST={0} {1}", ast.nodeType, ast.yyline);
            
            if (context.Status == KeyValueExecutionStatus.Stop)
                break;

            if (cancellationToken.IsCancellationRequested)
                break;
            
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
                
                case NodeType.Let:
                {
                    LetCommand.Execute(context, ast);
                    break;
                }
                
                case NodeType.Set:
                    await SetCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                
                case NodeType.Delete:
                    await DeleteCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                
                case NodeType.Extend:
                    await ExtendCommand.Execute(manager, context, ast, KeyValueDurability.Persistent, cancellationToken);
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
                    await SetCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
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
                    await DeleteCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;
                
                case NodeType.Eextend:
                    await ExtendCommand.Execute(manager, context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;
                
                case NodeType.Commit:
                    context.Action = KeyValueTransactionAction.Commit;
                    break;
                
                case NodeType.Rollback:
                    context.Action = KeyValueTransactionAction.Abort;
                    break;
                
                case NodeType.Return:
                    if (ast.leftAst is not null)
                        context.Result = KeyValueTransactionExpression.Eval(context, ast.leftAst).ToTransactionResult();
                    
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;
                
                case NodeType.Sleep:
                    await SleepCommand.Execute(ast, cancellationToken);
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
                case NodeType.FuncCall:
                case NodeType.ArgumentList:
                    context.Result = KeyValueTransactionExpression.Eval(context, ast).ToTransactionResult();
                    break;
                    
                case NodeType.SetNotExists:
                case NodeType.SetExists:
                case NodeType.SetCmp:
                case NodeType.SetCmpRev:
                    break;
                
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
}