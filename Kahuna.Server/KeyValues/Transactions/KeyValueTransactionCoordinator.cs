
using System.Text;
using Kommander;
using Kahuna.Server.Configuration;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander.Support;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

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
public sealed class KeyValueTransactionCoordinator
{
    private const int LockMaxDegreeOfParallelism = 5;
    
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;
    
    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;
    
    public KeyValueTransactionCoordinator(KeyValuesManager manager, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
    }

    public async Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash, List<KeyValueParameter>? parameters)
    {
        try
        {
            NodeAst ast = ScriptParserProcessor.Parse(script, hash);

            switch (ast.nodeType)
            {
                case NodeType.Set:
                    return await ExecuteSet(GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Get:
                    return await ExecuteGet(GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);
                
                case NodeType.Exists:
                    return await ExecuteExists(GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Delete:
                    return await ExecuteDelete(GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Extend:
                    return await ExecuteExtend(GetTempTransactionContext(parameters), ast, KeyValueDurability.Persistent, CancellationToken.None);

                case NodeType.Eset:
                    return await ExecuteSet(GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Eget:
                    return await ExecuteGet(GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);
                
                case NodeType.Eexists:
                    return await ExecuteExists(GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Edelete:
                    return await ExecuteDelete(GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

                case NodeType.Eextend:
                    return await ExecuteExtend(GetTempTransactionContext(parameters), ast, KeyValueDurability.Ephemeral, CancellationToken.None);

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

    private static KeyValueTransactionContext GetTempTransactionContext(List<KeyValueParameter>? parameters)
    {
        return new()
        {
            TransactionId = HLCTimestamp.Zero,
            Locking = KeyValueTransactionLocking.Pessimistic,
            Action = KeyValueTransactionAction.Commit,
            Parameters = parameters
        };
    }

    private async Task<KeyValueTransactionResult> ExecuteSet(KeyValueTransactionContext context, NodeAst ast, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.leftAst.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid value", ast.yyline);
        
        string keyName = GetKeyName(context, ast.leftAst);

        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            lock (context.LockSync)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((keyName, durability));
            }
        }

        int expiresMs = 0;
        
        if (ast.extendedOne is not null)
            expiresMs = int.Parse(ast.extendedOne.yytext!);

        KeyValueFlags flags = KeyValueFlags.Set;

        if (ast.extendedTwo is not null)
        {
            if (ast.extendedTwo.nodeType == NodeType.SetNotExists)
                flags = KeyValueFlags.SetIfNotExists;
            
            if (ast.extendedTwo.nodeType == NodeType.SetExists)
                flags = KeyValueFlags.SetIfExists;
        }
        
        long compareRevision = 0;
        byte[]? compareValue = null;

        if (ast.extendedThree is not null)
        {
            if (ast.extendedThree.leftAst is null)
                throw new KahunaScriptException("Invalid SET cmp/cmprev", ast.yyline);

            if (ast.extendedThree.nodeType == NodeType.SetCmp)
            {
                flags = KeyValueFlags.SetIfEqualToValue;
                compareValue = KeyValueTransactionExpression.Eval(context, ast.extendedThree.leftAst).ToBytes();
            }
            
            if (ast.extendedThree.nodeType == NodeType.SetCmpRev)
            {
                flags = KeyValueFlags.SetIfEqualToRevision;
                compareRevision = KeyValueTransactionExpression.Eval(context, ast.extendedThree.leftAst).ToLong();
            }
        }
        
        KeyValueTransactionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst).ToTransactionResult();
        
        (KeyValueResponseType type, long revision) = await manager.LocateAndTrySetKeyValue(
            context.TransactionId,
            key: keyName,
            value: result.Value,
            compareValue,
            compareRevision,
            flags,
            expiresMs,
            durability,
            cancellationToken
        );

        if (type == KeyValueResponseType.Set)
        {
            context.ModifiedKeys ??= [];
            context.ModifiedKeys.Add((keyName, durability));
        }
        else
        {
            if (type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry)
            {
                context.Action = KeyValueTransactionAction.Abort;
                context.Status = KeyValueExecutionStatus.Stop;
            }    
        }

        context.Result = new()
        {
            Type = type,
            Revision = revision
        };

        return new()
        {
            ServedFrom = "",
            Type = type,
            Revision = revision
        };
    }
    
    private async Task<KeyValueTransactionResult> ExecuteDelete(KeyValueTransactionContext context, NodeAst ast, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.leftAst.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);

        string keyName = GetKeyName(context, ast.leftAst);

        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            lock (context.LockSync)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((keyName, durability));
            }
        }

        (KeyValueResponseType type, long revision) = await manager.LocateAndTryDeleteKeyValue(
            context.TransactionId,
            key: keyName,
            durability,
            cancellationToken
        );
        
        switch (type)
        {
            case KeyValueResponseType.Deleted:
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((keyName, durability));
                break;
            
            case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                context.Action = KeyValueTransactionAction.Abort;
                context.Status = KeyValueExecutionStatus.Stop;
                break;
        }
        
        context.Result = new()
        {
            Type = type,
            Revision = revision
        };

        return new()
        {
            ServedFrom = "",
            Type = type,
            Revision = revision
        };
    }
    
    private async Task<KeyValueTransactionResult> ExecuteExtend(KeyValueTransactionContext context, NodeAst ast, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.leftAst.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        string keyName = GetKeyName(context, ast.leftAst);
        
        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            lock (context.LockSync)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((keyName, durability));
            }
        }
        
        int expiresMs = 0;
        
        if (ast.rightAst is not null)
            expiresMs = int.Parse(ast.rightAst.yytext!);
        
        (KeyValueResponseType type, long revision) = await manager.LocateAndTryExtendKeyValue(
            context.TransactionId,
            key: keyName,
            expiresMs: expiresMs,
            durability,
            cancellationToken
        );
        
        switch (type)
        {
            case KeyValueResponseType.Extended:
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((keyName, durability));
                break;
            
            case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                context.Action = KeyValueTransactionAction.Abort;
                context.Status = KeyValueExecutionStatus.Stop;
                break;
        }
        
        context.Result = new()
        {
            Type = type,
            Revision = revision
        };

        return new()
        {
            ServedFrom = "",
            Type = type,
            Revision = revision
        };
    }
    
    private async Task<KeyValueTransactionResult> ExecuteGet(KeyValueTransactionContext context, NodeAst ast, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.leftAst.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        string keyName = GetKeyName(context, ast.leftAst);
        
        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            lock (context.LockSync)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((keyName, durability));
            }
        }
        
        long compareRevision = -1;
        
        if (ast.extendedOne is not null)
            compareRevision = int.Parse(ast.extendedOne.yytext!);
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? readOnlyContext) = await manager.LocateAndTryGetValue(
            context.TransactionId,
            keyName,
            compareRevision,
            durability,
            cancellationToken
        );
        
        if (type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry)
        {
            context.Action = KeyValueTransactionAction.Abort;
            context.Status = KeyValueExecutionStatus.Stop;
        }

        if (readOnlyContext is null)
        {
            if (ast.rightAst is not null)
                context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new() { Type = KeyValueExpressionType.NullType });
            
            return new()
            {
                ServedFrom = "",
                Type = type
            };
        }
        
        if (ast.rightAst is not null)
            context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new()
            {
                Type = KeyValueExpressionType.StringType, 
                StrValue = Encoding.UTF8.GetString(readOnlyContext.Value ?? []),
                Revision = readOnlyContext.Revision
            });
            
        return new()
        {
            ServedFrom = "",
            Type = type,
            Value = readOnlyContext.Value,
            Revision = readOnlyContext.Revision,
            Expires = readOnlyContext.Expires
        };
    }
    
    private async Task<KeyValueTransactionResult> ExecuteExists(KeyValueTransactionContext context, NodeAst ast, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.leftAst.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        string keyName = GetKeyName(context, ast.leftAst);
        
        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            lock (context.LockSync)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((keyName, durability));
            }
        }
        
        long compareRevision = -1;
        
        if (ast.extendedOne is not null)
            compareRevision = int.Parse(ast.extendedOne.yytext!);
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? readOnlyContext) = await manager.LocateAndTryExistsValue(
            context.TransactionId,
            keyName,
            compareRevision,
            durability,
            cancellationToken
        );
        
        if (type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry)
        {
            context.Action = KeyValueTransactionAction.Abort;
            context.Status = KeyValueExecutionStatus.Stop;
        }

        if (readOnlyContext is null)
        {
            if (ast.rightAst is not null)
                context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new() { Type = KeyValueExpressionType.NullType });
            
            return new()
            {
                ServedFrom = "",
                Type = type
            };
        }
        
        if (ast.rightAst is not null)
            context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new()
            {
                Type = KeyValueExpressionType.BoolType, 
                BoolValue = type == KeyValueResponseType.Exists,
                Revision = readOnlyContext.Revision
            });
            
        return new()
        {
            ServedFrom = "",
            Type = type,
            Value = readOnlyContext.Value,
            Revision = readOnlyContext.Revision,
            Expires = readOnlyContext.Expires
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
        long timeout = configuration.DefaultTransactionTimeout;
        using CancellationTokenSource cts = new();
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
                    _ => throw new KahunaScriptException("Unsupported autoCommit option: " + optionValue, optionsAst.yyline)
                };
            }
            
            if (options.TryGetValue("timeout", out optionValue))
            {
                if (!long.TryParse(optionValue, out timeout))
                    throw new KahunaScriptException("Invalid timeout option: " + timeout, optionsAst.yyline);
            }
        }
        
        cts.CancelAfter(TimeSpan.FromMilliseconds(timeout));
        
        // Need HLC timestamp for the transaction id
        HLCTimestamp transactionId = raft.HybridLogicalClock.SendOrLocalEvent();
        
        KeyValueTransactionContext context = new()
        {
            TransactionId = transactionId,
            Locking = locking,
            Action = autoCommit ? KeyValueTransactionAction.Commit : KeyValueTransactionAction.Abort,
            Result = new() { Type = KeyValueResponseType.Aborted },
            Parameters = parameters
        };
        
        HashSet<string> ephemeralLocksToAcquire = [];
        HashSet<string> linearizableLocksToAcquire = [];

        // Acquire all locks in advance for pessimistic locking
        if (locking == KeyValueTransactionLocking.Pessimistic)
            GetLocksToAcquire(context, ast, ephemeralLocksToAcquire, linearizableLocksToAcquire);

        try
        {
            // Step 1: Acquire locks in advance for pessimistic locking
            if (locking == KeyValueTransactionLocking.Pessimistic)
            {
                context.LocksAcquired = [];
                
                await linearizableLocksToAcquire.ForEachAsync(LockMaxDegreeOfParallelism, async key =>
                {
                    (KeyValueResponseType response, string _lockedKey, KeyValueDurability _durability) = await manager.LocateAndTryAcquireExclusiveLock(transactionId, key, 5050, KeyValueDurability.Persistent, cts.Token);
                    
                    if (response == KeyValueResponseType.Locked)
                    {
                        lock (context.LockSync) context.LocksAcquired.Add((_lockedKey, _durability));
                        return;
                    }

                    throw new KahunaAbortedException("Failed to acquire locks");
                });
                
                await ephemeralLocksToAcquire.ForEachAsync(LockMaxDegreeOfParallelism, async key =>
                {
                    (KeyValueResponseType response, string _lockedKey, KeyValueDurability _durability) = await manager.LocateAndTryAcquireExclusiveLock(transactionId, key, 5050, KeyValueDurability.Ephemeral, cts.Token);

                    if (response == KeyValueResponseType.Locked)
                    {
                        lock (context.LockSync) context.LocksAcquired.Add((_lockedKey, _durability));
                        return;
                    }

                    throw new KahunaAbortedException("Failed to acquire locks");
                });
            }

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
            if (context.LocksAcquired is not null && context.LocksAcquired.Count > 0)
            {
                // Final Step: Release locks
                await context.LocksAcquired.ForEachAsync(LockMaxDegreeOfParallelism, async ((string key, KeyValueDurability durability) p) =>
                {
                    await manager.LocateAndTryReleaseExclusiveLock(transactionId, p.key, p.durability, cts.Token);
                });
            }
        }
    }

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

    private async Task TwoPhaseCommit(KeyValueTransactionContext context, CancellationToken cancellationToken)
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null)
            return;

        // Step 3: Prepare mutations
        
        object syncObject = new();
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> proposalResponses = [];
        
        await context.ModifiedKeys.ForEachAsync(LockMaxDegreeOfParallelism, async ((string key, KeyValueDurability durability) p) =>
        {
            (KeyValueResponseType response, HLCTimestamp ticketId, string _lockedKey, KeyValueDurability _durability) = await manager.LocateAndTryPrepareMutations(context.TransactionId, p.key, p.durability, cancellationToken);
            
            lock (syncObject)
                proposalResponses.Add((response, ticketId, _lockedKey, _durability));
            
        });
        
        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            foreach ((KeyValueResponseType, HLCTimestamp, string, KeyValueDurability) proposalResponse in proposalResponses)
            {
                if (proposalResponse.Item1 == KeyValueResponseType.Prepared)
                    await manager.LocateAndTryRollbackMutations(context.TransactionId, proposalResponse.Item3, proposalResponse.Item2, proposalResponse.Item4, cancellationToken);
                
                Console.WriteLine("{0} {1}", proposalResponse.Item3, proposalResponse.Item1);
            }

            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };
            return;
        }
        
        /*List<Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> proposalTasks = new(context.LocksAcquired.Count);
        
        foreach ((string key, KeyValueDurability durability) in context.ModifiedKeys)
            proposalTasks.Add(manager.LocateAndTryPrepareMutations(context.TransactionId, key, durability, cancellationToken));

        (KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)[] proposalResponses = await Task.WhenAll(proposalTasks);

        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            foreach ((KeyValueResponseType, HLCTimestamp, string, KeyValueDurability) proposalResponse in proposalResponses)
            {
                if (proposalResponse.Item1 == KeyValueResponseType.Prepared)
                    await manager.LocateAndTryRollbackMutations(context.TransactionId, proposalResponse.Item3, proposalResponse.Item2, proposalResponse.Item4, cancellationToken);
                
                Console.WriteLine("{0} {1}", proposalResponse.Item3, proposalResponse.Item1);
            }

            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };
            return;
        }*/

        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared = proposalResponses.Select(r => (r.Item3, r.Item2, r.Item4)).ToList();

        List<Task<(KeyValueResponseType, long)>> commitTasks = new(context.LocksAcquired.Count);

        // Step 4: Commit mutations
        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) in mutationsPrepared)
            commitTasks.Add(manager.LocateAndTryCommitMutations(context.TransactionId, key, ticketId, durability, cancellationToken));

        await Task.WhenAll(commitTasks);
    }

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
                    ExecuteLet(context, ast);
                    break;
                }
                
                case NodeType.Set:
                    await ExecuteSet(context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                
                case NodeType.Delete:
                    await ExecuteDelete(context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                
                case NodeType.Extend:
                    await ExecuteExtend(context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;

                case NodeType.Get:
                {
                    context.Result = await ExecuteGet(context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                }
                
                case NodeType.Exists:
                {
                    context.Result = await ExecuteExists(context, ast, KeyValueDurability.Persistent, cancellationToken);
                    break;
                }
                
                case NodeType.Eset:
                    await ExecuteSet(context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;

                case NodeType.Eget:
                {
                    context.Result = await ExecuteGet(context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;
                }
                
                case NodeType.Eexists:
                {
                    context.Result = await ExecuteExists(context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;
                }
                
                case NodeType.Edelete:
                    await ExecuteDelete(context, ast, KeyValueDurability.Ephemeral, cancellationToken);
                    break;
                
                case NodeType.Eextend:
                    await ExecuteExtend(context, ast, KeyValueDurability.Ephemeral, cancellationToken);
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
                    await ExecuteSleep(ast, cancellationToken);
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

    private static void ExecuteLet(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid LET expression", ast.yyline);
        
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid LET expression", ast.yyline);
        
        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        context.Result = result.ToTransactionResult();
        
        context.SetVariable(ast.leftAst, ast.leftAst.yytext!, result);
    }
    
    private static async Task ExecuteSleep(NodeAst ast, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid SLEEP duration", ast.yyline);
                    
        if (!long.TryParse(ast.leftAst.yytext, out long duration))
            throw new KahunaScriptException($"Invalid SLEEP duration {duration}", ast.yyline);
                    
        if (duration is < 0 or > 300000)
            throw new KahunaScriptException($"Invalid SLEEP duration {duration}", ast.yyline);
                    
        await Task.Delay(TimeSpan.FromMilliseconds(duration), cancellationToken);
    }

    private static string GetKeyName(KeyValueTransactionContext context, NodeAst ast)
    {
        return ast.nodeType switch
        {
            NodeType.Identifier => ast.yytext!,
            NodeType.Placeholder => context.GetParameter(ast),
            _ => throw new KahunaScriptException($"Invalid key name type {ast.nodeType}", ast.yyline)
        };
    }

    /// <summary>
    /// Obtains that must be acquired to start the transaction
    /// </summary>
    /// <param name="context"></param>
    /// <param name="ast"></param>
    /// <param name="ephemeralLocks"></param>
    /// <param name="linearizableLocks"></param>
    /// <exception cref="KahunaScriptException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    private static void GetLocksToAcquire(KeyValueTransactionContext context, NodeAst ast, HashSet<string> ephemeralLocks, HashSet<string> linearizableLocks)
    {
        while (true)
        {
            //Console.WriteLine("AST={0}", ast.nodeType);
            
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null) 
                        GetLocksToAcquire(context, ast.leftAst, ephemeralLocks, linearizableLocks);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.Begin:
                    if (ast.leftAst is not null) 
                        GetLocksToAcquire(context, ast.leftAst, ephemeralLocks, linearizableLocks);
                    break;
                
                case NodeType.If:
                {
                    if (ast.rightAst is not null) 
                        GetLocksToAcquire(context, ast.rightAst, ephemeralLocks, linearizableLocks);
                    
                    if (ast.extendedOne is not null) 
                        GetLocksToAcquire(context, ast.extendedOne, ephemeralLocks, linearizableLocks);

                    break;
                }
                
                case NodeType.Set:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET expression", ast.yyline);
                    
                    linearizableLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                    
                case NodeType.Eset:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET expression", ast.yyline);
                    
                    ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Get:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET expression", ast.yyline);
                    
                    if (ast.extendedOne is null) // make sure if isn't querying a revision
                        linearizableLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                    
                case NodeType.Eget:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid GET expression", ast.yyline);
                    
                    if (ast.extendedOne is null) // make sure if isn't querying a revision
                        ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Extend:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid EXTEND expression", ast.yyline);
                    
                    linearizableLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                    
                case NodeType.Eextend:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid EXTEND expression", ast.yyline);
                    
                    ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Delete:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid DELETE expression", ast.yyline);
                    
                    linearizableLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Edelete:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid DELETE expression", ast.yyline);
                    
                    ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.IntegerType:
                case NodeType.StringType:
                case NodeType.FloatType:
                case NodeType.BooleanType:
                case NodeType.Identifier:
                case NodeType.Let:
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
                case NodeType.SetNotExists:
                case NodeType.SetExists:
                case NodeType.SetCmp:
                case NodeType.SetCmpRev:
                case NodeType.Rollback:
                case NodeType.Commit:
                case NodeType.Return:
                case NodeType.Sleep:
                    break;
                
                default:
                    throw new ArgumentOutOfRangeException();
            }

            break;
        }
    }
}