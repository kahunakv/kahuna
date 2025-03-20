
using Kommander;
using Kahuna.Server.Configuration;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
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
/// -Commit Phase: Once all prewrites succeed, the coordinator commits the transaction by first committing the primary key
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
    
    public async Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash)
    {
        NodeAst ast = ScriptParserProcessor.Parse(script, hash);

        switch (ast.nodeType)
        {
            case NodeType.Set:
                return await ExecuteSet(GetTempTransactionContext(), ast, KeyValueConsistency.Linearizable, CancellationToken.None);

            case NodeType.Get:
                return await ExecuteGet(GetTempTransactionContext(), ast, KeyValueConsistency.Linearizable, CancellationToken.None);
            
            case NodeType.Delete:
                return await ExecuteDelete(GetTempTransactionContext(), ast, KeyValueConsistency.Linearizable, CancellationToken.None);
            
            case NodeType.Extend:
                return await ExecuteExtend(GetTempTransactionContext(), ast, KeyValueConsistency.Linearizable, CancellationToken.None);
            
            case NodeType.Eset:
                return await ExecuteSet(GetTempTransactionContext(), ast, KeyValueConsistency.Ephemeral, CancellationToken.None);
            
            case NodeType.Eget:
                return await ExecuteGet(GetTempTransactionContext(), ast, KeyValueConsistency.Ephemeral, CancellationToken.None);
            
            case NodeType.Edelete:
                return await ExecuteDelete(GetTempTransactionContext(), ast, KeyValueConsistency.Ephemeral, CancellationToken.None);
            
            case NodeType.Eextend:
                return await ExecuteExtend(GetTempTransactionContext(), ast, KeyValueConsistency.Ephemeral, CancellationToken.None);
            
            case NodeType.Begin:
                return await ExecuteTransaction(ast.leftAst!, false);
            
            case NodeType.StmtList:
            case NodeType.Integer:
            case NodeType.String:
            case NodeType.Float:
            case NodeType.Boolean:
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
                return await ExecuteTransaction(ast, true);
            
            case NodeType.SetNotExists:
            case NodeType.SetExists:
                break;
            
            case NodeType.Rollback:
            case NodeType.Commit:
                throw new Exception("Invalid transaction");
            
            default:
                throw new NotImplementedException();
        }
        
        return new() { Type = KeyValueResponseType.Errored };
    }

    private static KeyValueTransactionContext GetTempTransactionContext()
    {
        return new()
        {
            TransactionId = HLCTimestamp.Zero,
            Action = KeyValueTransactionAction.Commit
        };
    }

    private async Task<KeyValueTransactionResult> ExecuteSet(KeyValueTransactionContext context, NodeAst ast, KeyValueConsistency consistency, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        if (ast.rightAst is null)
            throw new Exception("Invalid value");

        int expiresMs = 0;
        
        if (ast.extendedOne is not null)
            expiresMs = int.Parse(ast.extendedOne.yytext!);
        
        KeyValueTransactionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst).ToTransactionRsult();
        
        (KeyValueResponseType type, long revision) = await manager.LocateAndTrySetKeyValue(
            context.TransactionId,
            key: ast.leftAst.yytext,
            value: result.Value,
            null,
            0,
            KeyValueFlags.Set,
            expiresMs,
            consistency,
            cancellationToken
        );

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
    
    private async Task<KeyValueTransactionResult> ExecuteDelete(KeyValueTransactionContext context, NodeAst ast, KeyValueConsistency consistency, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        (KeyValueResponseType type, long revision) = await manager.LocateAndTryDeleteKeyValue(
            context.TransactionId,
            key: ast.leftAst.yytext,
            consistency,
            cancellationToken
        );
        
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
    
    private async Task<KeyValueTransactionResult> ExecuteExtend(KeyValueTransactionContext context, NodeAst ast, KeyValueConsistency consistency, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        int expiresMs = 0;
        
        if (ast.rightAst is not null)
            expiresMs = int.Parse(ast.rightAst.yytext!);
        
        (KeyValueResponseType type, long revision) = await manager.LocateAndTryExtendKeyValue(
            context.TransactionId,
            key: ast.leftAst.yytext,
            expiresMs: expiresMs,
            consistency,
            cancellationToken
        );
        
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
    
    private async Task<KeyValueTransactionResult> ExecuteGet(KeyValueTransactionContext context, NodeAst ast, KeyValueConsistency consistency, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? readOnlyContext) = await manager.LocateAndTryGetValue(
            context.TransactionId,
            ast.leftAst.yytext,
            consistency,
            cancellationToken
        );

        if (readOnlyContext is null)
        {
            if (ast.rightAst is not null)
                context.SetVariable(ast.rightAst.yytext!, new() { Type = KeyValueResponseType.Get, Value = null });
            
            return new()
            {
                ServedFrom = "",
                Type = type
            };
        }
        
        if (ast.rightAst is not null)
            context.SetVariable(ast.rightAst.yytext!, new()
            {
                Type = KeyValueResponseType.Get, 
                Value = readOnlyContext.Value, 
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
    ///
    /// The autoCommit flag offers an specific commit/rollback instruction or an automatic commit behavior on success.
    /// </summary>
    /// <param name="ast"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    private async Task<KeyValueTransactionResult> ExecuteTransaction(NodeAst ast, bool autoCommit)
    {
        using CancellationTokenSource cts = new();
        
        cts.CancelAfter(TimeSpan.FromMilliseconds(5000));
        
        HLCTimestamp transactionId = await raft.HybridLogicalClock.SendOrLocalEvent();
        
        KeyValueTransactionContext context = new()
        {
            TransactionId = transactionId,
            Action = autoCommit ? KeyValueTransactionAction.Commit : KeyValueTransactionAction.Abort,
            Result = new() { Type = KeyValueResponseType.Aborted }
        };
        
        HashSet<string> locksToAcquire = [];

        GetLocksToAcquire(ast, locksToAcquire);

        try
        {
            List<Task<(KeyValueResponseType, string)>> acquireLocksTasks = new(locksToAcquire.Count);
            
            // Step 1: Acquire locks
            foreach (string key in locksToAcquire)
                acquireLocksTasks.Add(manager.LocateAndTryAcquireExclusiveLock(transactionId, key, 5250, KeyValueConsistency.Linearizable, cts.Token));
            
            (KeyValueResponseType, string)[] acquireResponses = await Task.WhenAll(acquireLocksTasks);

            if (acquireResponses.Any(r => r.Item1 != KeyValueResponseType.Locked))
            {
                foreach ((KeyValueResponseType, string) proposalResponse in acquireResponses)
                    Console.WriteLine("{0} {1}", proposalResponse.Item2, proposalResponse.Item1);
                
                return new() { Type = KeyValueResponseType.Aborted };
            }

            context.LocksAcquired = acquireResponses.Select(r => r.Item2).ToList();
            
            // Step 2: Execute transaction
            await ExecuteTransactionInternal(context, ast, cts.Token);

            if (context.Action == KeyValueTransactionAction.Commit)
            {
                await TwoPhaseCommit(context, cts.Token);

                if (context.Result?.Type == KeyValueResponseType.Aborted)
                    return new() { Type = KeyValueResponseType.Aborted };
                
                return context.Result ?? new() { Type = KeyValueResponseType.Errored };
            }
            
            return new() { Type = KeyValueResponseType.Aborted };
        }
        finally
        {
            if (context.LocksAcquired is not null && context.LocksAcquired.Count > 0)
            {
                List<Task<(KeyValueResponseType, string)>> releaseLocksTasks = new(locksToAcquire.Count);

                // Final Step: Release locks
                foreach (string key in context.LocksAcquired) 
                    releaseLocksTasks.Add(manager.LocateAndTryReleaseExclusiveLock(transactionId, key, KeyValueConsistency.Linearizable, cts.Token));

                await Task.WhenAll(releaseLocksTasks);
            }
        }
    }

    private async Task TwoPhaseCommit(KeyValueTransactionContext context, CancellationToken cancellationToken)
    {
        if (context.LocksAcquired is null)
            return;
        
        List<Task<(KeyValueResponseType, HLCTimestamp, string)>> proposalTasks = new(context.LocksAcquired.Count);

        // Step 3: Prepare mutations
        foreach (string key in context.LocksAcquired)
            proposalTasks.Add(manager.LocateAndTryPrepareMutations(context.TransactionId, key, KeyValueConsistency.Linearizable, cancellationToken));

        (KeyValueResponseType, HLCTimestamp, string)[] proposalResponses = await Task.WhenAll(proposalTasks);

        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            foreach ((KeyValueResponseType, HLCTimestamp, string) proposalResponse in proposalResponses)
            {
                if (proposalResponse.Item1 == KeyValueResponseType.Prepared)
                    await manager.LocateAndTryRollbackMutations(context.TransactionId, proposalResponse.Item3, proposalResponse.Item2, KeyValueConsistency.Linearizable, cancellationToken);
                
                Console.WriteLine("{0} {1}", proposalResponse.Item3, proposalResponse.Item1);
            }

            context.Result = new() { Type = KeyValueResponseType.Aborted };
            return;
        }

        List<(string key, HLCTimestamp ticketId)> mutationsPrepared = proposalResponses.Select(r => (r.Item3, r.Item2)).ToList();

        List<Task<(KeyValueResponseType, long)>> commitTasks = new(context.LocksAcquired.Count);

        // Step 4: Commit mutations
        foreach ((string key, HLCTimestamp ticketId) in mutationsPrepared)
            commitTasks.Add(manager.LocateAndTryCommitMutations(context.TransactionId, key, ticketId, KeyValueConsistency.Linearizable, cancellationToken));

        await Task.WhenAll(commitTasks);
    }

    private async Task ExecuteTransactionInternal(KeyValueTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        while (true)
        {
            //Console.WriteLine("AST={0}", ast.nodeType);
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
                
                case NodeType.Set:
                    await ExecuteSet(context, ast, KeyValueConsistency.Linearizable, cancellationToken);
                    break;
                
                case NodeType.Delete:
                    await ExecuteDelete(context, ast, KeyValueConsistency.Linearizable, cancellationToken);
                    break;
                
                case NodeType.Extend:
                    await ExecuteExtend(context, ast, KeyValueConsistency.Linearizable, cancellationToken);
                    break;

                case NodeType.Get:
                {
                    context.Result = await ExecuteGet(context, ast, KeyValueConsistency.Linearizable, cancellationToken);
                    break;
                }
                
                case NodeType.Eset:
                    await ExecuteSet(context, ast, KeyValueConsistency.Ephemeral, cancellationToken);
                    break;

                case NodeType.Eget:
                {
                    context.Result = await ExecuteGet(context, ast, KeyValueConsistency.Ephemeral, cancellationToken);
                    break;
                }
                
                case NodeType.Edelete:
                    await ExecuteDelete(context, ast, KeyValueConsistency.Ephemeral, cancellationToken);
                    break;
                
                case NodeType.Eextend:
                    await ExecuteExtend(context, ast, KeyValueConsistency.Ephemeral, cancellationToken);
                    break;
                
                case NodeType.Commit:
                    context.Action = KeyValueTransactionAction.Commit;
                    break;
                
                case NodeType.Rollback:
                    context.Action = KeyValueTransactionAction.Abort;
                    break;
                
                case NodeType.Return:
                    if (ast.leftAst is not null)
                        context.Result = KeyValueTransactionExpression.Eval(context, ast.leftAst).ToTransactionRsult();
                    
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;
                
                case NodeType.Begin:
                    throw new Exception("Nested transactions are not supported");
                
                case NodeType.Integer:
                case NodeType.String:
                case NodeType.Float:
                case NodeType.Boolean:
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
                    context.Result = KeyValueTransactionExpression.Eval(context, ast).ToTransactionRsult();
                    break;
                    
                case NodeType.SetNotExists:
                case NodeType.SetExists:
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
            throw new Exception("Invalid IF expression");
        
        KeyValueExpressionResult expressionResult = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        
        if (expressionResult is { Type: KeyValueExpressionType.Bool, BoolValue: true })
        {
            if (ast.rightAst is not null) 
                await ExecuteTransactionInternal(context, ast.rightAst, cancellationToken);
            
            return;
        }
        
        if (ast.extendedOne is not null) 
            await ExecuteTransactionInternal(context, ast.extendedOne, cancellationToken);
    }

    /// <summary>
    /// Obtains that must be acquired to start the transaction
    /// </summary>
    /// <param name="ast"></param>
    /// <param name="locksToAcquire"></param>
    /// <exception cref="Exception"></exception>
    private static void GetLocksToAcquire(NodeAst ast, HashSet<string> locksToAcquire)
    {
        while (true)
        {
            //Console.WriteLine("AST={0}", ast.nodeType);
            
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null) 
                        GetLocksToAcquire(ast.leftAst, locksToAcquire);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.Begin:
                    if (ast.leftAst is not null) 
                        GetLocksToAcquire(ast.leftAst, locksToAcquire);
                    break;
                
                case NodeType.If:
                {
                    if (ast.rightAst is not null) 
                        GetLocksToAcquire(ast.rightAst, locksToAcquire);
                    
                    if (ast.extendedOne is not null) 
                        GetLocksToAcquire(ast.extendedOne, locksToAcquire);

                    break;
                }
                
                case NodeType.Set:
                    if (ast.leftAst is null)
                        throw new Exception("Invalid SET expression");
                    
                    locksToAcquire.Add(ast.leftAst.yytext!);
                    break;
                
                case NodeType.Get:
                    if (ast.leftAst is null)
                        throw new Exception("Invalid GET expression");
                    
                    locksToAcquire.Add(ast.leftAst.yytext!);
                    break;
                
                case NodeType.Extend:
                    if (ast.leftAst is null)
                        throw new Exception("Invalid EXTEND expression");
                    
                    locksToAcquire.Add(ast.leftAst.yytext!);
                    break;
                
                case NodeType.Delete:
                    if (ast.leftAst is null)
                        throw new Exception("Invalid DELETE expression");
                    
                    locksToAcquire.Add(ast.leftAst.yytext!);
                    break;
            }

            break;
        }
    }
}