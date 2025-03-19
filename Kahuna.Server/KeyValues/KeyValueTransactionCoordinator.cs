
using System.Globalization;
using Kommander;
using System.Text;
using DotNext.Text;
using Kahuna.Configuration;
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
                return await ExecuteSet(GetTempTransactionContext(), ast, KeyValueConsistency.Linearizable);

            case NodeType.Get:
                return await ExecuteGet(GetTempTransactionContext(), ast, KeyValueConsistency.Linearizable);
            
            case NodeType.Delete:
                return await ExecuteDelete(GetTempTransactionContext(), ast, KeyValueConsistency.Linearizable);
            
            case NodeType.Eset:
                return await ExecuteSet(GetTempTransactionContext(), ast, KeyValueConsistency.Ephemeral);
            
            case NodeType.Eget:
                return await ExecuteGet(GetTempTransactionContext(), ast, KeyValueConsistency.Ephemeral);
            
            case NodeType.Edelete:
                return await ExecuteDelete(GetTempTransactionContext(), ast, KeyValueConsistency.Ephemeral);
            
            case NodeType.StmtList:
                return await ExecuteTransaction(ast, true);
            
            case NodeType.Begin:
                return await ExecuteTransaction(ast.leftAst!, false);
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

    private async Task<KeyValueTransactionResult> ExecuteSet(KeyValueTransactionContext context, NodeAst ast, KeyValueConsistency consistency)
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
            CancellationToken.None
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
    
    private async Task<KeyValueTransactionResult> ExecuteDelete(KeyValueTransactionContext context, NodeAst ast, KeyValueConsistency consistency)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        (KeyValueResponseType type, long revision) = await manager.LocateAndTryDeleteKeyValue(
            context.TransactionId,
            key: ast.leftAst.yytext,
            consistency,
            CancellationToken.None
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
    
    private async Task<KeyValueTransactionResult> ExecuteGet(KeyValueTransactionContext context, NodeAst ast, KeyValueConsistency consistency)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? readOnlyContext) = await manager.LocateAndTryGetValue(
            context.TransactionId,
            ast.leftAst.yytext,
            consistency,
            CancellationToken.None
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
                acquireLocksTasks.Add(manager.LocateAndTryAcquireExclusiveLock(transactionId, key, 5000, KeyValueConsistency.Linearizable, CancellationToken.None));
            
            (KeyValueResponseType, string)[] acquireResponses = await Task.WhenAll(acquireLocksTasks);
            
            if (acquireResponses.Any(r => r.Item1 != KeyValueResponseType.Locked))
                return new() { Type = KeyValueResponseType.Aborted };
            
            context.LocksAcquired = acquireResponses.Select(r => r.Item2).ToList();
            
            // Step 2: Execute transaction
            await ExecuteTransactionInternal(context, ast);

            if (context.Action == KeyValueTransactionAction.Commit)
            {
                await TwoPhaseCommit(context);

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
                    releaseLocksTasks.Add(manager.LocateAndTryReleaseExclusiveLock(transactionId, key, KeyValueConsistency.Linearizable, CancellationToken.None));

                await Task.WhenAll(releaseLocksTasks);
            }
        }
    }

    private async Task TwoPhaseCommit(KeyValueTransactionContext context)
    {
        if (context.LocksAcquired is null)
            return;
        
        List<Task<(KeyValueResponseType, HLCTimestamp, string)>> proposalTasks = new(context.LocksAcquired.Count);

        // Step 3: Prepare mutations
        foreach (string key in context.LocksAcquired)
            proposalTasks.Add(manager.LocateAndTryPrepareMutations(context.TransactionId, key, KeyValueConsistency.Linearizable, CancellationToken.None));

        (KeyValueResponseType, HLCTimestamp, string)[] proposalResponses = await Task.WhenAll(proposalTasks);

        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            foreach ((KeyValueResponseType, HLCTimestamp, string) proposalResponse in proposalResponses)
                Console.WriteLine("{0} {1}", proposalResponse.Item3, proposalResponse.Item1);
            
            context.Result = new() { Type = KeyValueResponseType.Aborted };
            return;
        }

        List<(string key, HLCTimestamp ticketId)> mutationsPrepared = proposalResponses.Select(r => (r.Item3, r.Item2)).ToList();

        List<Task<(KeyValueResponseType, long)>> commitTasks = new(context.LocksAcquired.Count);

        // Step 4: Commit mutations
        foreach ((string key, HLCTimestamp ticketId) in mutationsPrepared)
            commitTasks.Add(manager.LocateAndTryCommitMutations(context.TransactionId, key, ticketId, KeyValueConsistency.Linearizable, CancellationToken.None));

        await Task.WhenAll(commitTasks);
    }

    private async Task ExecuteTransactionInternal(KeyValueTransactionContext context, NodeAst ast)
    {
        while (true)
        {
            //Console.WriteLine("AST={0}", ast.nodeType);
            if (context.Status == KeyValueExecutionStatus.Stop)
                break;
            
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null) 
                        await ExecuteTransactionInternal(context, ast.leftAst);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.If:
                    await ExecuteIf(context, ast);
                    break;
                
                case NodeType.Set:
                    await ExecuteSet(context, ast, KeyValueConsistency.Linearizable);
                    break;
                
                case NodeType.Delete:
                    await ExecuteDelete(context, ast, KeyValueConsistency.Linearizable);
                    break;

                case NodeType.Get:
                {
                    context.Result = await ExecuteGet(context, ast, KeyValueConsistency.Linearizable);
                    break;
                }
                
                case NodeType.Eset:
                    await ExecuteSet(context, ast, KeyValueConsistency.Ephemeral);
                    break;

                case NodeType.Eget:
                {
                    context.Result = await ExecuteGet(context, ast, KeyValueConsistency.Ephemeral);
                    break;
                }
                
                case NodeType.Edelete:
                    await ExecuteDelete(context, ast, KeyValueConsistency.Ephemeral);
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
    private async Task ExecuteIf(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid IF expression");
        
        KeyValueExpressionResult expressionResult = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        
        if (expressionResult.Type == KeyValueExpressionType.Bool && expressionResult.BoolValue)
        {
            if (ast.rightAst is not null) 
                await ExecuteTransactionInternal(context, ast.rightAst);
            
            return;
        }
        
        if (ast.extendedOne is not null) 
            await ExecuteTransactionInternal(context, ast.extendedOne);
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
                
                case NodeType.If:
                {
                    if (ast.leftAst is not null) 
                        GetLocksToAcquire(ast.leftAst, locksToAcquire);
                    
                    if (ast.rightAst is not null) 
                        GetLocksToAcquire(ast.rightAst, locksToAcquire);

                    break;
                }
                
                case NodeType.Set:
                    if (ast.leftAst is null)
                        throw new Exception("Invalid SET expression");
                    
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