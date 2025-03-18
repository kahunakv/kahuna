
using Kommander;
using System.Text;
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
    
    public async Task<KeyValueTransactionResult> TryExecuteTx(string script)
    {
        logger.LogDebug("Executing tx for {Script}", script);
        
        NodeAst ast = ScriptParserProcessor.Parse(script);
        
        Console.WriteLine(ast.nodeType);
        Console.WriteLine(ast.leftAst?.nodeType);
        Console.WriteLine(ast.rightAst?.nodeType);

        switch (ast.nodeType)
        {
            case NodeType.Set:
                return await ExecuteSet(HLCTimestamp.Zero, ast, KeyValueConsistency.Linearizable);

            case NodeType.Get:
                return await ExecuteGet(HLCTimestamp.Zero, ast);
            
            case NodeType.Eset:
                return await ExecuteSet(HLCTimestamp.Zero, ast, KeyValueConsistency.Ephemeral);
            
            case NodeType.Eget:
                return await ExecuteEget(HLCTimestamp.Zero, ast);
            
            case NodeType.StmtList:
                return await ExecuteTransaction(ast, true);
            
            case NodeType.Begin:
                return await ExecuteTransaction(ast.leftAst!, false);
        }
        
        return new() { Type = KeyValueResponseType.Errored };
    }

    private async Task<KeyValueTransactionResult> ExecuteSet(HLCTimestamp transactionId, NodeAst ast, KeyValueConsistency consistency)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        if (ast.rightAst is null)
            throw new Exception("Invalid value");
        
        if (ast.rightAst.yytext is null)
            throw new Exception("Invalid value");
        
        (KeyValueResponseType type, long revision) = await manager.LocateAndTrySetKeyValue(
            transactionId,
            key: ast.leftAst.yytext,
            value: Encoding.UTF8.GetBytes(ast.rightAst.yytext),
            null,
            0,
            KeyValueFlags.Set,
            0,
            consistency,
            CancellationToken.None
        );

        return new()
        {
            ServedFrom = "",
            Type = type,
            Revision = revision
        };
    }
    
    private async Task<KeyValueTransactionResult> ExecuteGet(HLCTimestamp transactionId, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid key");
        
        if (ast.leftAst.yytext is null)
            throw new Exception("Invalid key");
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? context) = await manager.LocateAndTryGetValue(
            transactionId,
            ast.leftAst.yytext,
            KeyValueConsistency.Linearizable,
            CancellationToken.None
        );

        if (context is null)
        {
            return new()
            {
                ServedFrom = "",
                Type = type
            };
        }
            
        return new()
        {
            ServedFrom = "",
            Type = type,
            Value = context.Value,
            Revision = context.Revision,
            Expires = context.Expires
        };
    }
    
    private async Task<KeyValueTransactionResult> ExecuteEget(HLCTimestamp transactionId, NodeAst ast)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueContext? context) = await manager.LocateAndTryGetValue(
            transactionId,
            ast.leftAst!.yytext!,
            KeyValueConsistency.Ephemeral,
            CancellationToken.None
        );

        if (context is null)
        {
            return new()
            {
                ServedFrom = "",
                Type = type
            };
        }
            
        return new()
        {
            ServedFrom = "",
            Type = type,
            Value = context.Value,
            Revision = context.Revision,
            Expires = context.Expires
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
        
        List<string> locksToAcquire = [];

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
            
            Console.WriteLine(context.Action);

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
                
                case NodeType.Set:
                    await ExecuteSet(context.TransactionId, ast, KeyValueConsistency.Linearizable);
                    break;

                case NodeType.Get:
                {
                    context.Result = await ExecuteGet(context.TransactionId, ast);
                    break;
                }
                
                case NodeType.Eset:
                    await ExecuteSet(context.TransactionId, ast, KeyValueConsistency.Ephemeral);
                    break;

                case NodeType.Eget:
                {
                    context.Result = await ExecuteEget(context.TransactionId, ast);
                    break;
                }
                
                case NodeType.Commit:
                    context.Action = KeyValueTransactionAction.Commit;
                    break;
                
                case NodeType.Rollback:
                    context.Action = KeyValueTransactionAction.Abort;
                    break;
                
                case NodeType.Return:
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;
            }

            break;
        }
    }

    private static void GetLocksToAcquire(NodeAst ast, List<string> locksToAcquire)
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
                
                case NodeType.Set:
                    locksToAcquire.Add(ast.leftAst!.yytext!);
                    break;
            }

            break;
        }
    }
}