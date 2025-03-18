
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
                return await ExecuteSet(HLCTimestamp.Zero, ast);

            case NodeType.Get:
                return await ExecuteSimpleGet(ast);
            
            case NodeType.StmtList:
                return await ExecuteTransaction(ast);
        }
        
        return new() { Type = KeyValueResponseType.Errored };
    }

    private async Task<KeyValueTransactionResult> ExecuteSet(HLCTimestamp transactionId, NodeAst ast)
    {
        (KeyValueResponseType type, long revision) = await manager.LocateAndTrySetKeyValue(
            transactionId,
            key: ast.leftAst!.yytext!,
            value: Encoding.UTF8.GetBytes(ast.rightAst!.yytext!),
            null,
            0,
            KeyValueFlags.Set,
            0,
            KeyValueConsistency.Linearizable,
            CancellationToken.None
        );

        return new()
        {
            ServedFrom = "",
            Type = type,
            Revision = revision
        };
    }
    
    private async Task<KeyValueTransactionResult> ExecuteSimpleGet(NodeAst ast)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueContext? context) = await manager.LocateAndTryGetValue(
            ast.leftAst!.yytext!,
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
    
    private async Task<KeyValueTransactionResult> ExecuteTransaction(NodeAst ast)
    {
        HLCTimestamp transactionId = await raft.HybridLogicalClock.SendOrLocalEvent();
        
        List<string> locksAcquired = [];
        List<string> locksToAcquire = [];
        List<(string, HLCTimestamp)> prparedMutations = [];

        GetLocksToAcquire(ast, locksToAcquire);

        try
        {
            // Step 1: Acquire locks
            foreach (string key in locksToAcquire)
            {
                KeyValueResponseType response = await manager.LocateAndTryAcquireExclusiveLock(transactionId, key, 5000, KeyValueConsistency.Linearizable, CancellationToken.None);

                if (response == KeyValueResponseType.Locked)
                {
                    locksAcquired.Add(key);
                    continue;
                }
                
                Console.WriteLine(response);

                return new()
                {
                    Type = KeyValueResponseType.Aborted
                };
            }
            
            // Step 2: Execute transaction
            await ExecuteTransactionInternal(transactionId, ast);
            
            // Step 3: Prepare mutations
            foreach (string key in locksAcquired)
            {
                (KeyValueResponseType response, HLCTimestamp proposalTicket) = await manager.LocateAndTryPrepareMutations(transactionId, key, KeyValueConsistency.Linearizable, CancellationToken.None);

                if (response == KeyValueResponseType.Prepared)
                {
                    prparedMutations.Add((key, proposalTicket));
                    continue;
                }
                
                // @rollback
                
                Console.WriteLine(response);

                return new()
                {
                    Type = KeyValueResponseType.Aborted
                };
            }
            
            // Step 4: Commit mutations
            foreach ((string key, HLCTimestamp ticketId) in prparedMutations)
            {
                (KeyValueResponseType response, _) = await manager.LocateAndTryCommitMutations(transactionId, key, ticketId, KeyValueConsistency.Linearizable, CancellationToken.None);
                
                if (response == KeyValueResponseType.Committed)
                    continue;
                
                Console.WriteLine(response);

                return new()
                {
                    Type = KeyValueResponseType.Aborted
                };
            }
        }
        finally
        {
            // Final Step: Release locks
            foreach (string key in locksAcquired)
            {
                KeyValueResponseType response = await manager.LocateAndTryReleaseExclusiveLock(transactionId, key, KeyValueConsistency.Linearizable, CancellationToken.None);
                
                if (response != KeyValueResponseType.Unlocked)
                    Console.WriteLine(response);
            }
        }

        return new KeyValueTransactionResult();
    }

    private async Task ExecuteTransactionInternal(HLCTimestamp transactionId, NodeAst ast)
    {
        while (true)
        {
            //Console.WriteLine("AST={0}", ast.nodeType);
            
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null) 
                        await ExecuteTransactionInternal(transactionId, ast.leftAst);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.Set:
                    await ExecuteSet(transactionId, ast);
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