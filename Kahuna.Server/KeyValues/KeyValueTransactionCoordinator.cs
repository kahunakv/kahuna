
using Kommander;
using System.Text;
using Kahuna.Configuration;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

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
                return await ExecuteSimpleSet(ast);

            case NodeType.Get:
                return await ExecuteSimpleGet(ast);
            
            case NodeType.StmtList:
                return await ExecuteTransaction(ast);
        }
        
        return new() { Type = KeyValueResponseType.Errored };
    }

    private async Task<KeyValueTransactionResult> ExecuteSimpleSet(NodeAst ast)
    {
        (KeyValueResponseType type, long revision) = await manager.LocateAndTrySetKeyValue(
            ast.leftAst!.yytext!,
            Encoding.UTF8.GetBytes(ast.rightAst!.yytext!),
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

        GetLocksToAcquire(ast, locksToAcquire);

        try
        {
            foreach (string key in locksToAcquire)
            {
                KeyValueResponseType response = await manager.LocateAndTryAcquireExclusiveLock(transactionId, key, 5000, KeyValueConsistency.Linearizable, CancellationToken.None);

                if (response == KeyValueResponseType.Locked)
                {
                    locksAcquired.Add(key);
                    continue;
                }

                return new()
                {
                    Type = KeyValueResponseType.Aborted
                };
            }
        }
        finally
        {
            foreach (string key in locksAcquired)
            {
                KeyValueResponseType response = await manager.LocateAndTryReleaseExclusiveLock(transactionId, key, KeyValueConsistency.Linearizable, CancellationToken.None);
                
                Console.WriteLine(response);
            }
        }

        return new KeyValueTransactionResult();
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