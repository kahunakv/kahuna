
using Kommander;
using System.Text;
using Kahuna.Configuration;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

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
        NodeAst ast = ScriptParserProcessor.Parse(script);
        
        Console.WriteLine(ast.nodeType);
        Console.WriteLine(ast.leftAst?.nodeType);
        Console.WriteLine(ast.rightAst?.nodeType);

        switch (ast.nodeType)
        {
            case NodeType.Set:
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

            case NodeType.Get:
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
        }
        
        return new() { Type = KeyValueResponseType.Errored };
    }
}