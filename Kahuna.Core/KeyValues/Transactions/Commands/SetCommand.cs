
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command that performs a set operation within a key-value transactional context.
/// This command interacts with the key-value store to set or update data based on the provided parameters.
/// </summary>
internal sealed class SetCommand : BaseCommand
{
    public static async Task<KeyValueTransactionResult> Execute(
        KeyValuesManager manager, 
        KeyValueTransactionContext context, 
        NodeAst ast, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (ast.leftAst?.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid value", ast.yyline);
        
        string keyName = GetKeyName(context, ast.leftAst);

        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            context.LocksAcquired ??= [];
            context.LocksAcquired.Add((keyName, durability));
        }

        int expiresMs = 0;
        long compareRevision = 0;
        byte[]? compareValue = null;
        KeyValueFlags flags = KeyValueFlags.Set;

        if (ast.extendedOne is not null)
        {
            List<KeyValueSetFlag> arguments = [];

            GetSetFlags(ast.extendedOne, arguments);
            
            foreach (KeyValueSetFlag flag in arguments)
            {
                switch (flag.NodeType)
                {
                    case NodeType.SetEx:
                        if (flag.ExprAst is null)
                            throw new KahunaScriptException("Invalid SET EX expression", ast.yyline); 
                        
                        KeyValueExpressionResult ex = KeyValueTransactionExpression.Eval(context, flag.ExprAst);
                        if (ex.Type != KeyValueExpressionType.LongType)
                            throw new KahunaScriptException("Invalid SET EX expression", ast.yyline);
                        
                        expiresMs = (int)ex.LongValue;
                        break;
                    
                    case NodeType.SetExists:
                        flags |= KeyValueFlags.SetIfExists;
                        break;
                    
                    case NodeType.SetNotExists:
                        flags |= KeyValueFlags.SetIfNotExists;
                        break;
                    
                    case NodeType.SetCmp:
                        if (flag.ExprAst is null)
                            throw new KahunaScriptException("Invalid SET CMP expression", ast.yyline); 
                        
                        flags |= KeyValueFlags.SetIfEqualToValue;
                        compareValue = KeyValueTransactionExpression.Eval(context, flag.ExprAst).ToBytes();
                        break;
                    
                    case NodeType.SetCmpRev:
                        if (flag.ExprAst is null)
                            throw new KahunaScriptException("Invalid SET CMPREV expression", ast.yyline); 
                        
                        flags |= KeyValueFlags.SetIfEqualToRevision;
                        compareRevision = KeyValueTransactionExpression.Eval(context, flag.ExprAst).ToLong();
                        break;
                    
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await manager.LocateAndTrySetKeyValue(
            context.TransactionId,
            key: keyName,
            value: result.ToBytes(),
            compareValue,
            compareRevision,
            flags,
            expiresMs,
            durability,
            cancellationToken
        );

        switch (type)
        {
            case KeyValueResponseType.Set:
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((keyName, durability));
                break;
            
            case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                context.Action = KeyValueTransactionAction.Abort;
                context.Status = KeyValueExecutionStatus.Stop;
                break;
        }
        
        context.ModifiedResult = new()
        {
            Type = type,
            Values = [
                new()
                {
                    Key = keyName,
                    Revision = revision,
                    LastModified = lastModified
                }
            ]
        };

        return new()
        {
            ServedFrom = "",
            Type = type,
            Values = [
                new()
                {
                    Key = keyName,
                    Revision = revision,
                    LastModified = lastModified
                }
            ]
        };
    }
    
    private static void GetSetFlags(NodeAst ast, List<KeyValueSetFlag> flags)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.SetFlagsList:
                {
                    if (ast.leftAst is not null)
                        GetSetFlags(ast.leftAst, flags);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.SetCmp:
                case NodeType.SetCmpRev:
                case NodeType.SetEx:
                case NodeType.SetNotExists:
                case NodeType.SetExists:
                case NodeType.SetNoRev:
                    flags.Add(new(ast.nodeType, ast.leftAst));
                    break;
                
                default:
                    throw new NotImplementedException();
            }

            break;
        }
    }
}