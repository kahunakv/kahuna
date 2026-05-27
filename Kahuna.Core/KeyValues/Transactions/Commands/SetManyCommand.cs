
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class SetManyCommand : BaseCommand
{
    public static async Task<KeyValueTransactionResult> Execute(
        KeyValuesManager manager,
        KeyValueTransactionContext context,
        NodeAst ast,        
        CancellationToken cancellationToken
    )
    {
        List<KahunaSetKeyValueRequestItem> arguments = [];

        GetSetCalls(context, ast, arguments);

        if (arguments.Count == 0)
        {
            return new()
            {
                Type = KeyValueResponseType.Set
            };
        }
        
        List<KahunaSetKeyValueResponseItem> responses = await manager.LocateAndTrySetManyKeyValue(arguments, cancellationToken);

        foreach (KahunaSetKeyValueResponseItem response in responses)
        {
            switch (response.Type)
            {                                
                case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                    context.Action = KeyValueTransactionAction.Abort;
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;
            }
            
            context.ModifiedResult = new()
            {
                Type = response.Type,
                Values = [
                    new()
                    {
                        Key = response.Key ?? "",
                        Revision = response.Revision,
                        LastModified = response.LastModified
                    }
                ]
            };
            
            context.ModifiedKeys ??= [];
            context.ModifiedKeys.Add((response.Key ?? "", response.Durability));
        }
        
        if (context.ModifiedResult is null)
            return new()
            {
                Type = KeyValueResponseType.Set
            };

        return context.ModifiedResult;
    }
    
    private static void GetSetCalls(KeyValueTransactionContext context, NodeAst ast, List<KahunaSetKeyValueRequestItem> arguments)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null)
                        GetSetCalls(context, ast.leftAst, arguments);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.Set:
                    arguments.Add(GetSetCall(context, ast, KeyValueDurability.Persistent));
                    break;
                
                case NodeType.Eset:
                    arguments.Add(GetSetCall(context, ast, KeyValueDurability.Ephemeral));
                    break;
                
                default:
                    throw new KahunaScriptException($"Invalid SET command {ast.nodeType}", ast.yyline);
            }

            break;
        }
    }

    private static KahunaSetKeyValueRequestItem GetSetCall(KeyValueTransactionContext context, NodeAst ast, KeyValueDurability durability)
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
        
        return new()
        {
            TransactionId = context.TransactionId,
            Key = keyName,
            Value = result.ToBytes(),
            CompareValue = compareValue,
            CompareRevision = compareRevision,
            Flags = flags,
            ExpiresMs = expiresMs,
            Durability = durability            
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