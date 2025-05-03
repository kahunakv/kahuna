
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
        
        if (ast.extendedOne is not null)
            expiresMs = int.Parse(ast.extendedOne.yytext!);

        KeyValueFlags flags = KeyValueFlags.Set;

        if (ast.extendedTwo is not null)
        {
            flags = ast.extendedTwo.nodeType switch
            {
                NodeType.SetNotExists => KeyValueFlags.SetIfNotExists,
                NodeType.SetExists => KeyValueFlags.SetIfExists,
                _ => flags
            };
        }
        
        long compareRevision = 0;
        byte[]? compareValue = null;

        /*if (ast.extendedThree is not null)
        {
            if (ast.extendedThree.leftAst is null)
                throw new KahunaScriptException("Invalid SET cmp/cmprev", ast.yyline);

            switch (ast.extendedThree.nodeType)
            {
                case NodeType.SetCmp:
                    flags = KeyValueFlags.SetIfEqualToValue;
                    compareValue = KeyValueTransactionExpression.Eval(context, ast.extendedThree.leftAst).ToBytes();
                    break;
                
                case NodeType.SetCmpRev:
                    flags = KeyValueFlags.SetIfEqualToRevision;
                    compareRevision = KeyValueTransactionExpression.Eval(context, ast.extendedThree.leftAst).ToLong();
                    break;
            }
        }*/

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
    
    private static void GetSetFlags(KeyValueTransactionContext context, NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.ArgumentList:
                {
                    if (ast.leftAst is not null)
                        GetFuncCallArguments(context, ast.leftAst, arguments);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                default:
                    arguments.Add(KeyValueTransactionExpression.Eval(context, ast));
                    break;
            }

            break;
        }
    }
}