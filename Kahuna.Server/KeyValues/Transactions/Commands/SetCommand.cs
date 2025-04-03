
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

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
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.leftAst.yytext is null)
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
}