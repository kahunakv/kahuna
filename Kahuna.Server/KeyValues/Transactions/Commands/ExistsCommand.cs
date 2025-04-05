
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class ExistsCommand : BaseCommand
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
        
        string keyName = GetKeyName(context, ast.leftAst);
        
        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            context.LocksAcquired ??= [];
            context.LocksAcquired.Add((keyName, durability));
        }
        
        long compareRevision = -1;
        
        if (ast.extendedOne is not null)
            compareRevision = int.Parse(ast.extendedOne.yytext!);
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? readOnlyContext) = await manager.LocateAndTryExistsValue(
            context.TransactionId,
            keyName,
            compareRevision,
            durability,
            cancellationToken
        );
        
        if (type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry)
        {
            context.Action = KeyValueTransactionAction.Abort;
            context.Status = KeyValueExecutionStatus.Stop;
        }

        if (readOnlyContext is null)
        {
            if (ast.rightAst is not null)
                context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new() { Type = KeyValueExpressionType.NullType });
            
            return new()
            {
                ServedFrom = "",
                Type = type
            };
        }
        
        if (ast.rightAst is not null)
            context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new()
            {
                Type = KeyValueExpressionType.BoolType, 
                BoolValue = type == KeyValueResponseType.Exists,
                Revision = readOnlyContext.Revision,
                Expires = readOnlyContext.Expires.L
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
}