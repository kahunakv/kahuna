
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command to check for the existence of a key in the key-value store within a transactional context.
/// </summary>
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
        
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? readOnlyContext) = await manager.LocateAndTryExistsValue(
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
                context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new(KeyValueExpressionType.NullType));
            
            return new()
            {
                ServedFrom = "",
                Type = type,
                Values = [
                    new()
                    {
                        Key = keyName,
                        Revision = -1,
                        Expires = HLCTimestamp.Zero
                    }
                ]
            };
        }
        
        if (ast.rightAst is not null)
            context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new(
                type == KeyValueResponseType.Exists, 
                readOnlyContext.Revision, 
                readOnlyContext.Expires.L
            ));
            
        return new()
        {
            ServedFrom = "",
            Type = type,
            Values = [
                new()
                {
                    Key = keyName,
                    Value = readOnlyContext.Value,
                    Revision = readOnlyContext.Revision,
                    Expires = readOnlyContext.Expires,
                    LastModified = readOnlyContext.LastModified
                }
            ]                        
        };
    }
}