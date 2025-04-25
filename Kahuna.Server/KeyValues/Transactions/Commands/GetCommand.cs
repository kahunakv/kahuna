
using System.Text;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command for retrieving a key-value pair within a transactional context.
/// </summary>
/// <remarks>
/// The GetCommand is used to execute operations that fetch data associated with a given key
/// within the key-value store, ensuring compliance with transaction durability settings and
/// leveraging the provided transaction context.
/// </remarks>
internal sealed class GetCommand : BaseCommand
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

        string keyName = GetKeyName(context, ast.leftAst);
        
        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            context.LocksAcquired ??= [];
            context.LocksAcquired.Add((keyName, durability));
        }
        
        long compareRevision = -1;
        
        if (ast.extendedOne is not null)
            compareRevision = int.Parse(ast.extendedOne.yytext!);
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? readOnlyContext) = await manager.LocateAndTryGetValue(
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

        if (type != KeyValueResponseType.Get || readOnlyContext is null)
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
                Encoding.UTF8.GetString(readOnlyContext.Value ?? []),
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