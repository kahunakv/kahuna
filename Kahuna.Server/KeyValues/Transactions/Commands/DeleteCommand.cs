
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command to execute a delete operation within a key-value transaction context.
/// </summary>
/// <remarks>
/// The <c>DeleteCommand</c> is designed to handle the deletion of key-value pairs within
/// a transactional operation. It inherits from the <c>BaseCommand</c> class, ensuring
/// consistency with other command types in the transaction system.
/// </remarks>
internal sealed class DeleteCommand : BaseCommand
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

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await manager.LocateAndTryDeleteKeyValue(
            context.TransactionId,
            key: keyName,
            durability,
            cancellationToken
        );
        
        switch (type)
        {
            case KeyValueResponseType.Deleted:
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
}