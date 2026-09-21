
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
        ScriptTransactionContext context,
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

        // Inside an actor turn the request is served by the actor that is already running the script.
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = context.ActorTurn is { } turn
            ? await turn.TryDelete(keyName, durability)
            : await manager.LocateAndTryDeleteKeyValue(
                context.TransactionId,
                key: keyName,
                durability,
                cancellationToken
            );
        
        switch (type)
        {
            case KeyValueResponseType.Deleted:
                context.RecordModifiedKey((keyName, durability));
                context.StageMutation(keyName, null, KeyValueState.Deleted, revision, 0, noRevision: false); // deletes have no TTL and retain history
                break;
            
            case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                context.StopOnStatementFailure("DELETE", keyName, durability, type);
                break;
        }
        
        // One list, referenced by both results. The two carried identical values, so building the list
        // twice produced a second copy of the same three objects per statement. Nothing downstream
        // mutates a result's values, and the caller's result is discarded unless this is the script's
        // last statement, so the two can never diverge.
        List<KeyValueTransactionResultValue> values =
        [
            new()
            {
                Key = keyName,
                Revision = revision,
                LastModified = lastModified
            }
        ];

        context.ModifiedResult = new()
        {
            Type = type,
            Values = values
        };

        return new()
        {
            ServedFrom = "",
            Type = type,
            Values = values
        };
    }
}