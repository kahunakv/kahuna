
using System.Text;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class GetByPrefixCommand : BaseCommand
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
        
        /*if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            context.LocksAcquired ??= [];
            context.LocksAcquired.Add((keyName, durability));
        }*/
                       
        KeyValueGetByPrefixResult response = await manager.LocateAndGetByPrefix(
            context.TransactionId,
            keyName,            
            durability,
            cancellationToken
        );
        
        if (response.Type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry)
        {
            context.Action = KeyValueTransactionAction.Abort;
            context.Status = KeyValueExecutionStatus.Stop;
        }

        if (response.Items.Count == 0)
        {
            if (ast.rightAst is not null)
                context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new([]));
            
            return new()
            {
                ServedFrom = "",
                Type = KeyValueResponseType.DoesNotExist
            };
        }

        if (ast.rightAst is not null)
        {
            List<KeyValueExpressionResult> varValues = new(response.Items.Count);
            
            foreach ((string key, ReadOnlyKeyValueContext valueContext) item in response.Items)
                varValues.Add(new(Encoding.UTF8.GetString(item.valueContext.Value ?? []), item.valueContext.Revision, item.valueContext.Expires.L));
            
            context.SetVariable(ast.rightAst, ast.rightAst.yytext!, new(varValues));
        }

        List<KeyValueTransactionResultValue> values = new(response.Items.Count);
        
        foreach ((string key, ReadOnlyKeyValueContext valueContext) item in response.Items)
        {
            values.Add(new()
            {
                Key = item.key,
                Value = item.valueContext.Value,
                Revision = item.valueContext.Revision,
                Expires = item.valueContext.Expires
            });                        
        }
        
        return new()
        {
            ServedFrom = "",
            Type = KeyValueResponseType.Get,
            Values = values
        };
    }
}