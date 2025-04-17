
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class ReturnCommand : BaseCommand
{
    public static KeyValueTransactionResult? Execute(
        KeyValueTransactionContext context,
        NodeAst ast
    )
    {
        context.Status = KeyValueExecutionStatus.Stop;
        
        if (ast.leftAst is not null)
        {
            KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.leftAst);
            
            return new()
            {
                ServedFrom = "",
                Type = KeyValueResponseType.Get,
                Values = [
                    new()
                    {
                        Key = ast.leftAst.yytext!,
                        Revision = result.Revision,
                        Expires = new(result.Expires, 0),
                        LastModified = HLCTimestamp.Zero
                    }
                ]
            };
        }

        return null;
    }
}