
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

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

            return result.ToTransactionResult();
        }

        return null;
    }
}