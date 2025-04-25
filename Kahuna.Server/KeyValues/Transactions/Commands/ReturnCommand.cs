
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command that halts the execution of a key-value transaction
/// and optionally evaluates and returns the result of an associated expression.
/// </summary>
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