
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command that halts the execution of a key-value transaction
/// and optionally evaluates and returns the result of an associated expression.
/// </summary>
internal sealed class ReturnCommand : BaseCommand
{
    public static void Execute(
        ScriptTransactionContext context,
        NodeAst ast
    )
    {
        context.Status = KeyValueExecutionStatus.Stop;
        
        if (ast.leftAst is null)
            return;

        // Recorded, not built. A script that returns without committing never has this answered at all —
        // the uncommitted outcome is decided by the statement that stopped the script.
        context.DeferResult(KeyValueTransactionExpression.Eval(context, ast.leftAst));
    }
}