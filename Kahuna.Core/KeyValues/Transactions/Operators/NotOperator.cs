using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Represents a static operator used to evaluate logical NOT expressions within a key-value transaction context.
/// </summary>
/// <remarks>
/// The operand must be a boolean, which is the same rule the other logical operators and IF follow.
/// </remarks>
internal static class NotOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        return new(!BooleanOperand.Require(context, ast.leftAst, ast, "!"));
    }
}
