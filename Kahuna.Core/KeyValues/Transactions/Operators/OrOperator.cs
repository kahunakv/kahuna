using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Represents a static operator used to evaluate logical OR expressions within a key-value transaction context.
/// </summary>
/// <remarks>
/// Both operands must be boolean. The operator short-circuits: a true left operand returns true and the
/// right operand is never evaluated.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown if the left or right AST node is null, or if an evaluated operand is not a boolean.
/// </exception>
internal static class OrOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        if (BooleanOperand.Require(context, ast.leftAst, ast, "||"))
            return new(true);

        return new(BooleanOperand.Require(context, ast.rightAst, ast, "||"));
    }
}
