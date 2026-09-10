using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Represents a static operator used to evaluate logical AND expressions within a key-value transaction context.
/// </summary>
/// <remarks>
/// Both operands must be boolean. The operator short-circuits: a false left operand returns false and the
/// right operand is never evaluated, so the ordinary guard "IF d != 0 &amp;&amp; n / d &gt; 1" does not divide by
/// zero. Expression evaluation has no side effects a script can depend on, so skipping the right operand
/// changes nothing but the error and the work avoided.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown if the left or right AST node is null, or if an evaluated operand is not a boolean.
/// </exception>
internal static class AndOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        if (!BooleanOperand.Require(context, ast.leftAst, ast, "&&"))
            return new(false);

        return new(BooleanOperand.Require(context, ast.rightAst, ast, "&&"));
    }
}
