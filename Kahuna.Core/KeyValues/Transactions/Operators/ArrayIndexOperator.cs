using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class ArrayIndexOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        if (left.Type != KeyValueExpressionType.ArrayType || left.ArrayValue is null)
            throw new KahunaScriptException("Expression to index must be an array: Found: " + left.Type, ast.yyline);

        // A double or a numeric string is accepted as an index and converted, so a subscript computed by
        // arithmetic or read out of a stored value works without the author having to convert it first. The
        // bounds are checked on the converted value, not on the original expression: checking the original
        // would read a long out of a result that holds a double or a string, and that field is always zero
        // for those types, so every non-long subscript would be reported as out of range.
        long index = ScriptIntegerArgument.Require(ast, right, "Index");

        if (index < 0 || index >= left.ArrayValue.Count)
            throw new KahunaScriptException("Index must be positive and less than size of array. Found: " + right.Type + " Value: " + index, ast.yyline);

        return left.ArrayValue[(int)index];
    }
}
