
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
        long index = CastToLong(right, ast);

        if (index < 0 || index >= left.ArrayValue.Count)
            throw new KahunaScriptException("Index must be positive and less than size of array. Found: " + right.Type + " Value: " + index, ast.yyline);

        return left.ArrayValue[(int)index];
    }

    private static long CastToLong(KeyValueExpressionResult argument, NodeAst ast)
    {
        switch (argument.Type)
        {
            case KeyValueExpressionType.LongType:
                return argument.LongValue;

            case KeyValueExpressionType.DoubleType:
                // A fractional subscript is refused rather than truncated. Silently reading element 1 for
                // arr[1.9] hides an arithmetic mistake the author would otherwise see immediately.
                double value = argument.DoubleValue;

                if (double.IsNaN(value) || double.IsInfinity(value) || Math.Floor(value) != value)
                    throw new KahunaScriptException("Index must be a whole number. Found: " + value, ast.yyline);

                if (value < long.MinValue || value > long.MaxValue)
                    throw new KahunaScriptException("Index is out of range: " + value, ast.yyline);

                return (long)value;

            case KeyValueExpressionType.StringType:
                return TryCastString(ast, argument);

            default:
                throw new KahunaScriptException("Index must be an integer. Found: " + argument.Type, ast.yyline);
        }
    }

    private static long TryCastString(NodeAst ast, KeyValueExpressionResult argument)
    {
        // An empty string is refused rather than read as zero. It is far more likely to be an unset variable
        // than a deliberate request for the first element.
        if (long.TryParse(argument.StrValue, out long converted))
            return converted;

        throw new KahunaScriptException("Index must be an integer: " + argument.Type + " '" + argument.StrValue + "'", ast.yyline);
    }
}
