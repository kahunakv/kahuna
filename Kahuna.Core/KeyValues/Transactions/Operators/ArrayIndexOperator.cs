
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class ArrayIndexOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        if (left.Type != KeyValueExpressionType.ArrayType || left.ArrayValue is null)
            throw new KahunaScriptException("Expression to index must be an array: Found: " + left.Type, ast.yyline);
        
        KeyValueExpressionResult index = CastToLong(right, ast);
        
        if (right.Type != KeyValueExpressionType.LongType || right.LongValue < 0 || right.LongValue >= left.ArrayValue.Count)
            throw new KahunaScriptException("Index must be integer and positive and less than size of array. Found: " + right.Type + " Value: " + right.LongValue, ast.yyline);

        return left.ArrayValue[(int)index.LongValue];
    }

    private static KeyValueExpressionResult CastToLong(KeyValueExpressionResult argument, NodeAst ast)
    {
        return argument.Type switch
        {
            KeyValueExpressionType.LongType => argument,
            KeyValueExpressionType.DoubleType => new((long)argument.DoubleValue),
            KeyValueExpressionType.StringType => new(TryCastString(ast, argument)),
            _ => throw new KahunaScriptException("Index must be integer and positive. Found: " + argument.Type, ast.yyline)
        };
    }

    private static long TryCastString(NodeAst ast, KeyValueExpressionResult argument)
    {
        if (string.IsNullOrEmpty(argument.StrValue))
            return 0;
        
        if (long.TryParse(argument.StrValue, out long converted))
            return converted;

        throw new KahunaScriptException("Index must be integer and positive: " + argument.Type + " " + argument.StrValue, ast.yyline);
    }
}