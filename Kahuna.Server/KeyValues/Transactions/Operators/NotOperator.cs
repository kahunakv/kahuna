using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class NotOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.BoolType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = !left.BoolValue };
            
            case KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue != 0 };
            
            case KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue != 0 };

            case KeyValueExpressionType.NullType:
            case KeyValueExpressionType.StringType:
            case KeyValueExpressionType.BytesType:
            default:
                throw new KahunaScriptException("Invalid operands: not(" + left.Type + ")", ast.yyline);
        }
    }
}