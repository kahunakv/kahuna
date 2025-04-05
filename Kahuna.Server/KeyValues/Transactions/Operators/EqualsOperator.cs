
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class EqualsOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast, string operatorType)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
        
        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        switch (left.Type)
        {
            case KeyValueExpressionType.NullType when right.Type == KeyValueExpressionType.NullType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = true };
            
            case KeyValueExpressionType.NullType when right.Type != KeyValueExpressionType.NullType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = false };
            
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.BoolValue == right.BoolValue };
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.StringType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.StrValue == right.StrValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue == right.LongValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = Math.Abs(left.DoubleValue - right.DoubleValue) < 0.01 };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = Math.Abs(left.LongValue - right.DoubleValue) < 0.01 };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = Math.Abs(left.DoubleValue - right.LongValue) < 0.01 };
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.StringType:
                byte[] rightBytes = right.StrValue is not null ? Encoding.UTF8.GetBytes(right.StrValue) : [];
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(rightBytes) };
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.BytesType:
                byte[] leftBytes = left.StrValue is not null ? Encoding.UTF8.GetBytes(left.StrValue) : [];
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ((ReadOnlySpan<byte>)right.BytesValue).SequenceEqual(leftBytes) };
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.BytesType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(right.BytesValue) };

            default:
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
        }
    }
}