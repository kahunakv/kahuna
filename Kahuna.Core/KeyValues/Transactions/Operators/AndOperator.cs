using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Represents a static operator used to evaluate logical AND expressions within a key-value transaction context.
/// </summary>
/// <remarks>
/// This operator processes left and right abstract syntax tree (AST) nodes to determine the result
/// of a logical AND operation based on their types and values. Supported value types include Boolean,
/// Long, and Double. The result of the operation is returned as a KeyValueExpressionResult.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown if the left or right AST node is null, or if the operand types are incompatible for
/// an AND operation.
/// </exception>
internal static class AndOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return new(left.BoolValue && right.BoolValue);
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new(left.LongValue != 0 && right.LongValue != 0);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new(left.DoubleValue != 0 && right.LongValue != 0);
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new(left.LongValue != 0 && right.DoubleValue != 0);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new(left.DoubleValue != 0 && right.DoubleValue != 0);
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " and " + right.Type, ast.yyline);
        }
    }
}