
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Functions;
using Kahuna.Server.KeyValues.Transactions.Operators;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions;

internal static class KeyValueTransactionExpression
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        switch (ast.nodeType)
        {
            case NodeType.Identifier:
                return context.GetVariable(ast, ast.yytext!);
            
            case NodeType.IntegerType:
                return new() { Type = KeyValueExpressionType.LongType, LongValue = long.Parse(ast.yytext!) };
            
            case NodeType.StringType:
                return new() { Type = KeyValueExpressionType.StringType, StrValue = ast.yytext! };
            
            case NodeType.FloatType:
                return new() { Type = KeyValueExpressionType.StringType, DoubleValue = double.Parse(ast.yytext!) };
            
            case NodeType.BooleanType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ast.yytext! == "true" };
            
            case NodeType.Placeholder:
                return new() { Type = KeyValueExpressionType.StringType, StrValue = "some value" };
            
            case NodeType.Equals:
                return EqualsOperator.Eval(context, ast, "==");
            
            case NodeType.GreaterThan:
                return GreaterThanOperator.Eval(context, ast, ">");
            
            case NodeType.LessThan:
                return LessThanOperator.Eval(context, ast, "<");
            
            case NodeType.LessThanEquals:
            {
                KeyValueExpressionResult result = GreaterThanOperator.Eval(context, ast, "<=");
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = !result.BoolValue };
            }
            
            case NodeType.GreaterThanEquals:
            {
                KeyValueExpressionResult result = LessThanOperator.Eval(context, ast, ">=");
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = !result.BoolValue };
            }
            
            case NodeType.NotEquals:
            {
                KeyValueExpressionResult result = EqualsOperator.Eval(context, ast, "!=");
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = !result.BoolValue };
            }
            
            case NodeType.Add:
                return AddOperator.Eval(context, ast);
            
            case NodeType.Subtract:
                return SubOperator.Eval(context, ast);
            
            case NodeType.Mult:
                return MultOperator.Eval(context, ast);
            
            case NodeType.Div:
                return DivOperator.Eval(context, ast);
            
            case NodeType.FuncCall:
                return CallFunction.Eval(context, ast);
            
            case NodeType.And:
                return AndOperator.Eval(context, ast);
            
            case NodeType.Or:
                return OrOperator.Eval(context, ast);
            
            case NodeType.Not:
                return NotOperator.Eval(context, ast);
            
            case NodeType.StmtList:
            case NodeType.Set:
            case NodeType.Get:
            case NodeType.Eset:
            case NodeType.Eget:
            case NodeType.If:
            case NodeType.SetNotExists:
            case NodeType.SetExists:
            case NodeType.SetCmp:
            case NodeType.SetCmpRev:
            case NodeType.Begin:
            case NodeType.Rollback:
            case NodeType.Commit:
            case NodeType.Return:
            case NodeType.ArgumentList:
            case NodeType.Delete:
            case NodeType.Edelete:
            case NodeType.Exists:
            case NodeType.Eexists:
                break;

            case NodeType.Let:
            case NodeType.Extend:
            case NodeType.Eextend:
            case NodeType.BeginOptionList:
            case NodeType.BeginOption:
            case NodeType.Sleep:
            case NodeType.Throw:    
            default:
                throw new NotImplementedException();
        }

        return new() { Type = KeyValueExpressionType.NullType };
    }
}