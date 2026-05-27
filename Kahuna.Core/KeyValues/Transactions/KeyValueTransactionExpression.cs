
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Functions;
using Kahuna.Server.KeyValues.Transactions.Operators;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Contains static methods to evaluate key-value transaction expressions
/// within a specified context and abstract syntax tree (AST).
/// Provides functionality to traverse and compute results for transaction AST nodes.
/// </summary>
internal static class KeyValueTransactionExpression
{
    /// <summary>
    /// Evaluates the specified abstract syntax tree (AST) within the context of a key-value transaction,
    /// producing a result based on the AST evaluation logic.
    /// </summary>
    /// <param name="context">The context of the key-value transaction which contains necessary state and configuration.</param>
    /// <param name="ast">The abstract syntax tree to be evaluated, representing the input expression or operation.</param>
    /// <returns>A result encapsulated in a <see cref="KeyValueExpressionResult"/> object, containing the evaluation outcome.</returns>
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        switch (ast.nodeType)
        {
            case NodeType.Identifier:
                return context.GetVariable(ast, ast.yytext!);
            
            case NodeType.IntegerType:
                return new(long.Parse(ast.yytext!));
            
            case NodeType.StringType:
                return new(ast.yytext!);
            
            case NodeType.FloatType:               
                return new(double.Parse(ast.yytext!));
            
            case NodeType.BooleanType:
                return new(ast.yytext! == "true");
            
            case NodeType.Placeholder:
                return new(context.GetParameter(ast));
            
            case NodeType.NullType:
                return new(KeyValueExpressionType.NullType);
            
            case NodeType.Equals:
                return EqualsOperator.Eval(context, ast, "==");
            
            case NodeType.GreaterThan:
                return GreaterThanOperator.Eval(context, ast, ">");
            
            case NodeType.LessThan:
                return LessThanOperator.Eval(context, ast, "<");
            
            case NodeType.LessThanEquals:
            {
                KeyValueExpressionResult result = GreaterThanOperator.Eval(context, ast, "<=");
                return new(!result.BoolValue);
            }
            
            case NodeType.GreaterThanEquals:
            {
                KeyValueExpressionResult result = LessThanOperator.Eval(context, ast, ">=");
                return new(!result.BoolValue);
            }
            
            case NodeType.NotEquals:
            {
                KeyValueExpressionResult result = EqualsOperator.Eval(context, ast, "!=");
                return new(!result.BoolValue);
            }
            
            case NodeType.Add:
                return AddOperator.Eval(context, ast);
            
            case NodeType.Subtract:
                return SubOperator.Eval(context, ast);
            
            case NodeType.Mult:
                return MultOperator.Eval(context, ast);
            
            case NodeType.Div:
                return DivOperator.Eval(context, ast);
            
            case NodeType.ArrayIndex:
                return ArrayIndexOperator.Eval(context, ast);
            
            case NodeType.Range:
                return RangeOperator.Eval(context, ast);
            
            case NodeType.FuncCall:
                return CallFunction.Eval(context, ast);
            
            case NodeType.And:
                return AndOperator.Eval(context, ast);
            
            case NodeType.Or:
                return OrOperator.Eval(context, ast);
            
            case NodeType.Not:
                return NotOperator.Eval(context, ast);
            
            case NodeType.NotSet:
                return NotSetOperator.Eval(context, ast);
            
            case NodeType.NotFound:
                return NotFoundOperator.Eval(context, ast);
            
            case NodeType.StmtList:
            case NodeType.Set:
            case NodeType.Get:
            case NodeType.Eset:
            case NodeType.Eget:
            case NodeType.If:
            case NodeType.For:
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
            case NodeType.GetByBucket:
            case NodeType.EGetByBucket:
            case NodeType.ScanByPrefix:
            case NodeType.EscanByPrefix:
            default:
                throw new NotImplementedException();
        }

        return new(KeyValueExpressionType.NullType);
    }
}