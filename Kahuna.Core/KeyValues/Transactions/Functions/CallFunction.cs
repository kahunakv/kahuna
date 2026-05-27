
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Represents a static class that provides functionality to evaluate function calls
/// within a key-value transaction context using abstract syntax trees (ASTs).
/// </summary>
internal static class CallFunction
{        
    private static readonly Dictionary<string, Func<NodeAst, List<KeyValueExpressionResult>, KeyValueExpressionResult>> FunctionMap = new()
    {
        { "abs", AbsFunction.Execute },
        { "pow", PowFunction.Execute },
        { "round", RoundFunction.Execute },
        { "ceil", CeilFunction.Execute },
        { "floor", FloorFunction.Execute },
        { "min", MinFunction.Execute },
        { "max", MaxFunction.Execute },
        { "to_int", CastToLongFunction.Execute },
        { "to_integer", CastToLongFunction.Execute },
        { "to_long", CastToLongFunction.Execute },
        { "to_number", CastToLongFunction.Execute },
        { "is_int", IsLongFunction.Execute },
        { "is_integer", IsLongFunction.Execute },
        { "is_long", IsLongFunction.Execute },
        { "to_float", CastToDoubleFunction.Execute },
        { "to_double", CastToDoubleFunction.Execute },
        { "is_float", IsDoubleFunction.Execute },
        { "is_double", IsDoubleFunction.Execute },
        { "to_str", CastToStrFunction.Execute },
        { "to_string", CastToStrFunction.Execute },        
        { "is_str", IsStringFunction.Execute },
        { "is_string", IsStringFunction.Execute },
        { "to_bool", CastToBoolFunction.Execute },
        { "to_boolean", CastToBoolFunction.Execute },
        { "is_bool", IsBoolFunction.Execute },
        { "is_boolean", IsBoolFunction.Execute },
        { "is_null", IsNullFunction.Execute },
        { "is_array", IsArrayFunction.Execute },
        { "to_json", ToJsonFunction.Execute },
        { "concat", ConcatFunction.Execute },
        { "upper", UpperFunction.Execute },
        { "lower", LowerFunction.Execute },
        { "count", CountFunction.Execute },        
        { "revision", GetRevisionFunction.Execute },
        { "rev", GetRevisionFunction.Execute },
        { "expires", GetExpiresFunction.Execute },
        { "len", GetLengthFunction.Execute },
        { "length", GetLengthFunction.Execute },
        { "current_time", CurrentTimeFunction.Execute }
    };

    /// <summary>
    /// Evaluates a function call represented by an abstract syntax tree (AST) node within a given transactional context.
    /// </summary>
    /// <param name="context">The transaction context in which the function evaluation is executed.</param>
    /// <param name="ast">The representation of the abstract syntax tree for the function being evaluated.</param>
    /// <returns>The result of evaluating the function as a <see cref="KeyValueExpressionResult"/>.</returns>
    /// <exception cref="KahunaScriptException">
    /// Thrown when the AST node for the function is invalid, the function name is missing,
    /// or the function is not defined.
    /// </exception>
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid function expression", ast.yyline);
        
        if (string.IsNullOrEmpty(ast.leftAst.yytext))
            throw new KahunaScriptException("Invalid function name", ast.yyline);
        
        List<KeyValueExpressionResult> arguments = [];
                
        if (ast.rightAst is not null)
            GetFuncCallArguments(context, ast.rightAst, arguments);
        
        if (FunctionMap.TryGetValue(ast.leftAst.yytext, out Func<NodeAst, List<KeyValueExpressionResult>, KeyValueExpressionResult>? function))        
            return function(ast, arguments);

        throw new KahunaScriptException($"Undefined function {ast.leftAst.yytext} expression", ast.yyline);
    }

    /// <summary>
    /// Recursively extracts and evaluates function call arguments from the provided abstract syntax tree (AST) node
    /// and appends the results to the specified list of arguments within a given transactional context.
    /// </summary>
    /// <param name="context">The transactional context in which the evaluation occurs.</param>
    /// <param name="ast">The abstract syntax tree (AST) node representing the function call arguments.</param>
    /// <param name="arguments">The list to which the evaluated function call arguments are appended.</param>
    private static void GetFuncCallArguments(KeyValueTransactionContext context, NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.ArgumentList:
                {
                    if (ast.leftAst is not null)
                        GetFuncCallArguments(context, ast.leftAst, arguments);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                default:
                    arguments.Add(KeyValueTransactionExpression.Eval(context, ast));
                    break;
            }

            break;
        }
    }
}