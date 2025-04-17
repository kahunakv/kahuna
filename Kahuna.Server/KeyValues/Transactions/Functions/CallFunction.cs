
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

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