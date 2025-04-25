
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Represents a static utility class that implements the 'is_bool' function
/// in the scripting engine. This function is used to evaluate whether
/// a given argument is of a boolean type.
/// </summary>
internal static class IsBoolFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'is_bool' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return new(argument.Type == KeyValueExpressionType.BoolType);
    }
}