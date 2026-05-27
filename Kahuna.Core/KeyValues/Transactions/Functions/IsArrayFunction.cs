
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Represents a function that checks whether a given argument is of the array type.
/// </summary>
/// <remarks>
/// The function evaluates the type of the provided argument and determines if it
/// is an array. It throws an exception if the number of arguments provided is invalid.
/// </remarks>
internal static class IsArrayFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'is_array' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return new(argument.Type == KeyValueExpressionType.ArrayType);
    }
}