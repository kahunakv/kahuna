
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to retrieve the expiration information of a key-value entity.
/// </summary>
/// <remarks>
/// - The class is used as part of the key-value transaction system.
/// - The function extracts the `Expires` property from the argument provided.
/// - Throws an exception if an invalid number of arguments is supplied.
/// </remarks>
internal static class GetExpiresFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'expires' function", ast.yyline);

        return new(arguments[0].Expires);
    }
}