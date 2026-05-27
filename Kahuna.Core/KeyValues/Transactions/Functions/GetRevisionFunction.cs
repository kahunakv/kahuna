
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to execute the 'revision' function within the script parser.
/// </summary>
/// <remarks>
/// The 'revision' function retrieves the revision of a given key-value expression result.
/// This function expects a single argument, and if the argument count is incorrect,
/// an exception will be thrown indicating an invalid function call.
/// </remarks>
internal static class GetRevisionFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'revision' function", ast.yyline);

        return new(arguments[0].Revision, arguments[0].Revision, arguments[0].Expires);        
    }
}