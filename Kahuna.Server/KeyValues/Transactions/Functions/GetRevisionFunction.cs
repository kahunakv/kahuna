
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Functions;

internal static class GetRevisionFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'revision' function", ast.yyline);

        return new() { Type = KeyValueExpressionType.LongType, LongValue = arguments[0].Revision };
    }
}