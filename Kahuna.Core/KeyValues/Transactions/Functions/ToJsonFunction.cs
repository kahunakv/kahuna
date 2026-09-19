using System.Text.Json;
using System.Text.Json.Serialization;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Represents a function that serializes a given input to its JSON representation.
/// </summary>
internal static class ToJsonFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_json' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.LongType => new(JsonSerializer.Serialize(arg.LongValue, ToJsonContext.Default.Int64)),
            KeyValueExpressionType.DoubleType => new(JsonSerializer.Serialize(arg.DoubleValue, ToJsonContext.Default.Double)),
            KeyValueExpressionType.StringType => new(JsonSerializer.Serialize(arg.StrValue, ToJsonContext.Default.String)),
            KeyValueExpressionType.ArrayType => new(JsonSerializer.Serialize(arg.ArrayValue, ToJsonContext.Default.ListKeyValueExpressionResult)),
            _ => throw new KahunaScriptException($"Cannot use 'to_json' function on argument {arg.Type}", ast.yyline)
        };
    }
}

/// <summary>
/// Source-generated metadata for the values <c>to_json</c> accepts, so the function keeps working in
/// a trimmed build. Metadata-only generation keeps the output identical to reflection-based
/// serialization: the generated fast-path writer writes a null byte array as an empty string.
/// </summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(List<KeyValueExpressionResult>))]
internal sealed partial class ToJsonContext : JsonSerializerContext;
