
using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Converts an array argument only when the function asks for it.
///
/// <para>An array argument can nest to any depth, and most functions never touch the arguments they
/// do not declare. Converting one eagerly would make a function that ignores it pay for every
/// element. The first <c>AsArray()</c> call converts, and the result is kept here: a
/// <see cref="KahunaValue"/> is a struct and cannot cache anything itself, but every copy of it
/// shares this object.</para>
/// </summary>
internal sealed class ScriptArraySource(List<KeyValueExpressionResult> source) : IKahunaArraySource
{
    private KahunaValue[]? materialized;

    public IReadOnlyList<KahunaValue> Materialize()
    {
        if (materialized is not null)
            return materialized;

        KahunaValue[] values = new KahunaValue[source.Count];

        for (int i = 0; i < source.Count; i++)
            values[i] = ScriptFunctionMarshal.ToKahunaValue(source[i]);

        materialized = values;

        return values;
    }
}

/// <summary>
/// Translates between the evaluator's value type and the value type a user-defined function sees.
///
/// <para>The two types stay separate on purpose. <see cref="KeyValueExpressionResult"/> carries
/// revision and expiry metadata and is free to change with the script engine;
/// <see cref="KahunaValue"/> is a published API that extensions compile against. Nothing an
/// extension sees should pin the evaluator's internals.</para>
/// </summary>
internal static class ScriptFunctionMarshal
{
    /// <summary>
    /// Converts one evaluated argument.
    ///
    /// <para>Revision and expiry are dropped: a function receives a value, not a key. A string or a
    /// byte buffer that is null arrives as an empty one of its own kind rather than as null, so a
    /// function that switches on <see cref="KahunaValue.Kind"/> sees what the script sees. The
    /// script's own null is a separate kind and is preserved as such.</para>
    /// </summary>
    public static KahunaValue ToKahunaValue(KeyValueExpressionResult result)
    {
        return result.Type switch
        {
            KeyValueExpressionType.NullType   => KahunaValue.Null,
            KeyValueExpressionType.BoolType   => KahunaValue.From(result.BoolValue),
            KeyValueExpressionType.LongType   => KahunaValue.From(result.LongValue),
            KeyValueExpressionType.DoubleType => KahunaValue.From(result.DoubleValue),
            KeyValueExpressionType.StringType => KahunaValue.From(result.StrValue ?? string.Empty),
            KeyValueExpressionType.BytesType  => KahunaValue.From(result.BytesValue ?? []),
            KeyValueExpressionType.ArrayType  => result.ArrayValue is null
                ? KahunaValue.FromArray([])
                : KahunaValue.FromArraySource(new ScriptArraySource(result.ArrayValue)),
            _ => KahunaValue.Null
        };
    }

    /// <summary>
    /// Converts a returned value back for the evaluator.
    ///
    /// <para>The result is a plain value with no revision and no expiry, exactly like the result of
    /// <c>concat</c> or <c>upper</c>. A byte buffer is copied: the array that comes back can enter a
    /// transaction's write set and travel to the persistence backend, so it must not alias memory a
    /// function reuses between calls.</para>
    /// </summary>
    public static KeyValueExpressionResult ToExpressionResult(KahunaValue value)
    {
        switch (value.Kind)
        {
            case KahunaValueKind.Null:
                return KeyValueExpressionResult.Null;

            case KahunaValueKind.Bool:
                return KeyValueExpressionResult.FromBool(value.AsBool());

            case KahunaValueKind.Long:
                return new(value.AsLong());

            case KahunaValueKind.Double:
                return new(value.AsDouble());

            case KahunaValueKind.String:
                return new(value.AsString());

            case KahunaValueKind.Bytes:
                return new(value.AsBytes().ToArray());

            case KahunaValueKind.Array:
            {
                IReadOnlyList<KahunaValue> values = value.AsArray();

                List<KeyValueExpressionResult> converted = new(values.Count);

                for (int i = 0; i < values.Count; i++)
                    converted.Add(ToExpressionResult(values[i]));

                return new(converted);
            }

            default:
                return KeyValueExpressionResult.Null;
        }
    }
}
