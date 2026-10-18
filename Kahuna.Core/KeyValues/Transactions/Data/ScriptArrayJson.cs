using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Writes a script array as JSON: numbers, strings, booleans and null as their JSON values, and a nested
/// array as a nested JSON array.
///
/// <para>The walk keeps its own stack instead of recursing. A script can nest an array inside itself once
/// per loop iteration, so the nesting depth is chosen by the script, and a stack overflow cannot be caught.
/// The size of the output is bounded where arrays are built, not here: see
/// <see cref="KeyValueExpressionResult.TotalValueCount"/>.</para>
///
/// <para>Nothing in here throws for a value a script can hold, because the result of a script is encoded on
/// demand, which can be after its commit. A byte value is written as a string, decoded as UTF-8 the same
/// way a bucket read decodes the values it returns. JSON has no representation for a non-finite double, so
/// one is written as the string "NaN", "Infinity" or "-Infinity". A lone surrogate in a string is written
/// as U+FFFD.</para>
/// </summary>
internal static class ScriptArrayJson
{
    private static readonly JsonWriterOptions WriterOptions = new()
    {
        // The writer's own limit throws past a thousand levels. Depth is not what this encoder guards
        // against: the explicit stack makes it safe at any depth.
        MaxDepth = int.MaxValue
    };

    public static byte[] ToUtf8(List<KeyValueExpressionResult>? array)
    {
        return Encode(array).WrittenSpan.ToArray();
    }

    public static string ToJsonString(List<KeyValueExpressionResult>? array)
    {
        return Encoding.UTF8.GetString(Encode(array).WrittenSpan);
    }

    private static ArrayBufferWriter<byte> Encode(List<KeyValueExpressionResult>? array)
    {
        ArrayBufferWriter<byte> buffer = new();

        using (Utf8JsonWriter writer = new(buffer, WriterOptions))
            Write(writer, array);

        return buffer;
    }

    private static void Write(Utf8JsonWriter writer, List<KeyValueExpressionResult>? root)
    {
        writer.WriteStartArray();

        if (root is null)
        {
            writer.WriteEndArray();
            return;
        }

        // Each frame is an array that is open in the output and the position of the next element to write.
        Stack<(List<KeyValueExpressionResult> Items, int Next)> pending = new();

        pending.Push((root, 0));

        while (pending.TryPop(out (List<KeyValueExpressionResult> Items, int Next) frame))
        {
            if (frame.Next == frame.Items.Count)
            {
                writer.WriteEndArray();
                continue;
            }

            pending.Push((frame.Items, frame.Next + 1));

            KeyValueExpressionResult item = frame.Items[frame.Next];

            if (item.Type == KeyValueExpressionType.ArrayType)
            {
                writer.WriteStartArray();

                if (item.ArrayValue is null)
                    writer.WriteEndArray();
                else
                    pending.Push((item.ArrayValue, 0));

                continue;
            }

            WriteScalar(writer, item);
        }
    }

    private static void WriteScalar(Utf8JsonWriter writer, KeyValueExpressionResult item)
    {
        switch (item.Type)
        {
            case KeyValueExpressionType.LongType:
                writer.WriteNumberValue(item.LongValue);
                break;

            case KeyValueExpressionType.DoubleType:
                if (double.IsFinite(item.DoubleValue))
                    writer.WriteNumberValue(item.DoubleValue);
                else
                    writer.WriteStringValue(double.IsNaN(item.DoubleValue) ? "NaN" : item.DoubleValue > 0 ? "Infinity" : "-Infinity");
                break;

            case KeyValueExpressionType.StringType:
                if (item.StrValue is null)
                    writer.WriteNullValue();
                else if (item.StrValue.AsSpan().IndexOfAnyInRange('\uD800', '\uDFFF') < 0)
                    writer.WriteStringValue(item.StrValue);
                else
                    // A surrogate can be half of a pair split by a string function, and the writer throws on
                    // a lone one. The UTF-8 encoder replaces a lone surrogate with U+FFFD, and a whole pair
                    // passes through unchanged.
                    writer.WriteStringValue(Encoding.UTF8.GetBytes(item.StrValue));
                break;

            case KeyValueExpressionType.BoolType:
                writer.WriteBooleanValue(item.BoolValue);
                break;

            case KeyValueExpressionType.BytesType:
                if (item.BytesValue is null)
                    writer.WriteNullValue();
                else
                    writer.WriteStringValue(Encoding.UTF8.GetString(item.BytesValue));
                break;

            default:
                writer.WriteNullValue();
                break;
        }
    }
}
