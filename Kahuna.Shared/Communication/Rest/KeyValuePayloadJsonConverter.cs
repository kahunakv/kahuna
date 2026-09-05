
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Carries a key-value payload over JSON. A null array becomes JSON null. Any array, the empty one
/// included, becomes a base64 string. This keeps apart a key that holds no value and a key that
/// holds zero bytes, which is the same distinction the gRPC transport carries in the presence flag
/// of an optional field.
///
/// <para>The distinction needs a converter because the System.Text.Json source generator writes a
/// <c>byte[]</c> property through <c>Utf8JsonWriter.WriteBase64String</c>. That method takes a span,
/// and a null array converts to an empty span, so the generated fast path emits an empty string for
/// a payload that is absent. The receiver then stores zero bytes under a key the caller set to no
/// value. A converter on the property replaces the fast path for the type that declares it, and the
/// null array reaches the wire as null.</para>
///
/// <para>Attach this converter to payload properties only. A lock owner is not a payload: the lock
/// wire has no presence flag for it, so a null owner is coerced to an empty owner on both
/// transports, and writing null there would fail server validation instead.</para>
/// </summary>
public sealed class KeyValuePayloadJsonConverter : JsonConverter<byte[]?>
{
    /// <summary>
    /// Reads null as an absent payload. Every other token goes through the same base64 decode the
    /// default converter uses, so a malformed body fails the way it always did.
    /// </summary>
    public override byte[]? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        return reader.GetBytesFromBase64();
    }

    public override void Write(Utf8JsonWriter writer, byte[]? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteBase64StringValue(value);
    }
}
