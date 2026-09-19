
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kahuna.Server.Sequencer.Data;

/// <summary>
/// Source-generated metadata for reading sequence records stored in the original JSON format.
/// The web defaults (camel-case names, case-insensitive matching, numbers accepted as strings) are
/// the options those records were always read with.
/// </summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(SequenceStateCodec.JsonSequenceState))]
internal sealed partial class SequenceStateJsonContext : JsonSerializerContext;
