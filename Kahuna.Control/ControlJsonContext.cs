/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Serialization;
using Kahuna.Client;

namespace Kahuna.Control;

/// <summary>
/// Source-generated metadata for the JSON the sequence commands print. Generated metadata keeps the
/// output working in a trimmed build. The web defaults give the camel-case names this output always
/// used.
/// </summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(KahunaSequence))]
[JsonSerializable(typeof(KahunaSequenceRange))]
[JsonSerializable(typeof(SequenceDeletedOutput))]
internal sealed partial class SequenceOutputJsonContext : JsonSerializerContext;

/// <summary>Source-generated metadata for the interactive console's history file.</summary>
[JsonSerializable(typeof(List<string>))]
internal sealed partial class HistoryJsonContext : JsonSerializerContext;

/// <summary>The JSON a sequence delete prints: the sequence name and whether it existed.</summary>
internal sealed record SequenceDeletedOutput(string Name, bool Deleted);
