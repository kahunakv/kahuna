
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The answer to one probe of a batched commit-time conflict probe: what a single probe of the key answers.
/// </summary>
/// <param name="Type">The probe's answer.</param>
/// <param name="CommitFloor">The commit floor of the caller's own staged intent on the key, when the probe asked
/// for it and a snapshot read left one; zero otherwise.</param>
internal readonly record struct KeyValueConflictProbeResult(KeyValueResponseType Type, HLCTimestamp CommitFloor);
