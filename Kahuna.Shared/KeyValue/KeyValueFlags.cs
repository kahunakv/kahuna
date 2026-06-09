
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents the set of flags used to determine the behavior of key-value "set" operations.
/// </summary>
[Flags]
public enum KeyValueFlags
{
    None = 0,
    Set = 1 << 0,                    // 1
    SetNoRevision = 1 << 1,          // 2
    SetIfExists = 1 << 2,            // 4
    SetIfNotExists = 1 << 3,         // 8
    SetIfEqualToValue = 1 << 4,      // 16
    SetIfEqualToRevision = 1 << 5,   // 32

    /// <summary>
    /// Internal signal on a ReleaseProposal: the proposal was rejected by the key-range generation
    /// fence (Task 4), so the release resolves the client promise as <c>MustRetry</c> (re-resolve
    /// LocateRange and retry), not <c>Errored</c>.
    /// </summary>
    FenceRetry = 1 << 6              // 64
}