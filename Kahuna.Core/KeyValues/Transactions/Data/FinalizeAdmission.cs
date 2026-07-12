
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The result of a caller attempting to enter a session's single finalize slot.
/// </summary>
internal enum FinalizeAdmission
{
    /// <summary>The caller installed the active attempt and must run the finalize and publish its outcome.</summary>
    Owner,

    /// <summary>Another finalize is already active; the caller awaits and mirrors its outcome.</summary>
    Mirror,

    /// <summary>The session is being reaped or is already terminal; there is nothing left to finalize.</summary>
    Rejected
}
