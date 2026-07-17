
namespace Kahuna.Server.KeyValues;

/// <summary>
/// The kind of two-phase-commit Raft round trip a <see cref="KeyValuePhaseTwoRequest"/> asks the
/// off-mailbox worker to perform: the prepare proposal, the commit of a prepared ticket, or the
/// rollback of a prepared ticket. Each maps to a distinct <see cref="Kommander.IRaft"/> call.
/// </summary>
internal enum PhaseTwoOpKind
{
    Prepare,
    Commit,
    Rollback,
}
