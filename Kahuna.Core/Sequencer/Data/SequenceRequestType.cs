namespace Kahuna.Server.Sequencer.Data;

/// <summary>
/// Operations a <see cref="SequenceActor"/> understands.
/// </summary>
internal enum SequenceRequestType
{
    /// <summary>Creates the sequence record if it does not already exist.</summary>
    Create,

    /// <summary>Allocates <c>Count</c> consecutive values, serving them from the actor's reserved block when possible.</summary>
    Reserve,

    /// <summary>Removes the sequence record and discards the actor's reserved block.</summary>
    Delete,

    /// <summary>
    /// Rewrites the sequence's parameters — its current value above all — and breaks its identity as a
    /// value stream so that any block reserved from the previous record is voided rather than drained.
    /// </summary>
    Update,

    /// <summary>
    /// Discards every reserved block held by the actor. Sent when partition leadership moves, because a
    /// block's exclusivity was established against a revision chain whose owning partition just changed
    /// hands; the abandoned tail of each block becomes a gap, never a duplicate.
    /// </summary>
    Invalidate
}
