using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>Why a prepare's acknowledgement was refused at its apply, as far as the acknowledging leader can
/// tell: <see cref="KeyHeld"/> — another transaction's live intent owns the key (a retryable conflict once that
/// intent settles, if it is decided); <see cref="StaleBase"/> — the key's committed head moved past the validated
/// base (final: a retry re-asks the same stale question). <see cref="None"/> means acknowledged, or unknown.</summary>
internal enum PrepareRejectionKind
{
    None = 0,
    KeyHeld = 1,
    StaleBase = 2,

    /// <summary>Never sent on the wire: the origin's own classification of a refused prepare whose partition
    /// the key no longer routes to (a split or merge since freeze released the fenced submission). Final for
    /// this frozen input; the caller must retry from a fresh freeze.</summary>
    RangeMoved = 3
}

/// <summary>A remote partition leader's answer to a forwarded durable bundle. The two signals stay separate:
/// a committed batch whose prepare was refused must drive a truthful abort (the record exists), while a batch
/// that never committed is a clean retry with nothing durable. <paramref name="Rejection"/> names the refusal.</summary>
internal readonly record struct DurableBundleReply(bool BatchCommitted, bool PrepareAcknowledged, PrepareRejectionKind Rejection)
{
    public DurableBundleWireReply ToWire() => new(BatchCommitted, PrepareAcknowledged, (int)Rejection);

    public static DurableBundleReply FromWire(DurableBundleWireReply wire) =>
        new(wire.BatchCommitted, wire.PrepareAcknowledged, (PrepareRejectionKind)wire.PrepareRejection);
}

/// <summary>A remote anchor leader's answer to a forwarded terminal decision: whether the batch carrying it
/// committed, and — when <paramref name="Known"/> — the CANONICAL decision read from the leader's record store
/// after the ordered apply, which is what a competing decision that won in log order leaves there. When
/// <paramref name="Known"/> is false the sender must look the record up itself.</summary>
internal readonly record struct DurableDecisionReply(bool Replicated, bool Known, TransactionDecision Decision, TransactionAbortClass AbortClass)
{
    public DurableDecisionWireReply ToWire() => new(Replicated, Known, (int)Decision, (int)AbortClass);

    public static DurableDecisionReply FromWire(DurableDecisionWireReply wire) =>
        new(wire.Replicated, wire.Known, (TransactionDecision)wire.Decision, (TransactionAbortClass)wire.AbortClass);
}

/// <summary>The anchor leader's answer to a one-phase bundle ([record init, prepare, commit decision] as one
/// atomic batch): the bundle signals of <see cref="DurableBundleReply"/>, plus the CANONICAL outcome read from
/// the leader's record store after the ordered apply — the batch is durable but the commit transition is judged
/// at apply, so the batch signals alone can never name the winner. When the record stays Undecided,
/// <paramref name="GatedVerdict"/> names the bundled-commit gate's rejection (<see cref="BundledCommitVerdict.Admit"/>
/// when the gate recorded none — the deadline gate withheld the commit instead).</summary>
internal readonly record struct DurableOnePhaseReply(
    bool BatchCommitted, bool PrepareAcknowledged, PrepareRejectionKind Rejection,
    bool DecisionKnown, TransactionDecision Decision, TransactionAbortClass AbortClass, BundledCommitVerdict GatedVerdict)
{
    public DurableOnePhaseWireReply ToWire() =>
        new(BatchCommitted, PrepareAcknowledged, (int)Rejection, DecisionKnown, (int)Decision, (int)AbortClass, (int)GatedVerdict);

    public static DurableOnePhaseReply FromWire(DurableOnePhaseWireReply wire) => new(
        wire.BatchCommitted, wire.PrepareAcknowledged, (PrepareRejectionKind)wire.PrepareRejection,
        wire.DecisionKnown, (TransactionDecision)wire.Decision, (TransactionAbortClass)wire.AbortClass,
        (BundledCommitVerdict)wire.GatedVerdict);
}

/// <summary>The public wire shape of <see cref="DurableBundleReply"/>, for the node and transport contracts:
/// <paramref name="PrepareRejection"/> is 0 none, 1 a foreign intent holds the key, 2 the validated base moved.</summary>
public readonly record struct DurableBundleWireReply(bool BatchCommitted, bool PrepareAcknowledged, int PrepareRejection);

/// <summary>The public wire shape of <see cref="DurableOnePhaseReply"/>, for the node and transport contracts:
/// <paramref name="Decision"/>, <paramref name="AbortClass"/> and <paramref name="GatedVerdict"/> carry the
/// internal enum values (<paramref name="GatedVerdict"/> 0 = no gate rejection recorded).</summary>
public readonly record struct DurableOnePhaseWireReply(
    bool BatchCommitted, bool PrepareAcknowledged, int PrepareRejection,
    bool DecisionKnown, int Decision, int AbortClass, int GatedVerdict);

/// <summary>The public wire shape of <see cref="DurableDecisionReply"/>, for the node and transport contracts:
/// <paramref name="Decision"/> and <paramref name="AbortClass"/> carry the internal enum values.</summary>
public readonly record struct DurableDecisionWireReply(bool Replicated, bool Known, int Decision, int AbortClass);
