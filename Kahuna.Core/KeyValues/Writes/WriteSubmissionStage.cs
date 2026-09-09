namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// The transaction stage (or non-transactional producer) that created a scheduler submission. The stage is
/// chosen at the submission's creation site — where the producer already knows what it is building — and
/// travels with the submission to dispatch, where it tags the per-submission queue-delay histogram. It is
/// never inferred from the submission's log types: several stages share the same (admission class, log type)
/// shape, so inference at dispatch cannot separate them.
///
/// <para>The values are bounded and stable. They also cross the inter-node durable-bundle wire as integers,
/// so an origin's stage survives forwarding to a remote partition leader; the numeric values must not be
/// reordered. An older sender that does not carry the field yields <see cref="Other"/>.</para>
/// </summary>
public enum WriteSubmissionStage
{
    /// <summary>No specific transaction stage: direct key/value writes, recovery record purges,
    /// state-transfer imports, and untyped forwards from older nodes.</summary>
    Other = 0,

    /// <summary>A one-phase commit's bundled proposal: [record init + anchor prepare + commit decision]
    /// as one atomic batch.</summary>
    OnePhase = 1,

    /// <summary>A standalone canonical-record initialization (the unbundled 2PC init, when the anchor key
    /// routes outside the participant partitions).</summary>
    RecordInit = 2,

    /// <summary>A first-barrier 2PC prepare. The anchor's [record init + prepare] bundle is one submission
    /// and is tagged with this stage: it is the transaction's first prepare barrier, and the init rides in it.</summary>
    Prepare = 3,

    /// <summary>A prepare re-proposed by the finalizer's conflict-retry loop.</summary>
    RePrepare = 4,

    /// <summary>A terminal commit/abort decision record, whether driven by a finalize or by recovery's
    /// presumed abort.</summary>
    Decision = 5,

    /// <summary>A post-decision materialization of a committed intent's value into a key/value record.</summary>
    Materialize = 6,

    /// <summary>A post-decision intent settlement (resolve + remove).</summary>
    Settle = 7
}
