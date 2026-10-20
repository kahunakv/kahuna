namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// What one helping pass over a refused prepare's keys found and did. The finalizer's prepare retry loop reads
/// it to tell settlement lag from a live conflict: a blocker whose canonical record is already terminal will
/// settle (the pass settles it itself, or deferred settlement does), so waiting for it is productive and the
/// loop is bounded by the decision deadline; a blocker that is still undecided is a live coordinator inside
/// its decision window, and the loop keeps its bounded rounds.
/// </summary>
/// <param name="Inspected">True when the pass could read the keys' holders (it ran on the partition leader).
/// False when it could not (not the leader, or helping disabled): the counts are then meaningless.</param>
/// <param name="Settled">How many blocking intents the pass confirmed settled (their settle delta replicated).</param>
/// <param name="Decided">How many blocked keys were held by a transaction whose record is terminal, whether or
/// not the pass managed to settle them this round.</param>
/// <param name="Undecided">How many blocked keys were held by a transaction whose record is still undecided
/// or not yet initialized — a live conflict.</param>
internal readonly record struct BlockerHelpResult(bool Inspected, int Settled, int Decided, int Undecided)
{
    /// <summary>The pass could not look at the holders at all.</summary>
    internal static readonly BlockerHelpResult NotInspected = new(false, 0, 0, 0);

    /// <summary>The pass looked and found no foreign holder on any key.</summary>
    internal static readonly BlockerHelpResult NoBlockers = new(true, 0, 0, 0);

    /// <summary>Folds another partition's pass into this one.</summary>
    internal BlockerHelpResult Add(BlockerHelpResult other) =>
        new(Inspected || other.Inspected, Settled + other.Settled, Decided + other.Decided, Undecided + other.Undecided);
}
