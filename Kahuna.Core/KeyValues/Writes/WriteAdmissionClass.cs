namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Admission class of a submission to the shared partition write scheduler. The class decides which capacity a
/// submission may draw on, so work that <b>finishes</b> an already-prepared transaction can never be starved by
/// work that <b>starts</b> new writes.
/// </summary>
internal enum WriteAdmissionClass
{
    /// <summary>Work that adds new pending state: direct key/value writes, and a durable transaction's record
    /// initialize and prepared-intent prepare. Bounded strictly by the base item/byte budget — never allowed to
    /// consume the terminal reserve — so a burst of new writes backpressures instead of crowding out settlement.</summary>
    Ordinary,

    /// <summary>Work that resolves already-prepared state: a durable transaction's commit/abort decision record,
    /// its post-decision intent settlement, materialization, recovery re-drive, and topology-transfer imports.
    /// May draw on the reserve headroom above the base budget, so an ordinary-write burst saturating a partition
    /// can never reject the decision or settle that completes an in-flight transaction.</summary>
    Terminal
}
