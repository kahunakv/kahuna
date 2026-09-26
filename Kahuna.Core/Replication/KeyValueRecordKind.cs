
namespace Kahuna.Server.Replication;

/// <summary>
/// How a consumer of a logged <c>KeyValueMessage</c> applies it. Every consumer of the key-value log (the
/// replicator on live apply, the restorer on restart replay, and the point-in-time restore engine) picks its
/// path by this kind, not by the request type, so a request type that a producer logs reaches all of them
/// through one classification in <see cref="KeyValueMessageDecoder.Classify"/>.
///
/// <para>The kind exists because the consumers once each kept their own list of the types they apply. A type
/// added to a producer but missing from one list was skipped there as an unknown message, and that is a
/// silently lost write on the node that replays it.</para>
/// </summary>
internal enum KeyValueRecordKind
{
    /// <summary>A value this build does not name: a record from a newer build, or a corrupt type.</summary>
    Unknown = 0,

    /// <summary>
    /// A request type no producer writes into a log: reads, scans, locks, 2PC control and actor-internal
    /// messages. A producer that tries to log one is refused by <see cref="KeyValueMessageDecoder.ToLoggedType"/>.
    /// </summary>
    NotLogged,

    /// <summary>A committed mutation that carries its own value (or its absence) in the record.</summary>
    ValueMutation,

    /// <summary>
    /// A committed mutation that carries no value and names the prepared intent that holds it; the consumer
    /// resolves the value from its own prepared-intent store.
    /// </summary>
    ByReferenceMutation,
}
