
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// How a point read, a batched point probe, a range page or a batched range probe treats the state of the reading
/// transaction when the read carries a read timestamp.
/// </summary>
public enum KeyValueReadMode
{
    /// <summary>
    /// The read shape every caller had before the mode existed. Without a read timestamp a transactional read
    /// pins the committed state it observes and serves that pin on every later read of the key. With a read
    /// timestamp the read serves the committed state at that timestamp and does not show the transaction's own
    /// staged writes.
    /// </summary>
    Default = 0,

    /// <summary>
    /// A read at a timestamp inside a transaction, one timestamp per statement. The transaction's own staged
    /// writes are visible. Every other key answers with the committed state at the timestamp. A read observation
    /// an earlier read of the transaction left on a key is neither served nor checked, and the read leaves no
    /// observation behind, so a later statement at a later timestamp sees later commits. A registered read still
    /// records the revision it served for commit-time validation. Requires a read timestamp; with a zero
    /// transaction id it behaves as <see cref="Default"/>.
    /// </summary>
    StatementSnapshot = 1
}
