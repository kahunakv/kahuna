
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Controls whether reads performed inside a transaction are tracked so they can
/// be validated for write-skew during the prepare phase.
/// </summary>
public enum ReadValidation
{
    /// <summary>Reads are not tracked; no read-set conflict check is performed at commit.</summary>
    None,

    /// <summary>Every read is recorded and checked against concurrent writes at commit.</summary>
    TrackAndValidate
}

/// <summary>
/// Controls how durable the coordinator decision record must be before the client
/// receives the outcome.
/// </summary>
public enum DecisionDurability
{
    /// <summary>The decision may be held in memory; it can be lost if the coordinator crashes before the next checkpoint.</summary>
    BestEffort,

    /// <summary>The decision is written to durable storage before the response is sent.</summary>
    Durable
}
