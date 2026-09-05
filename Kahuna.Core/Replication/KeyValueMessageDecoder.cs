
using Kahuna.Server.KeyValues;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Replication;

/// <summary>
/// Single authoritative decode of <see cref="KeyValueMessage"/> into a mutation classification.
/// Both <c>KeyValueRestorer</c> (Raft log replay) and <c>RestoreEngine</c> (PITR WAL replay)
/// delegate here so that adding a new mutating <see cref="KeyValueRequestType"/> is one edit
/// in one place.
/// </summary>
internal static class KeyValueMessageDecoder
{
    /// <summary>
    /// Classifies <paramref name="msg"/> as a persistent state mutation and extracts the value
    /// bytes. Returns <c>KeyValueState.Undefined</c> for read-only operations, lock operations,
    /// scan operations, and 2PC control messages — callers should skip those entries.
    ///
    /// <para><see cref="KeyValueRequestType.MaterializeIntent"/> is deliberately NOT classified here: it
    /// carries no value, so its mutation cannot be read from the record at all. Every caller must resolve it
    /// against a prepared intent before reaching this decode, and both do.</para>
    /// </summary>
    internal static (KeyValueState state, byte[]? value) Decode(KeyValueMessage msg)
    {
        KeyValueState state = (KeyValueRequestType)msg.Type switch
        {
            KeyValueRequestType.TrySet    => KeyValueState.Set,
            KeyValueRequestType.TryExtend => KeyValueState.Set,
            KeyValueRequestType.TryDelete => KeyValueState.Deleted,
            _                             => KeyValueState.Undefined
        };

        if (state == KeyValueState.Undefined)
            return (KeyValueState.Undefined, null);

        // The proposal encoder clears the value field for a value-less write, so presence is the only
        // thing separating "set to no value" from "set to zero bytes". Reading the field alone would give
        // a follower an empty array where the leader holds null, and that divergence outlives the apply:
        // it becomes what the next read of this key returns once the follower is elected.
        byte[]? value = ByteStringPayload.GetArrayOrNull(msg.HasValue, msg.Value);

        return (state, value);
    }
}
