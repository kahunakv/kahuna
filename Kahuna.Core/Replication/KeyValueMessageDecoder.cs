
using System.Runtime.InteropServices;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Replication.Protos;
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

        byte[]? value;
        if (MemoryMarshal.TryGetArray(msg.Value.Memory, out ArraySegment<byte> seg))
            value = seg.Array;
        else
            value = msg.Value.ToByteArray();

        return (state, value);
    }
}
