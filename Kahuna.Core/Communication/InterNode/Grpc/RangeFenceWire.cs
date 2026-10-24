using Google.Protobuf.Collections;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues.Writes;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Moves a durable submission's range fences across the inter-node wire. The wire carries the whole fence list
/// in a repeated field, and mirrors the first fence into the single-fence fields that older nodes read: an older
/// receiver then fences one consistent key/generation pair (a subset of the fences, never a mismatched pair), and
/// a newer receiver reads the list when the sender filled it and falls back to the single pair otherwise.
/// </summary>
internal static class RangeFenceWire
{
    /// <summary>Fills <paramref name="target"/> from <paramref name="fences"/> and answers the single-fence
    /// mirror of the first fence (an empty key and zero when the submission is unfenced).</summary>
    public static (string LegacyKey, long LegacyGeneration) Write(IReadOnlyList<RangeFence> fences, RepeatedField<GrpcRangeFence> target)
    {
        for (int i = 0; i < fences.Count; i++)
            target.Add(new GrpcRangeFence { Key = fences[i].Key, Generation = fences[i].Generation });

        return fences.Count > 0 ? (fences[0].Key, fences[0].Generation) : (string.Empty, 0);
    }

    /// <summary>Reads the fence list a sender filled, or the single pair an older sender carried; empty means
    /// the submission is unfenced.</summary>
    public static IReadOnlyList<RangeFence> Read(RepeatedField<GrpcRangeFence> fences, string legacyKey, long legacyGeneration)
    {
        if (fences.Count > 0)
        {
            RangeFence[] read = new RangeFence[fences.Count];
            for (int i = 0; i < fences.Count; i++)
                read[i] = new RangeFence(fences[i].Key, fences[i].Generation);
            return read;
        }

        return string.IsNullOrEmpty(legacyKey) ? [] : [new RangeFence(legacyKey, legacyGeneration)];
    }
}
