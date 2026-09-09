using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Materializes a group of committed intents into key/value records in scheduler-sized windows: every record
/// of a window is submitted before the window is awaited, so the shared partition write scheduler can coalesce
/// the window into one capped proposal, while a group larger than the scheduler's admission capacity advances
/// window by window instead of admitting its whole working set at once. Shared by the finalizer's own
/// resolution and by every recovery path (the sweep, the prepare-conflict helping pass, the range-move
/// barrier), so a successor blocked behind a predecessor with many keys does not wait one durable round per key.
///
/// <para>Returns one flag per intent: true when that intent's record replicated. A false or thrown
/// replication leaves the flag false and the intent for a later pass; the caller decides what to settle.
/// The abort fence is the caller's: it must be checked before anything reaches the log.</para>
/// </summary>
internal static class DurableMaterializationWindow
{
    public static async Task<bool[]> MaterializeAsync(
        int partitionId,
        IReadOnlyList<PreparedIntent> intents,
        bool materializeByReference,
        int maxItems,
        long maxBytes,
        DurableTransactionFinalizer.ReplicateDelegate replicate,
        CancellationToken cancellationToken)
    {
        bool[] materialized = new bool[intents.Count];
        int next = 0;

        if (intents.Count == 0)
            return materialized;

        // One scratch message serves the whole group; each serialization fully consumes it before the next
        // intent overwrites it.
        KeyValueMessage scratch = new();

        // The record that overflowed the previous window's byte limit. It belongs to the intent at `next`
        // (the cursor did not advance on overflow), so the next window consumes it as its first entry
        // instead of serializing the same intent a second time.
        byte[]? carriedRecord = null;

        while (next < intents.Count)
        {
            List<(int Index, byte[] Record)> window = new(Math.Min(maxItems, intents.Count - next));
            long windowBytes = 0;

            while (next < intents.Count && window.Count < maxItems)
            {
                byte[] record = carriedRecord ?? PreparedIntentMaterializer.ToKeyValueRecord(intents[next], scratch, materializeByReference);
                if (window.Count > 0 && windowBytes + record.Length > maxBytes)
                {
                    carriedRecord = record;
                    break;
                }

                carriedRecord = null;
                window.Add((next, record));
                windowBytes += record.Length;
                next++;

                if (windowBytes >= maxBytes)
                    break;
            }

            Task<bool>[] tasks = new Task<bool>[window.Count];
            for (int i = 0; i < window.Count; i++)
                tasks[i] = MaterializeOneAsync(partitionId, window[i].Record, replicate, cancellationToken);

            bool[] results = await Task.WhenAll(tasks).ConfigureAwait(false);
            for (int i = 0; i < results.Length; i++)
                materialized[window[i].Index] = results[i];
        }

        return materialized;
    }

    private static async Task<bool> MaterializeOneAsync(int partitionId, byte[] kvRecord, DurableTransactionFinalizer.ReplicateDelegate replicate, CancellationToken cancellationToken)
    {
        try
        {
            // Replicate the committed value as an ordinary key/value record so followers converge. The leader
            // applies it separately through its owning actor after the durable record is acknowledged. This is
            // post-decision materialization — terminal work — so it draws on reserve capacity and is never
            // starved by an ordinary-write burst.
            return await replicate(partitionId, ReplicationTypes.KeyValues, kvRecord, Writes.WriteAdmissionClass.Terminal, Writes.WriteSubmissionStage.Materialize, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            return false;
        }
    }
}
