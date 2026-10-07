using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The lowest commit timestamp among the durable prepared intents this node held while the observation was
/// open: every intent live when it opened, and every intent installed on this node until it is disposed. An
/// aborted intent does not count, because it never installs a row.
///
/// <para>A coordinated backup opens one before it chooses its cut and reads it after the capture. An intent can
/// settle and leave the store while the observation is open; it is still counted, because it was seen when it
/// was live or when it was installed. An intent that settled before the observation opened is not counted: its
/// committed row was materialized on this node before the open, so it is in anything captured after it.</para>
/// </summary>
internal sealed class PreparedIntentCommitObservation : IDisposable
{
    private readonly Action<PreparedIntentCommitObservation> unregister;

    private readonly object minLock = new();

    private HLCTimestamp min = HLCTimestamp.Zero;

    private int disposed;

    internal PreparedIntentCommitObservation(Action<PreparedIntentCommitObservation> unregister) =>
        this.unregister = unregister;

    /// <summary>The lowest observed commit timestamp, or <see cref="HLCTimestamp.Zero"/> when no intent that can
    /// commit was observed.</summary>
    public HLCTimestamp MinCommitTimestamp
    {
        get
        {
            lock (minLock)
                return min;
        }
    }

    internal void Observe(PreparedIntent intent)
    {
        if (intent.Resolution == PreparedIntentResolution.Aborted || intent.CommitTimestamp == HLCTimestamp.Zero)
            return;

        lock (minLock)
        {
            if (min == HLCTimestamp.Zero || intent.CommitTimestamp.CompareTo(min) < 0)
                min = intent.CommitTimestamp;
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
            unregister(this);
    }
}
