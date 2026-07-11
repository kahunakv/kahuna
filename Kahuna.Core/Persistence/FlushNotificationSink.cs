
namespace Kahuna.Server.Persistence;

/// <summary>
/// Shared, late-bound bridge from the <see cref="BackgroundWriterActor"/> back to the key-value
/// layer. The writer is spawned before the <c>KeyValuesManager</c> exists, so the callback that
/// routes a flush acknowledgement to the owning actor cannot be supplied at construction time.
/// This holder is created first, handed to the writer, and populated once the key-value router is
/// available. A null <see cref="OnKeyValueFlushed"/> (before wiring, or in setups without a
/// key-value layer) makes <see cref="NotifyFlushed"/> a no-op.
/// </summary>
internal sealed class FlushNotificationSink
{
    /// <summary>
    /// Invoked with <c>(key, revision)</c> after the background writer confirms a key-value batch
    /// is durably stored. Set once the key-value router is constructed.
    /// </summary>
    public Action<string, long>? OnKeyValueFlushed { get; set; }

    public void NotifyFlushed(string key, long revision) => OnKeyValueFlushed?.Invoke(key, revision);
}
