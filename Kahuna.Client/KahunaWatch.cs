/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Client;

/// <summary>
/// An open watch: the frames of every committed mutation in its scope, from the nodes that serve it. Enumerate it
/// (or call <see cref="ReadAllAsync"/>) to read the frames, and dispose it to close the watch on every node.
///
/// <para><b>Registration.</b> The watch is registered on every partition of its scope before
/// <see cref="KahunaClient.WatchKeyValues"/> returns it. A write committed after that call returned produces an
/// event, once the serving replica applied it.</para>
///
/// <para><b>Delivery.</b> Delivery is at-least-once. For one key, the first delivery of each mutation arrives in
/// order, but a repeat of an older one can arrive later, so a consumer drops an event that
/// <see cref="KeyValueWatchEvent.Supersedes"/> does not report newer than the last event it holds for that key (an
/// extend keeps the revision, so the revision alone is not enough). Across keys no order is promised, and events of
/// different partitions can interleave in any order.</para>
///
/// <para><b>Endings.</b> The stream ends with one terminal frame and nothing after it:
/// <see cref="KeyValueWatchFrameType.Lagged"/> when continuity is lost (the reader fell a full capacity behind, a
/// serving node replaced a partition's state, failed an apply, saw the range map move the scope, shut down, or the
/// connection dropped), <see cref="KeyValueWatchFrameType.HistoryUnavailable"/> when a resume cannot be served, or
/// <see cref="KeyValueWatchFrameType.Refused"/>. Events between the last delivered one and the end can be missing.
/// The consumer opens a new watch from its last watermarks (see <see cref="Watermarks"/>) to have them replayed, or,
/// with no watermark (the commit admission window is off) or after <c>HistoryUnavailable</c>, treats everything the
/// watch covered as possibly changed.</para>
///
/// <para><b>Fan-out.</b> When no single node hosts every partition of the scope (the replication factor is below
/// the node count), the watch runs on several nodes, each limited to the partitions it hosts. A terminal frame from
/// any of them ends the whole watch and closes the others.</para>
///
/// <para><b>Resume.</b> The watch keeps the last progress watermark of each partition the reader was handed
/// (<see cref="Watermarks"/>): every event below it was handed to the reader before it. A consumer stores it together
/// with the state it built from those events, and opens a later watch from it
/// (<see cref="KeyValueWatchRequest.ResumeFrom"/>). With
/// automatic resume on, a node stream that ends with <see cref="KeyValueWatchFrameType.Lagged"/> (overflow, a dropped
/// connection, a shutdown, a replaced partition state, a failed apply) is reopened from those watermarks, and the node
/// replays what was missed: the consumer sees no lagged frame, but can see events again. A stream is resumed only when
/// every one of its partitions has a watermark. A <see cref="KeyValueWatchFrameType.HistoryUnavailable"/> frame, a
/// refusal, a range-map change and a watermark violation are never hidden: they end the watch.</para>
/// </summary>
public sealed class KahunaWatch : IAsyncEnumerable<KeyValueWatchFrame>, IAsyncDisposable
{
    // Consecutive resumes of one node stream with no frame in between before the watch gives up on it.
    private const int MaxConsecutiveResumes = 8;

    // Frames buffered between the node streams and the reader. A full buffer holds the node streams back, and the
    // serving node then ends the watch with an overflow once its own buffer is full.
    private const int MergeCapacity = 1024;

    private static readonly KeyValueWatchFrame ClosedFrame = KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.None, HLCTimestamp.Zero);

    private readonly Channel<KeyValueWatchFrame> merged = Channel.CreateBounded<KeyValueWatchFrame>(new BoundedChannelOptions(MergeCapacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = true,
        SingleWriter = false
    });

    private readonly CancellationTokenSource stop = new();

    private readonly Task[] pumps;

    private readonly KahunaWatchResumer? resumer;

    // The last watermark the reader was handed per partition: the public checkpoint.
    private readonly ConcurrentDictionary<int, HLCTimestamp> watermarks = new();

    // The last watermark a node stream delivered per partition, ahead of what the reader took: the automatic resume
    // reopens from it, because the frames between the two are still in the merge buffer and survive the resume.
    private readonly ConcurrentDictionary<int, HLCTimestamp> received = new();

    private KeyValueWatchFrame? terminal;

    internal KahunaWatch(KeyValueWatchScope scope, IReadOnlyList<KahunaWatchPart> parts, IReadOnlyList<int> partitions, IReadOnlyList<string> endpoints, KahunaWatchResumer? resumer = null)
    {
        Scope = scope;
        Partitions = partitions;
        Endpoints = endpoints;
        this.resumer = resumer;

        pumps = new Task[parts.Count];
        for (int i = 0; i < parts.Count; i++)
            pumps[i] = Pump(parts[i]);
    }

    /// <summary>The keys the watch covers.</summary>
    public KeyValueWatchScope Scope { get; }

    /// <summary>The data partitions the watch is registered on, across every serving node, sorted.</summary>
    public IReadOnlyList<int> Partitions { get; }

    /// <summary>The nodes that serve the watch.</summary>
    public IReadOnlyList<string> Endpoints { get; }

    /// <summary>The last progress watermark the reader was handed for each partition: no event at or below it arrives
    /// later on that partition, and every earlier one was handed to the reader first. The resume cursor of the
    /// partition, once the consumer processed what it read. Empty until the reader takes a progress frame, and while
    /// the nodes send none (the commit admission window is off).</summary>
    public IReadOnlyDictionary<int, HLCTimestamp> Watermarks => new Dictionary<int, HLCTimestamp>(watermarks);

    /// <summary>
    /// Reads the frames until the terminal frame, which is delivered last, or until <paramref name="cancellationToken"/>
    /// is cancelled. Only one reader may read a watch. A watch closed by <see cref="DisposeAsync"/> ends without a frame.
    /// </summary>
    public async IAsyncEnumerable<KeyValueWatchFrame> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ChannelReader<KeyValueWatchFrame> reader = merged.Reader;

        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (reader.TryRead(out KeyValueWatchFrame? frame))
            {
                // Advanced here, not when the frame arrived: the frames before it were handed to the reader already.
                if (frame.Type == KeyValueWatchFrameType.Progress)
                    Advance(watermarks, frame);

                yield return frame;
            }
        }

        KeyValueWatchFrame? end = Volatile.Read(ref terminal);
        if (end is not null && !ReferenceEquals(end, ClosedFrame))
            yield return end;
    }

    public IAsyncEnumerator<KeyValueWatchFrame> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);

    /// <summary>Closes the watch on every node. Idempotent.</summary>
    public async ValueTask DisposeAsync()
    {
        End(ClosedFrame);

        try
        {
            await Task.WhenAll(pumps).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        stop.Dispose();
    }

    private async Task Pump(KahunaWatchPart part)
    {
        KahunaWatchPart current = part;
        int consecutiveResumes = 0;

        try
        {
            while (true)
            {
                KeyValueWatchFrame? end = null;

                await foreach (KeyValueWatchFrame frame in current.Stream.ReadAllAsync(stop.Token).ConfigureAwait(false))
                {
                    if (frame.IsTerminal)
                    {
                        end = frame;
                        break;
                    }

                    consecutiveResumes = 0;

                    if (frame.Type == KeyValueWatchFrameType.Progress)
                        Advance(received, frame);

                    await merged.Writer.WriteAsync(frame, stop.Token).ConfigureAwait(false);
                }

                // A stream that ended with no frame was closed by this watch.
                if (end is null)
                    return;

                if (resumer is null
                    || consecutiveResumes >= MaxConsecutiveResumes
                    || !IsResumable(end)
                    || !TryCursors(current.Partitions, out Dictionary<int, HLCTimestamp> cursors))
                {
                    End(end);
                    return;
                }

                await current.Stream.DisposeAsync().ConfigureAwait(false);

                (KahunaWatchPart? resumed, KeyValueWatchFrame? failure) = await resumer(current, cursors, stop.Token).ConfigureAwait(false);
                if (resumed is not { } next)
                {
                    End(failure ?? end);
                    return;
                }

                current = next;
                consecutiveResumes++;
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
        {
        }
        catch (Exception)
        {
            End(KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.Disconnected, HLCTimestamp.Zero));
        }
        finally
        {
            await current.Stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    // A continuity loss a replay closes. A range-map change moved the scope's partitions, and a watermark violation
    // means a watermark cannot be trusted as a cursor: both end the watch.
    private static bool IsResumable(KeyValueWatchFrame end) =>
        end.Type == KeyValueWatchFrameType.Lagged
        && end.LagReason is KeyValueWatchLagReason.Overflow
            or KeyValueWatchLagReason.Disconnected
            or KeyValueWatchLagReason.Shutdown
            or KeyValueWatchLagReason.PartitionStateReplaced
            or KeyValueWatchLagReason.ApplyDiscontinuity;

    private static void Advance(ConcurrentDictionary<int, HLCTimestamp> cursors, KeyValueWatchFrame progress) =>
        cursors.AddOrUpdate(progress.PartitionId, progress.Timestamp, (_, known) => progress.Timestamp > known ? progress.Timestamp : known);

    // Every partition of the stream needs a cursor: one with none could have events the consumer never received,
    // below any timestamp the client could pick.
    private bool TryCursors(IReadOnlyList<int> partitions, out Dictionary<int, HLCTimestamp> cursors)
    {
        cursors = new Dictionary<int, HLCTimestamp>(partitions.Count);

        foreach (int partitionId in partitions)
        {
            if (!received.TryGetValue(partitionId, out HLCTimestamp cursor))
                return false;

            cursors[partitionId] = cursor;
        }

        return partitions.Count > 0;
    }

    // Sets the frame that ends the watch (the first one wins), stops every node stream and lets the reader drain
    // what is already buffered before it gets the terminal frame.
    private void End(KeyValueWatchFrame frame)
    {
        if (Interlocked.CompareExchange(ref terminal, frame, null) is not null)
            return;

        stop.Cancel();
        merged.Writer.TryComplete();
    }
}

/// <summary>One node stream of a watch: the node, the stream, and the partitions it serves.</summary>
internal readonly record struct KahunaWatchPart(string Url, IKahunaWatchStream Stream, IReadOnlyList<int> Partitions);

/// <summary>Reopens one node stream of a watch from <paramref name="cursors"/>: the new part, or the frame that ends the
/// watch when it cannot be reopened.</summary>
internal delegate Task<(KahunaWatchPart? Part, KeyValueWatchFrame? Failure)> KahunaWatchResumer(
    KahunaWatchPart part, IReadOnlyDictionary<int, HLCTimestamp> cursors, CancellationToken cancellationToken);
