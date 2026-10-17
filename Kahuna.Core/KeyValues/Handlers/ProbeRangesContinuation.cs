using Nixie;

using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stages 2 and 3 of a batched range probe whose stage 1 left seeks that need disk pages. Each such seek keeps its
/// own <see cref="RangeScanContinuation"/>, the merge state a single range page has, so a seek's answer is the single
/// page's answer; only where its pages are read differs.
/// <para>A round reads the next disk page of every seek still open as one scheduler operation, then merges them all
/// in one <c>ResumeRead</c> turn. A seek whose merge needs a further page (its rows were dead, as-of dropped or
/// merged already) stays open for the next round, so the common batch is one hand-off and one resume turn, not one
/// of each per seek.</para>
/// <para>A failure or the deadline answers every open seek <c>MustRetry</c>, never the whole batch, so a seek whose
/// Shared lock the probe took keeps reporting it.</para>
/// </summary>
internal sealed class ProbeRangesContinuation : ReadContinuation
{
    private readonly record struct Seek(int Index, bool Locked, RangeScanContinuation Scan);

    private readonly KeyValueProbeRangeResult[] results;

    private readonly string prefix;

    private readonly int partitionId;

    // The seeks still waiting for a page. Read by stage 2 on a scheduler thread and replaced by stage 3 on the
    // actor thread; a round's ResumeRead orders the two.
    private List<Seek> open = new(4);

    internal ProbeRangesContinuation(KeyValueProbeRangeResult[] results, string prefix, int partitionId, KeyValueReplyRef promise) : base(promise)
    {
        this.results = results;
        this.prefix = prefix;
        this.partitionId = partitionId;
    }

    /// <summary>The number of seeks still waiting for a disk page.</summary>
    internal int OpenSeeks => open.Count;

    /// <summary>Registers seek <paramref name="index"/>, whose next disk page the next round reads.</summary>
    internal void Add(int index, bool locked, RangeScanContinuation scan) => open.Add(new(index, locked, scan));

    /// <summary>
    /// Stage 2, scheduler thread: reads the next disk page of every open seek into its continuation. Touches only the
    /// continuations' page fields; every actor-owned mutation waits for the resume turn. Skipped once the deadline
    /// sweep expired the probe, and stops between pages if it expires meanwhile.
    /// </summary>
    internal void ReadPages(IPersistenceBackend backend)
    {
        foreach (Seek seek in open)
        {
            if (Cancelled)
            {
                KeyValueScanMetrics.ScansAbandonedCancelled.Add(1);
                return;
            }

            KeyValueScanMetrics.PagesReadOnScheduler.Add(1);
            seek.Scan.RangeScanPage = seek.Scan.ReadPage(backend, () => Cancelled);
        }
    }

    /// <summary>
    /// Stage 3, actor thread: merges the page each open seek received, answers the seeks the merge completed, and
    /// keeps open the seeks that need a further page. Returns true when no seek is open.
    /// </summary>
    internal bool MergePages(KeyValueContext context)
    {
        if (Faulted)
        {
            FailOpen();
            return true;
        }

        List<Seek>? stillOpen = null;

        foreach (Seek seek in open)
        {
            KeyValueResponse? answer = seek.Scan.MergePage(context);

            if (answer is null)
            {
                (stillOpen ??= []).Add(seek);
                continue;
            }

            results[seek.Index] = ToResult(answer, seek.Locked);
        }

        open = stillOpen ?? [];
        return open.Count == 0;
    }

    internal override void Execute(KeyValueContext context)
    {
        if (!MergePages(context))
        {
            DispatchPages(context);
            return;
        }

        context.PendingProbeBatches.Remove(this);
        Resolve(KeyValueResponse.ForProbeRanges(results));
    }

    /// <summary>
    /// Schedules one round: one scheduler operation reads the next page of every open seek, and its completion sends
    /// one <c>ResumeRead</c> carrying this continuation. Returns false when the scheduler refused the operation (back
    /// pressure, a stopping scheduler); every open seek then answers <c>MustRetry</c> and the batch is answered.
    /// </summary>
    internal bool DispatchPages(KeyValueContext context)
    {
        IPersistenceBackend backend = context.PersistenceBackend;

        Task<bool> round;
        try
        {
            round = context.BackendReadScheduler.EnqueueTask(partitionId, this, continuation =>
            {
                continuation.ReadPages(backend);
                return true;
            });
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(
                "KeyValueActor/TryProbeManyRanges: read scheduler rejected the pages of {Count} seeks under {Prefix}: {Ex}",
                open.Count, prefix, ex.Message);

            context.PendingProbeBatches.Remove(this);
            FailOpen();
            Resolve(KeyValueResponse.ForProbeRanges(results));
            return false;
        }

        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> self = context.ActorContext.Self;

        _ = round.ContinueWith(t =>
        {
            // Runs on a thread-pool thread: only the seeks' page fields were touched; the merge waits for the
            // ResumeRead message.
            if (!t.IsCompletedSuccessfully)
            {
                context.Logger.LogWarning(t.Exception?.GetBaseException(), "KeyValueActor/TryProbeManyRanges: disk page read faulted under {Prefix}", prefix);
                SetFaulted();
            }

            self.Send(new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = this });
        }, TaskScheduler.Default);

        return true;
    }

    /// <summary>
    /// Failure and deadline path (<see cref="ReadContinuation.Fail"/>, <see cref="ReadContinuation.Expire"/>): answers
    /// every open seek <c>MustRetry</c>, keeping its lock flag, and answers the batch. The whole-batch answer the
    /// caller of this path resolves afterwards then finds the reply already set.
    /// </summary>
    internal override void RemovePendingKey(KeyValueContext context)
    {
        context.PendingProbeBatches.Remove(this);
        FailOpen();
        Resolve(KeyValueResponse.ForProbeRanges(results));
    }

    /// <summary>Answers every open seek <c>MustRetry</c>, keeping its lock flag, and closes them.</summary>
    internal void FailOpen()
    {
        foreach (Seek seek in open)
            results[seek.Index] = KeyValueProbeRangeResult.MustRetry with { Locked = seek.Locked };

        open = [];
    }

    /// <summary>The probe result for one seek from the answer a single range page over its bounds gives.</summary>
    internal static KeyValueProbeRangeResult ToResult(KeyValueResponse? answer, bool locked)
    {
        if (answer is null)
            return KeyValueProbeRangeResult.MustRetry with { Type = KeyValueResponseType.Errored, Locked = locked };

        if (answer.RangeResult is { } page)
            return FromPage(page, locked);

        return KeyValueProbeRangeResult.MustRetry with { Type = answer.Type, Locked = locked };
    }

    /// <summary>The probe result for one seek from a single range page over its bounds.</summary>
    internal static KeyValueProbeRangeResult FromPage(KeyValueGetByRangeResult page, bool locked)
    {
        if (page.Type != KeyValueResponseType.Get)
            return KeyValueProbeRangeResult.MustRetry with { Type = page.Type, Locked = locked };

        string? resumeKey = null;

        if (page.HasMore)
        {
            // The cursor names the last key the page consumed, a returned row or a dead row the walk stopped on; the
            // rest of the seek starts strictly after it.
            if (page.NextCursor is not null && KeyValueRangeCursor.TryDecode(page.NextCursor, out string lastKey, out _, out _, out _))
                resumeKey = lastKey;
            else if (page.Items.Count > 0)
                resumeKey = page.Items[^1].Item1;
        }

        return new(KeyValueResponseType.Get, page.Items, page.HasMore, resumeKey, default, locked);
    }
}
