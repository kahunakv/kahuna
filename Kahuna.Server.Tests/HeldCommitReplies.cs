using Kommander;
using Kommander.Data;

namespace Kahuna.Server.Tests;

/// <summary>
/// Holds the replies of committed proposals on one partition of one node, so a fixture can model the window a
/// finalize cannot otherwise reach: the entry is durable on a quorum, every other replica can see it, the
/// leader applied it, and the coordinator that proposed it has learned nothing.
///
/// <para>A finalize awaits the very completion its answer rides on, so "durable at quorum" and "the coordinator
/// learned the outcome" are the same event in an ordinary in-process test. Kommander's reply hold separates
/// them. Only the reply is held: the commit-marker fan-out, the leader's local applies, the frontier advance and
/// follower delivery have all already run by the time a reply reaches here.</para>
///
/// <para><b>Every</b> committed reply on the partition is offered while the registration is live, so the fixture
/// says how many leading replies to let through with <paramref name="releaseFirst"/> and the rest are held. The
/// count is how a fixture selects one stage of a two-phase finalize: on a quiet partition the anchor bundle
/// (record initialize plus prepare) answers first and the decision second. A fixture never asserts on that
/// count alone — it waits for the node's own record and intent state to show which stage landed, which is what
/// makes the selection provable rather than assumed.</para>
///
/// <para>The hold is bounded by the node's <c>ProposalTimeout</c>: a fixture that means to hold across a
/// leadership change must raise that bound for its cluster, or the reply resolves underneath its assertions.</para>
/// </summary>
internal sealed class HeldCommitReplies : IDisposable
{
    private readonly IDisposable registration;

    private readonly List<HeldProposalReply> held = [];

    private readonly object gate = new();

    private int releasesLeft;

    private bool passThrough;

    /// <param name="raft">The node whose partition replies are intercepted. A partition this node does not host
    /// yields a handle that holds nothing.</param>
    /// <param name="partitionId">Partition to intercept.</param>
    /// <param name="releaseFirst">How many leading replies to answer normally before holding starts.</param>
    public HeldCommitReplies(IRaft raft, int partitionId, int releaseFirst = 0)
    {
        releasesLeft = releaseFirst;
        registration = raft.HoldCommittedProposalRepliesForTesting(partitionId, OnHeld);
    }

    /// <summary>Replies intercepted and still unanswered.</summary>
    public int Count
    {
        get { lock (gate) return held.Count; }
    }

    /// <summary>Replies answered normally because they arrived inside the leading allowance.</summary>
    public int Released
    {
        get { lock (gate) return released; }
    }

    private int released;

    private void OnHeld(HeldProposalReply reply)
    {
        lock (gate)
        {
            if (!passThrough && releasesLeft <= 0)
            {
                held.Add(reply);
                return;
            }

            if (releasesLeft > 0)
            {
                releasesLeft--;
                released++;
            }
        }

        // Outside the lock: resolving runs the waiter's continuations, which must not run under a lock the
        // partition thread also takes to offer the next reply.
        reply.Release();
    }

    /// <summary>
    /// Answers every reply held so far with its committed outcome and stops intercepting — the leader answers,
    /// late. The proposals were always durable; this is only when their callers find out.
    /// </summary>
    public void ReleaseAll() => Resolve(release: true);

    /// <summary>
    /// Abandons every reply held so far and stops intercepting: each caller waits out its own bound and reports
    /// an indeterminate outcome. This is the killed-leader shape — nobody ever answers.
    /// </summary>
    public void DropAll() => Resolve(release: false);

    private void Resolve(bool release)
    {
        HeldProposalReply[] pending;

        lock (gate)
        {
            passThrough = true;
            pending = [.. held];
            held.Clear();
        }

        foreach (HeldProposalReply reply in pending)
        {
            if (release)
                reply.Release();
            else
                reply.Drop();
        }
    }

    /// <summary>Detaches the registration. Anything still held is answered by Kommander so no caller is stranded.</summary>
    public void Dispose() => registration.Dispose();
}
