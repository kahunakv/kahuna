using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The admission gate itself: who starts next when the node is saturated, and — more importantly — the
/// invariants that make the gate safe to put in front of every transaction. A ceiling that can be breached
/// under contention, a slot that is lost when a caller gives up, a queue that grows without bound, or work
/// that can starve are each worse than having no gate at all, so those are tested directly rather than
/// inferred from ordering behavior.
///
/// <para>Elapsed time comes from a manually advanced <see cref="TimeProvider"/> so the aging tests assert an
/// exact policy rather than racing a real clock.</para>
/// </summary>
public sealed class TestTransactionPriorityOrderer
{
    /// <summary>A monotonic clock the test advances by hand.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        private long ticks = 1_000_000;

        public override long GetTimestamp() => Volatile.Read(ref ticks);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public void Advance(long milliseconds) => Interlocked.Add(ref ticks, milliseconds * TimeSpan.TicksPerMillisecond);
    }

    private static TransactionPriorityOrderer Build(
        int maxConcurrent,
        int reserved = 0,
        int agingThresholdMs = 0,
        int maxQueued = 0,
        ManualTimeProvider? time = null)
        => new(maxConcurrent, reserved, agingThresholdMs, maxQueued, time ?? new ManualTimeProvider());

    private static async Task<AdmissionLease> Admit(TransactionPriorityOrderer orderer, TransactionPriority priority)
    {
        AdmissionLease? lease = await orderer.AdmitAsync(priority, CancellationToken.None);

        Assert.NotNull(lease);

        return lease;
    }

    private static Task<AdmissionLease?> Park(TransactionPriorityOrderer orderer, TransactionPriority priority, CancellationToken cancellationToken = default)
        => orderer.AdmitAsync(priority, cancellationToken).AsTask();

    [Fact]
    public async Task BelowCeiling_EveryPriorityIsAdmittedWithoutWaiting()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 4);

        List<AdmissionLease> leases = [];

        foreach (TransactionPriority priority in new[] { TransactionPriority.Background, TransactionPriority.Low, TransactionPriority.High, TransactionPriority.Critical })
        {
            Task<AdmissionLease?> admission = Park(orderer, priority);

            // Completing synchronously is the contract for an unsaturated node: the gate must cost nothing
            // when it is not gating.
            Assert.True(admission.IsCompletedSuccessfully);

            leases.Add((await admission)!);
        }

        Assert.Equal(4, orderer.InFlight);
        Assert.Equal(0, orderer.Queued);

        foreach (AdmissionLease lease in leases)
            lease.Dispose();

        Assert.Equal(0, orderer.InFlight);
    }

    [Fact]
    public async Task AtCeiling_HigherPriorityStartsBeforeAnEarlierQueuedLowerPriority()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1);

        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        // Low queues first, High second. Arrival order deliberately favours Low so the assertion can only pass
        // on priority, not on chance.
        Task<AdmissionLease?> low = Park(orderer, TransactionPriority.Low);
        Task<AdmissionLease?> high = Park(orderer, TransactionPriority.High);

        Assert.False(low.IsCompleted);
        Assert.False(high.IsCompleted);
        Assert.Equal(2, orderer.Queued);

        occupant.Dispose();

        AdmissionLease started = (await high.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!;

        // The single freed slot went to High; Low is still waiting. This asserts start order, not merely that
        // both eventually ran.
        Assert.False(low.IsCompleted);

        started.Dispose();

        (await low.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();

        Assert.Equal(0, orderer.InFlight);
        Assert.Equal(0, orderer.Queued);
    }

    [Fact]
    public async Task UnderContention_InFlightNeverExceedsTheCeiling()
    {
        const int ceiling = 4;
        const int threads = 8;
        const int perThread = 40;

        using TransactionPriorityOrderer orderer = Build(maxConcurrent: ceiling);

        int concurrent = 0;
        int peak = 0;

        async Task Worker(int seed)
        {
            for (int i = 0; i < perThread; i++)
            {
                TransactionPriority priority = (TransactionPriority)((seed + i) % 5);

                using AdmissionLease lease = await Admit(orderer, priority);

                int now = Interlocked.Increment(ref concurrent);

                // Track the high-water mark without a lock: only ever raise it, and only towards the value
                // this thread actually observed.
                int observedPeak;
                while (now > (observedPeak = Volatile.Read(ref peak)))
                    Interlocked.CompareExchange(ref peak, now, observedPeak);

                await Task.Yield();

                Interlocked.Decrement(ref concurrent);
            }
        }

        await Task.WhenAll(Enumerable.Range(0, threads).Select(Worker));

        // The check-then-take handshake is the classic place for a ceiling to leak under contention.
        Assert.True(peak <= ceiling, $"observed {peak} concurrent admissions against a ceiling of {ceiling}");
        Assert.Equal(0, orderer.InFlight);
        Assert.Equal(0, orderer.Queued);
    }

    [Fact]
    public async Task Aging_LetsBackgroundWorkOvertakeAContinuousHighPriorityStream()
    {
        ManualTimeProvider time = new();
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1, agingThresholdMs: 100, time: time);

        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        Task<AdmissionLease?> background = Park(orderer, TransactionPriority.Background);

        // A stream of High work arrives after the background transaction and would, without aging, keep
        // taking every freed slot ahead of it forever.
        List<Task<AdmissionLease?>> stream = [];
        for (int i = 0; i < 3; i++)
        {
            time.Advance(10);
            stream.Add(Park(orderer, TransactionPriority.High));
        }

        // Long enough for the background waiter to age up to — but not past — High.
        time.Advance(500);

        occupant.Dispose();

        AdmissionLease started = (await background.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!;

        Assert.All(stream, s => Assert.False(s.IsCompleted));
        Assert.Equal(1, orderer.PromotedAt(TransactionPriority.Background));

        started.Dispose();

        foreach (Task<AdmissionLease?> pending in stream)
            (await pending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Fact]
    public async Task WithoutAging_HighPriorityKeepsOvertakingBackgroundWork()
    {
        ManualTimeProvider time = new();

        // Same shape as the aging test with aging switched off, so that test is shown to be asserting the
        // aging policy rather than an accident of arrival order.
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1, agingThresholdMs: 0, time: time);

        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        Task<AdmissionLease?> background = Park(orderer, TransactionPriority.Background);

        time.Advance(10);
        Task<AdmissionLease?> high = Park(orderer, TransactionPriority.High);

        time.Advance(500);

        occupant.Dispose();

        (await high.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();

        Assert.Equal(0, orderer.PromotedAt(TransactionPriority.Background));

        (await background.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Fact]
    public async Task AgingIsDrivenByElapsedTime_NotByUnrelatedClockActivity()
    {
        ManualTimeProvider time = new();
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1, agingThresholdMs: 1_000, time: time);

        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        Task<AdmissionLease?> background = Park(orderer, TransactionPriority.Background);
        Task<AdmissionLease?> high = Park(orderer, TransactionPriority.High);

        // Well short of the aging threshold. The orderer must measure this interval itself and must not
        // inherit a jump from any shared cluster clock — a single skewed peer dragging the Raft HLC forward
        // would otherwise promote every local waiter at once and erase the configured priority separation.
        time.Advance(100);

        occupant.Dispose();

        // No promotion has been earned, so priority still decides.
        (await high.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();

        Assert.Equal(0, orderer.PromotedAt(TransactionPriority.Background));

        (await background.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Fact]
    public async Task ReservedSlots_StayAvailableToHighPriorityUnderBulkLoad()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 2, reserved: 1);

        AdmissionLease bulk = await Admit(orderer, TransactionPriority.Background);

        // Ordinary work is capped at (ceiling - reserve), so a second background transaction waits.
        Task<AdmissionLease?> queuedBulk = Park(orderer, TransactionPriority.Background);
        Assert.False(queuedBulk.IsCompleted);

        // The reserved slot is exactly what High is for, so it starts immediately.
        Task<AdmissionLease?> urgent = Park(orderer, TransactionPriority.High);
        Assert.True(urgent.IsCompletedSuccessfully);

        Assert.Equal(2, orderer.InFlight);

        (await urgent)!.Dispose();
        bulk.Dispose();

        (await queuedBulk.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Fact]
    public async Task FreedCapacity_IsUsableByOrdinaryWorkEvenWhileHighPriorityHoldsTheReserve()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 2, reserved: 1);

        AdmissionLease firstUrgent = await Admit(orderer, TransactionPriority.High);
        AdmissionLease secondUrgent = await Admit(orderer, TransactionPriority.High);

        Task<AdmissionLease?> bulk = Park(orderer, TransactionPriority.Background);
        Assert.False(bulk.IsCompleted);

        secondUrgent.Dispose();

        // One High still runs and can be considered to hold the reserved slot; the slot it freed is ordinary
        // capacity and must be usable. Deciding this from the aggregate count instead would leave the node at
        // half utilization and — under a sustained high-priority stream — starve this waiter forever.
        AdmissionLease started = (await bulk.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!;

        Assert.Equal(2, orderer.InFlight);

        started.Dispose();
        firstUrgent.Dispose();

        Assert.Equal(0, orderer.InFlight);
    }

    [Fact]
    public async Task OrdinaryWork_NeverOccupiesMoreThanTheUnreservedCapacity()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 3, reserved: 1);

        AdmissionLease first = await Admit(orderer, TransactionPriority.Normal);
        AdmissionLease second = await Admit(orderer, TransactionPriority.Low);

        // Two of three slots are ordinary; the third is reserved and must stay out of reach.
        Task<AdmissionLease?> third = Park(orderer, TransactionPriority.Background);
        Assert.False(third.IsCompleted);

        AdmissionLease urgent = await Admit(orderer, TransactionPriority.Critical);
        Assert.Equal(3, orderer.InFlight);

        // Even with the node full, releasing an ordinary slot must not let ordinary work exceed its share.
        first.Dispose();

        (await third.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();

        second.Dispose();
        urgent.Dispose();

        Assert.Equal(0, orderer.InFlight);
    }

    [Fact]
    public async Task AgedWaiter_StillCannotConsumeTheReservedSlot()
    {
        ManualTimeProvider time = new();
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 2, reserved: 1, agingThresholdMs: 100, time: time);

        AdmissionLease ordinary = await Admit(orderer, TransactionPriority.Normal);

        Task<AdmissionLease?> bulk = Park(orderer, TransactionPriority.Background);
        Assert.False(bulk.IsCompleted);

        // Enough for the waiter to reach High as its effective ordering priority.
        time.Advance(1_000);

        // One slot is free, but it is the reserved one and ordinary work already holds its full share. Aging
        // moves a waiter's place in line, never its class.
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(bulk.IsCompleted);
        Assert.Equal(1, orderer.InFlight);

        AdmissionLease urgent = await Admit(orderer, TransactionPriority.High);
        Assert.Equal(2, orderer.InFlight);

        urgent.Dispose();
        ordinary.Dispose();

        (await bulk.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Fact]
    public async Task AQueueAtItsBound_RefusesFurtherCallersInsteadOfGrowing()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1, maxQueued: 2);

        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        Task<AdmissionLease?> firstWaiter = Park(orderer, TransactionPriority.Normal);
        Task<AdmissionLease?> secondWaiter = Park(orderer, TransactionPriority.Normal);

        Assert.Equal(2, orderer.Queued);

        // Beyond the bound the gate sheds load rather than accumulating it — the queue is what would consume
        // the memory the ceiling exists to protect.
        AdmissionLease? refused = await orderer.AdmitAsync(TransactionPriority.Normal, CancellationToken.None);

        Assert.Null(refused);
        Assert.Equal(2, orderer.Queued);
        Assert.Equal(1, orderer.RejectedQueueFullAt(TransactionPriority.Normal));

        // Refusal is distinct from a caller giving up, so an operator can tell shedding from timeouts.
        Assert.Equal(0, orderer.AbandonedWhileWaitingAt(TransactionPriority.Normal));

        occupant.Dispose();

        (await firstWaiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
        (await secondWaiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();

        // With room again, admission resumes.
        AdmissionLease after = await Admit(orderer, TransactionPriority.Normal);
        after.Dispose();
    }

    [Fact]
    public async Task AbandonedWaiter_IsReclaimedImmediatelyEvenBehindALongLivedOccupant()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1, maxQueued: 2);

        // Never released during the test, so no dispatch ever runs. A waiter that is only unlinked when a
        // dispatch reaches it would stay retained for the whole life of this occupant.
        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        using CancellationTokenSource givesUp = new();

        Task<AdmissionLease?> abandoned = Park(orderer, TransactionPriority.Normal, givesUp.Token);

        Assert.Equal(1, orderer.Queued);

        givesUp.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await abandoned);

        Assert.Equal(0, orderer.Queued);
        Assert.Equal(1, orderer.AbandonedWhileWaitingAt(TransactionPriority.Normal));

        // The vacated place is immediately usable, which is what proves the entry was actually unlinked
        // rather than merely marked.
        Task<AdmissionLease?> firstReplacement = Park(orderer, TransactionPriority.Normal);
        Task<AdmissionLease?> secondReplacement = Park(orderer, TransactionPriority.Normal);

        Assert.Equal(2, orderer.Queued);
        Assert.Equal(0, orderer.RejectedQueueFullAt(TransactionPriority.Normal));

        occupant.Dispose();

        (await firstReplacement.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
        (await secondReplacement.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Fact]
    public async Task AbandonedWaiter_NeverConsumesCapacityOwedToACallerStillWaiting()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1);

        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        using CancellationTokenSource givesUp = new();

        Task<AdmissionLease?> abandoned = Park(orderer, TransactionPriority.High, givesUp.Token);
        Task<AdmissionLease?> patient = Park(orderer, TransactionPriority.Low);

        Assert.Equal(2, orderer.Queued);

        givesUp.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await abandoned);

        occupant.Dispose();

        // The freed slot goes to the caller still waiting, not to the one that walked away.
        (await patient.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();

        Assert.Equal(0, orderer.InFlight);
        Assert.Equal(0, orderer.Queued);
    }

    [Fact]
    public async Task CancellationRacingDispatchAndDisposal_LeavesConsistentAccounting()
    {
        // Cancellation, dispatch, and teardown all claim waiters, and the registration that arms cancellation
        // is necessarily created outside the lock. This drives all three against each other repeatedly; the
        // invariant is that every waiter reaches exactly one outcome and no slot is lost or duplicated.
        for (int round = 0; round < 40; round++)
        {
            TransactionPriorityOrderer orderer = Build(maxConcurrent: 2);

            AdmissionLease first = await Admit(orderer, TransactionPriority.Normal);
            AdmissionLease second = await Admit(orderer, TransactionPriority.Normal);

            List<CancellationTokenSource> sources = [];
            List<Task<AdmissionLease?>> waiters = [];

            for (int i = 0; i < 8; i++)
            {
                CancellationTokenSource source = new();
                sources.Add(source);
                waiters.Add(Park(orderer, (TransactionPriority)(i % 5), source.Token));
            }

            // Cancel every waiter and free both slots concurrently, so cancellation lands in every possible
            // position relative to dispatch and to the registration being published.
            Task cancelling = Task.Run(() =>
            {
                foreach (CancellationTokenSource source in sources)
                    source.Cancel();
            }, TestContext.Current.CancellationToken);

            Task releasing = Task.Run(() =>
            {
                first.Dispose();
                second.Dispose();
            }, TestContext.Current.CancellationToken);

            await Task.WhenAll(cancelling, releasing);

            // Whoever won dispatch got a real lease and must give it back; the rest observed cancellation.
            foreach (Task<AdmissionLease?> waiter in waiters)
            {
                try
                {
                    (await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))?.Dispose();
                }
                catch (OperationCanceledException)
                {
                    // Expected for waiters cancellation claimed first.
                }
            }

            Assert.Equal(0, orderer.InFlight);
            Assert.Equal(0, orderer.Queued);

            orderer.Dispose();

            foreach (CancellationTokenSource source in sources)
                source.Dispose();
        }
    }

    [Fact]
    public async Task ReleasingALeaseTwice_DoesNotInflateCapacity()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 1);

        AdmissionLease lease = await Admit(orderer, TransactionPriority.Normal);

        lease.Dispose();
        lease.Dispose();

        Assert.Equal(0, orderer.InFlight);

        // A double release that decremented twice would let two transactions run against a ceiling of one.
        AdmissionLease next = await Admit(orderer, TransactionPriority.Normal);
        Task<AdmissionLease?> shouldWait = Park(orderer, TransactionPriority.Normal);

        Assert.False(shouldWait.IsCompleted);

        next.Dispose();

        (await shouldWait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Theory]
    [InlineData(99)]
    [InlineData(5)]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public async Task AnOutOfRangePriority_IsTreatedAsOrdinaryWorkAndNeverAsCritical(int raw)
    {
        // A raw number from a REST payload or a cast enum can carry any ordinal. Clamping it to the nearest
        // extreme would let untrusted input present itself as Critical, jump the queue, and claim reserved
        // capacity, so unknown values are normalized to Normal instead.
        TransactionPriority hostile = (TransactionPriority)raw;

        Assert.Equal(TransactionPriority.Normal, TransactionPriorityOrderer.Normalize(hostile));

        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 2, reserved: 1);

        AdmissionLease ordinary = await Admit(orderer, TransactionPriority.Normal);

        // Ordinary capacity is already spent. A genuine Critical could still start here; this must not.
        Task<AdmissionLease?> hostileAdmission = Park(orderer, hostile);

        Assert.False(hostileAdmission.IsCompleted);
        Assert.Equal(1, orderer.QueuedAt(TransactionPriority.Normal));
        Assert.Equal(0, orderer.QueuedAt(TransactionPriority.Critical));

        ordinary.Dispose();

        (await hostileAdmission.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Dispose();
    }

    [Fact]
    public async Task PassThroughMode_AdmitsEveryoneAndNeverQueues()
    {
        using TransactionPriorityOrderer orderer = Build(maxConcurrent: 0);

        Assert.True(orderer.IsPassThrough);

        List<AdmissionLease> leases = [];

        for (int i = 0; i < 64; i++)
        {
            Task<AdmissionLease?> admission = Park(orderer, TransactionPriority.Background);

            Assert.True(admission.IsCompletedSuccessfully);

            leases.Add((await admission)!);
        }

        Assert.Equal(0, orderer.Queued);

        // Priority is still recorded so an operator can size a ceiling before enabling one.
        Assert.Equal(64, orderer.AdmittedAt(TransactionPriority.Background));

        foreach (AdmissionLease lease in leases)
            lease.Dispose();
    }

    [Fact]
    public async Task Disposing_FailsCallersStillWaitingForASlot()
    {
        TransactionPriorityOrderer orderer = Build(maxConcurrent: 1);

        AdmissionLease occupant = await Admit(orderer, TransactionPriority.Normal);

        Task<AdmissionLease?> parked = Park(orderer, TransactionPriority.Normal);

        orderer.Dispose();

        // Better a cancelled caller than one awaiting a slot a torn-down node will never grant.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await parked);

        occupant.Dispose();
    }
}
