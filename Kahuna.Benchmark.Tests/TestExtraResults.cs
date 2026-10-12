/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Benchmark;

namespace Kahuna.Benchmark.Tests;

/// <summary>
/// The side queue a multi-round-trip workload fills beside the result it returns: every queued
/// measurement reaches the worker's stats under its own operation type and outcome, and the queue is
/// empty afterwards so the next call starts clean.
/// </summary>
public sealed class TestExtraResults
{
    private const long Ceiling = 15_000_000L;

    [Fact]
    public void DrainInto_RecordsEveryQueuedMeasurement_ThenEmptiesTheQueue()
    {
        WorkerStats stats = new(Ceiling);
        ExtraResults extra = new();

        extra.Add(OperationType.TxnStart, OpOutcome.Success, 400);
        extra.Add(OperationType.TxnGetNext, OpOutcome.Success, 250);
        extra.Add(OperationType.TxnGetNext, OpOutcome.Miss, 300);
        extra.Add(OperationType.TxnRollback, OpOutcome.Error, 900);
        Assert.Equal(4, extra.Count);

        extra.DrainInto(stats);

        Assert.Equal(0, extra.Count);
        Assert.Equal(1, stats.GetSuccess(OperationType.TxnStart));
        Assert.Equal(400, stats.GetMeanMicros(OperationType.TxnStart));
        Assert.Equal(1, stats.GetSuccess(OperationType.TxnGetNext));
        Assert.Equal(1, stats.GetMisses(OperationType.TxnGetNext));
        Assert.Equal(250, stats.GetMeanMicros(OperationType.TxnGetNext));
        Assert.Equal(1, stats.GetErrors(OperationType.TxnRollback));
        Assert.Equal(0, stats.GetSuccess(OperationType.TxnGetFirst));

        // The primary result of the call is recorded by the worker, not by the queue.
        Assert.DoesNotContain(OperationType.TxnGetFirst, stats.ActiveOps);
    }

    [Fact]
    public void DrainInto_OnAnEmptyQueue_ChangesNothing()
    {
        WorkerStats stats = new(Ceiling);
        ExtraResults extra = new();

        extra.DrainInto(stats);

        Assert.Empty(stats.ActiveOps);
    }
}
