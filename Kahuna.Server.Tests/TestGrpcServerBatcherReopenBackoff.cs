using Kommander;
using System.Collections.Concurrent;
using System.Reflection;

using Grpc.Core;
using Kahuna.Server.Communication.Internode.Grpc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The inter-node <see cref="GrpcServerBatcher"/> against a peer that refuses its streams. Every drain
/// used to rebuild the peer's streams and fail at once — 70–100 opens and 200–250 Warning lines a
/// second on a leader for the whole outage. The per-peer <see cref="PeerStreamEpisode"/> now spaces
/// the re-opens with a doubling backoff, refuses the drains that fall inside it without an open, and
/// rate-limits the log to one line per episode, one summary per interval and one line on recovery.
/// </summary>
public sealed class TestGrpcServerBatcherReopenBackoff
{
    private static readonly Type BatcherType = typeof(GrpcServerBatcher);

    private static readonly RpcException ConnectRefused = new(new(StatusCode.Unavailable, "Error connecting to subchannel."));

    private static ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>> Streamings()
        => (ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>>)BatcherType
            .GetField("streamings", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static ConcurrentDictionary<string, PeerStreamEpisode> Episodes()
        => (ConcurrentDictionary<string, PeerStreamEpisode>)BatcherType
            .GetField("episodes", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static ConcurrentDictionary<int, GrpcServerBatcherItem> RequestRefs()
        => (ConcurrentDictionary<int, GrpcServerBatcherItem>)BatcherType
            .GetField("requestRefs", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static ConcurrentDictionary<int, long> RequestStreamRefs()
        => (ConcurrentDictionary<int, long>)BatcherType
            .GetField("requestStreamRefs", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    /// <summary>A clock the test drives, so the delays and the summary interval need no waiting.</summary>
    private sealed class FakeClock
    {
        private long nowMs;

        public long Read() => Volatile.Read(ref nowMs);

        public void Advance(long ms) => Interlocked.Add(ref nowMs, ms);
    }

    // ── episode: log shape ───────────────────────────────────────────────────

    /// <summary>
    /// One Warning opens the episode and carries the status; every failure inside the interval is a
    /// Debug line; the first failure past the interval is one Warning summary with the counts; the
    /// write that recovers is one Information line with the totals.
    /// </summary>
    [Fact]
    public void Episode_OneWarningOpensIt_FailuresInsideTheIntervalLogAtDebug_ThenOneSummary_ThenOneRecoveryLine()
    {
        const string peer = "test://episode-log-shape";

        FakeClock clock = new();
        RecordingLogProvider recorder = new();
        PeerStreamEpisode episode = new(recorder.CreateLogger("test"), peer, clock.Read);

        episode.NoteEviction("streams torn down: a write failed or timed out", ConnectRefused);

        // Fifty failed re-opens over the next five seconds, each with the signals one attempt produces.
        for (int attempt = 0; attempt < 50; attempt++)
        {
            clock.Advance(100);
            episode.NoteOpened();
            episode.NoteFailure("key-value write failed", ConnectRefused);
            episode.NoteEviction("streams torn down: a write failed or timed out", ConnectRefused);
            episode.NoteFailure("key-value stream closed", ConnectRefused);
            episode.NoteFailure("lock stream closed", ConnectRefused);
        }

        IReadOnlyList<(LogLevel Level, string Message)> lines = recorder.Containing("streams to " + peer);

        Assert.Equal(LogLevel.Warning, lines[0].Level);
        Assert.Contains("Error connecting to subchannel.", lines[0].Message, StringComparison.Ordinal);
        Assert.Single(lines, line => line.Level == LogLevel.Warning);
        Assert.Equal(200, lines.Count(line => line.Level == LogLevel.Debug));

        // The interval has passed: the next failure is the summary, with the counts since the opening line.
        clock.Advance(5_000);
        episode.NoteFailure("key-value stream closed", ConnectRefused);

        (LogLevel Level, string Message) summary = Assert.Single(recorder.Containing("still failing"));
        Assert.Equal(LogLevel.Warning, summary.Level);
        Assert.Contains("50 re-opens, 50 torn down", summary.Message, StringComparison.Ordinal);
        Assert.Contains("(50, 51 and 0 since the episode opened 10 s ago)", summary.Message, StringComparison.Ordinal);

        episode.NoteSuccess();

        Assert.False(episode.IsOpen);
        (LogLevel Level, string Message) recovered = Assert.Single(recorder.Containing("recovered"));
        Assert.Equal(LogLevel.Information, recovered.Level);
        Assert.Contains("after 50 re-opens, 51 torn down", recovered.Message, StringComparison.Ordinal);
        Assert.Equal(2, recorder.Containing("streams to " + peer).Count(line => line.Level == LogLevel.Warning));
    }

    // ── episode: backoff ─────────────────────────────────────────────────────

    /// <summary>
    /// The first eviction delays the next open by the initial delay; every further eviction doubles it
    /// up to the cap. An allowed open is claimed, so a second drain before its outcome is refused. The
    /// first success closes the episode, and the next one starts over at the initial delay.
    /// </summary>
    [Fact]
    public void Episode_EachEvictionDoublesTheDelayToTheCap_AnAllowedOpenIsClaimedOnce_SuccessResetsIt()
    {
        FakeClock clock = new();
        PeerStreamEpisode episode = new(NullLogger.Instance, "test://episode-backoff", clock.Read);

        Assert.True(episode.TryAllowOpen());
        Assert.Equal(0, episode.BackoffMs);

        long[] delays = [100, 200, 400, 800, 1_600, 2_000, 2_000];

        foreach (long delay in delays)
        {
            episode.NoteEviction("streams torn down: a write failed or timed out", ConnectRefused);

            Assert.False(episode.TryAllowOpen());
            clock.Advance(delay - 1);
            Assert.False(episode.TryAllowOpen());
            clock.Advance(1);
            Assert.True(episode.TryAllowOpen());
            Assert.False(episode.TryAllowOpen());

            episode.NoteOpened();
        }

        Assert.Equal(delays.Length, episode.EvictionsInEpisode);
        Assert.Equal(delays.Length, episode.OpensInEpisode);

        episode.NoteSuccess();

        Assert.False(episode.IsOpen);
        Assert.True(episode.TryAllowOpen());
        Assert.True(episode.TryAllowOpen());

        episode.NoteEviction("streams torn down: a write failed or timed out", ConnectRefused);

        Assert.False(episode.TryAllowOpen());
        clock.Advance(100);
        Assert.True(episode.TryAllowOpen());
    }

    /// <summary>
    /// A failure signal on its own — the late close of a stream whose set was already torn down and
    /// replaced — neither opens an episode nor delays an open: only an eviction does either, once per
    /// torn-down set. Otherwise a disposed stream's read loop could start an episode after the recovery.
    /// </summary>
    [Fact]
    public void Episode_FailureWithoutEviction_NeitherOpensTheEpisodeNorDelaysAnOpen()
    {
        FakeClock clock = new();
        RecordingLogProvider recorder = new();
        PeerStreamEpisode episode = new(recorder.CreateLogger("test"), "test://episode-no-eviction", clock.Read);

        episode.NoteFailure("key-value stream closed", ConnectRefused);

        Assert.False(episode.IsOpen);
        Assert.Equal(0, episode.EvictionsInEpisode);
        Assert.True(episode.TryAllowOpen());
        Assert.True(episode.TryAllowOpen());

        (LogLevel Level, string Message) line = Assert.Single(recorder.Containing("streams to test://episode-no-eviction"));
        Assert.Equal(LogLevel.Debug, line.Level);
    }

    // ── batcher: the dispatch loop under the backoff ─────────────────────────

    /// <summary>
    /// After a failed write tears the streams down, the next drain finds no registry entry and the
    /// delay holding: it must refuse its request with the retryable status without touching the
    /// registry. Once the peer answers a write again, the episode closes with one Information line.
    /// </summary>
    [Fact]
    public async Task DispatchLoop_RefusesTheNextDrainWithoutAnOpenWhileTheDelayHolds_AndTheFirstSuccessfulWriteRecovers()
    {
        const string url = "test://reopen-backoff-refusal";
        const long streamIdBase = 9_810_000;

        FakeClock clock = new();
        RecordingLogProvider recorder = new();
        ILogger logger = recorder.CreateLogger("test");
        PeerStreamEpisode episode = new(logger, url, clock.Read);
        Episodes()[url] = episode;

        GrpcServerBatcher batcher = new(url, new RaftTransportSecurityOptions(), logger);
        Task<GrpcServerBatcherResponse>? recovered = null;

        try
        {
            // A dead peer: the write fails, the stream set is torn down and the episode opens.
            (GrpcServerSharedStreaming failing, _) = MakeSharedStreaming(streamIdBase, new ThrowingClientStreamWriter<GrpcBatchServerKeyValueRequest>());
            Streamings()[url] = CreatedLazy(failing);

            RpcException first = await Assert.ThrowsAsync<RpcException>(() => batcher.Enqueue(new GrpcLookupTransactionRecordRequest()));
            Assert.Equal(StatusCode.Unavailable, first.StatusCode);
            Assert.True(episode.IsOpen);
            Assert.Equal(1, episode.EvictionsInEpisode);
            Assert.False(Streamings().ContainsKey(url));

            // The delay holds: the next drain is refused before any open, with the retryable status.
            RpcException refused = await Assert.ThrowsAsync<RpcException>(() => batcher.Enqueue(new GrpcLookupTransactionRecordRequest()));
            Assert.Equal(StatusCode.Unavailable, refused.StatusCode);
            Assert.Contains("backed off", refused.Status.Detail, StringComparison.Ordinal);
            Assert.False(Streamings().ContainsKey(url));
            Assert.Equal(1, episode.RefusedInEpisode);
            Assert.Equal(0, episode.OpensInEpisode);

            // The peer is back: a write that reaches it closes the episode with one Information line.
            clock.Advance(100);
            RecordingClientStreamWriter<GrpcBatchServerKeyValueRequest> writer = new();
            (GrpcServerSharedStreaming healthy, _) = MakeSharedStreaming(streamIdBase + 1, writer);
            Streamings()[url] = CreatedLazy(healthy);

            recovered = batcher.Enqueue(new GrpcLookupTransactionRecordRequest());
            await WaitUntil(() => writer.Written.Count == 1);
            await WaitUntil(() => !episode.IsOpen);

            (LogLevel Level, string Message) line = Assert.Single(recorder.Containing("recovered"));
            Assert.Equal(LogLevel.Information, line.Level);
            Assert.Contains("1 requests refused", line.Message, StringComparison.Ordinal);
            Assert.Single(recorder.Containing("streams to " + url), l => l.Level == LogLevel.Warning);
        }
        finally
        {
            if (recovered is not null)
                SettlePending(recovered);

            Streamings().TryRemove(url, out _);
            Episodes().TryRemove(url, out _);
        }
    }

    /// <summary>
    /// Five re-opens that fail at once — what one second of the storm looked like — must produce one
    /// Warning line in total, not four per attempt, and nothing above Warning.
    /// </summary>
    [Fact]
    public async Task DispatchLoop_RepeatedFailedOpensLogOneWarning_NotFourPerAttempt()
    {
        const string url = "test://reopen-backoff-log";
        const long streamIdBase = 9_810_100;

        FakeClock clock = new();
        RecordingLogProvider recorder = new();
        ILogger logger = recorder.CreateLogger("test");
        PeerStreamEpisode episode = new(logger, url, clock.Read);
        Episodes()[url] = episode;

        GrpcServerBatcher batcher = new(url, new RaftTransportSecurityOptions(), logger);

        try
        {
            for (int attempt = 0; attempt < 5; attempt++)
            {
                // Each round stands for one re-open the backoff let through: a fresh set that fails at once.
                (GrpcServerSharedStreaming streaming, _) = MakeSharedStreaming(streamIdBase + attempt, new ThrowingClientStreamWriter<GrpcBatchServerKeyValueRequest>());
                Streamings()[url] = CreatedLazy(streaming);

                RpcException failure = await Assert.ThrowsAsync<RpcException>(() => batcher.Enqueue(new GrpcLookupTransactionRecordRequest()));
                Assert.Equal(StatusCode.Unavailable, failure.StatusCode);
                clock.Advance(50);
            }

            Assert.Equal(5, episode.EvictionsInEpisode);

            IReadOnlyList<(LogLevel Level, string Message)> lines = recorder.Containing("GrpcServerBatcher");
            Assert.Single(lines, line => line.Level == LogLevel.Warning);
            Assert.DoesNotContain(lines, line => line.Level > LogLevel.Warning);
            Assert.Contains(lines, line => line.Level == LogLevel.Debug);
        }
        finally
        {
            Streamings().TryRemove(url, out _);
            Episodes().TryRemove(url, out _);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 5_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;

        while (!condition())
        {
            Assert.True(Environment.TickCount64 < deadline, "The condition was not reached within the timeout.");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>Takes a request that no response will ever answer out of the tracking dictionaries.</summary>
    private static void SettlePending(Task<GrpcServerBatcherResponse> pending)
    {
        foreach (KeyValuePair<int, GrpcServerBatcherItem> entry in RequestRefs())
        {
            if (entry.Value.Promise.Task != pending)
                continue;

            GrpcServerBatcher.TryTakeRequest(entry.Key, out _);
            RequestStreamRefs().TryRemove(entry.Key, out _);
            entry.Value.Promise.TrySetCanceled();
        }
    }

    private static (GrpcServerSharedStreaming, DisposeCounter) MakeSharedStreaming(
        long streamId, IClientStreamWriter<GrpcBatchServerKeyValueRequest> keyValueWriter)
    {
        DisposeCounter disposes = new();

        AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> lockCall = new(
            new NoopClientStreamWriter<GrpcBatchServerLockRequest>(),
            new PendingAsyncStreamReader<GrpcBatchServerLockResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            disposes.Increment);

        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> keyValueCall = new(
            keyValueWriter,
            new PendingAsyncStreamReader<GrpcBatchServerKeyValueResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            disposes.Increment);

        return (new GrpcServerSharedStreaming(streamId, lockCall, keyValueCall), disposes);
    }

    private static Lazy<List<GrpcServerSharedStreaming>> CreatedLazy(GrpcServerSharedStreaming streaming)
    {
        Lazy<List<GrpcServerSharedStreaming>> lazy = new(() => [streaming]);
        _ = lazy.Value;    // force creation so eviction reaches the dispose path
        return lazy;
    }

    private sealed class DisposeCounter
    {
        private int count;
        public int Count => Volatile.Read(ref count);
        public void Increment() => Interlocked.Increment(ref count);
    }

    /// <summary>Never yields an item and never closes — a stream that has simply gone quiet.</summary>
    private sealed class PendingAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly TaskCompletionSource<bool> never = new();
        public T Current => default!;
        public Task<bool> MoveNext(CancellationToken cancellationToken) => never.Task;
    }

    private sealed class NoopClientStreamWriter<T> : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;
        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    /// <summary>Accepts every write immediately and records it for assertions.</summary>
    private sealed class RecordingClientStreamWriter<T> : IClientStreamWriter<T>
    {
        private readonly ConcurrentQueue<T> written = new();
        public IReadOnlyList<T> Written => written.ToArray();
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;

        public Task WriteAsync(T message)
        {
            written.Enqueue(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>Fails every write retryably — a stream whose transport is gone.</summary>
    private sealed class ThrowingClientStreamWriter<T> : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;

        public Task WriteAsync(T message)
            => Task.FromException(new RpcException(new(StatusCode.Unavailable, "Error connecting to subchannel.")));
    }
}
