
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Tests;

/// <summary>
/// Hermetic guards for three client changes that removed per-operation work:
///   • the lock and script entry points hand back the transport task instead of wrapping it in an
///     <c>async</c> method, so the exception a caller sees must still arrive through that task rather
///     than at the call itself;
///   • a gRPC channel cache key is no longer rebuilt while the options behind it are unchanged, and it
///     must still be rebuilt the moment any of them changes — including a pin list edited in place,
///     because serving a stale key there would mean reusing a channel under a retired trust policy;
///   • the batch list now belongs to the dispatch loop, so <c>RunBatch</c> must leave the caller's list
///     exactly as it found it.
/// None of these needs a server. No call in this file reaches a socket.
/// </summary>
public sealed class TestClientForwardingAndChannelKeys
{
    // A client whose transport is never reached: every case below fails while preparing the call.
    private static KahunaClient UnreachableClient() => new("https://127.0.0.1:1");

    // ── Forwarding methods must still report a synchronous failure through the returned task ────

    [Fact]
    public void Unlock_WithNullOwner_FaultsTheTask_RatherThanThrowingAtTheCall()
    {
        KahunaClient client = UnreachableClient();

        // Encoding the owner runs before the transport is called, so this is the exact spot where a
        // direct return would have moved the throw to the caller.
        Task<bool> task = client.Unlock("resource", (string)null!, LockDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.True(task.IsFaulted);
        Assert.IsType<ArgumentNullException>(task.Exception?.InnerException);
    }

    [Fact]
    public void TryExtendLock_WithNullOwner_FaultsTheTask_RatherThanThrowingAtTheCall()
    {
        KahunaClient client = UnreachableClient();

        Task<(bool, long)> task = client.TryExtendLock("resource", (string)null!, 1000, LockDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.True(task.IsFaulted);
        Assert.IsType<ArgumentNullException>(task.Exception?.InnerException);
    }

    [Fact]
    public void TryExtendLock_TimeSpanOverload_WithNullOwner_FaultsTheTask()
    {
        KahunaClient client = UnreachableClient();

        Task<(bool, long)> task = client.TryExtendLock("resource", (string)null!, TimeSpan.FromSeconds(1), LockDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.True(task.IsFaulted);
        Assert.IsType<ArgumentNullException>(task.Exception?.InnerException);
    }

    [Fact]
    public void ExecuteKeyValueTransactionScript_WithNullScript_FaultsTheTask()
    {
        KahunaClient client = UnreachableClient();

        Task<KahunaKeyValueTransactionResult> task = client.ExecuteKeyValueTransactionScript((string)null!, null, null, TestContext.Current.CancellationToken);

        Assert.True(task.IsFaulted);
        Assert.IsType<ArgumentNullException>(task.Exception?.InnerException);
    }

    [Fact]
    public async Task FaultedForwardingTask_StillThrowsTheOriginalExceptionOnAwait()
    {
        KahunaClient client = UnreachableClient();

        await Assert.ThrowsAsync<ArgumentNullException>(() => client.Unlock("resource", (string)null!, LockDurability.Persistent, TestContext.Current.CancellationToken));
    }

    // ── Channel cache keys: unchanged results, and rebuilt whenever an input changes ────────────

    [Fact]
    public void MakeCacheKey_ReturnsTheUrlItself_ForDefaultOptions()
    {
        // The default path must stay allocation-free, which means handing back the very same string.
        string url = "https://host:1234";

        Assert.Same(url, GrpcBatcher.MakeCacheKey(url, null));
        Assert.Same(url, GrpcBatcher.MakeCacheKey(url, new KahunaOptions()));
    }

    [Fact]
    public void MakeCacheKey_KeepsPinOrderAndCaseEquivalent()
    {
        KahunaOptions lower = new() { TrustedServerCertificateThumbprints = ["bbbb", "aaaa"] };
        KahunaOptions upper = new() { TrustedServerCertificateThumbprints = ["AAAA", "BBBB"] };

        string key = GrpcBatcher.MakeCacheKey("https://host:1234", lower);

        Assert.Equal(key, GrpcBatcher.MakeCacheKey("https://host:1234", upper));
        Assert.StartsWith("https://host:1234\0pin:", key);
    }

    [Fact]
    public void MakeCacheKey_SeparatesDistinctTrustPolicies()
    {
        const string url = "https://host:1234";

        HashSet<string> keys =
        [
            GrpcBatcher.MakeCacheKey(url, null),
            GrpcBatcher.MakeCacheKey(url, new KahunaOptions { AllowInsecureCertificateValidation = true }),
            GrpcBatcher.MakeCacheKey(url, new KahunaOptions { TrustedServerCertificateThumbprints = ["AABB"] }),
            GrpcBatcher.MakeCacheKey(url, new KahunaOptions { GrpcChannelPoolSize = 4 })
        ];

        Assert.Equal(4, keys.Count);
    }

    [Fact]
    public void MakeCacheKey_IsStableWhileTheOptionsAreUnchanged()
    {
        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = ["AAAA", "BBBB"] };

        string first = GrpcBatcher.MakeCacheKey("https://host:1234", opts);
        string second = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        Assert.Equal(first, second);
    }

    [Fact]
    public void MakeCacheKey_VariesByUrl_ForOneOptionsInstance()
    {
        // One client rotates over several endpoints, so the cached part must not pin the URL.
        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = ["AAAA"] };

        string first = GrpcBatcher.MakeCacheKey("https://host-1:1234", opts);
        string second = GrpcBatcher.MakeCacheKey("https://host-2:1234", opts);

        Assert.NotEqual(first, second);
        Assert.StartsWith("https://host-1:1234\0pin:", first);
        Assert.StartsWith("https://host-2:1234\0pin:", second);

        // Only the URL differs; the suffix the two share must be identical.
        Assert.Equal(first["https://host-1:1234".Length..], second["https://host-2:1234".Length..]);
    }

    [Fact]
    public void MakeCacheKey_RebuildsWhenThePoolSizeChanges()
    {
        KahunaOptions opts = new();

        string beforeChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        opts.GrpcChannelPoolSize = 4;
        string afterChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        Assert.NotEqual(beforeChange, afterChange);
        Assert.Contains("pool:4", afterChange);
    }

    [Fact]
    public void MakeCacheKey_RebuildsWhenTheInsecureFlagChanges()
    {
        KahunaOptions opts = new();

        string beforeChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        opts.AllowInsecureCertificateValidation = true;
        string afterChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        Assert.NotEqual(beforeChange, afterChange);
        Assert.Contains("insecure", afterChange);

        // And back again: a cache that only ever grew stale in one direction would still be wrong.
        opts.AllowInsecureCertificateValidation = false;
        Assert.Equal(beforeChange, GrpcBatcher.MakeCacheKey("https://host:1234", opts));
    }

    [Fact]
    public void MakeCacheKey_RebuildsWhenThePinListIsEditedInPlace()
    {
        // The property is IReadOnlyList, but the caller can hand over a List it still holds. Keying the
        // cache on the list reference would keep serving a channel under a retired trust policy, so
        // this is a security case, not a staleness case.
        List<string> pins = ["AAAA"];
        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = pins };

        string beforeChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        pins.Add("BBBB");
        string afterAdd = GrpcBatcher.MakeCacheKey("https://host:1234", opts);
        Assert.NotEqual(beforeChange, afterAdd);

        pins.RemoveAt(1);
        Assert.Equal(beforeChange, GrpcBatcher.MakeCacheKey("https://host:1234", opts));
    }

    [Fact]
    public void MakeCacheKey_RebuildsWhenAPinIsReplacedWithoutChangingTheCount()
    {
        // A count-only check would miss this: same number of pins, different trust policy.
        List<string> pins = ["AAAA", "BBBB"];
        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = pins };

        string beforeChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        pins[1] = "CCCC";
        string afterChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        Assert.NotEqual(beforeChange, afterChange);
        Assert.Contains("CCCC", afterChange);
        Assert.DoesNotContain("BBBB", afterChange);
    }

    [Fact]
    public void MakeCacheKey_RebuildsWhenTheWholePinListIsReplaced()
    {
        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = ["AAAA"] };

        string beforeChange = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        opts.TrustedServerCertificateThumbprints = ["DDDD"];
        Assert.NotEqual(beforeChange, GrpcBatcher.MakeCacheKey("https://host:1234", opts));

        // Replacing the list with equal contents is not a policy change, so the key must not move.
        opts.TrustedServerCertificateThumbprints = ["AAAA"];
        Assert.Equal(beforeChange, GrpcBatcher.MakeCacheKey("https://host:1234", opts));
    }

    [Fact]
    public void MakeCacheKey_ClampsAPoolSizeBelowOne()
    {
        KahunaOptions clamped = new() { GrpcChannelPoolSize = 0 };
        KahunaOptions one = new() { GrpcChannelPoolSize = 1 };

        Assert.Equal(
            GrpcBatcher.MakeCacheKey("https://host:1234", one),
            GrpcBatcher.MakeCacheKey("https://host:1234", clamped));
    }

    // ── The batch list belongs to the dispatch loop, not to RunBatch ───────────────────────────

    [Fact]
    public async Task RunBatch_LeavesTheCallersListUntouched()
    {
        // Every promise is already complete, so RunBatch takes its early exit and never opens a
        // stream. What matters here is only what it does to the list it was handed.
        GrpcBatcher batcher = new("https://127.0.0.1:1");

        List<GrpcBatcherItem> requests =
        [
            CompletedItem(1),
            CompletedItem(2)
        ];

        await batcher.RunBatch(requests);

        // The dispatch loop reuses this list for the next drain, so RunBatch must not clear it, and it
        // must not hand it to a pool the loop knows nothing about.
        Assert.Equal(2, requests.Count);
        Assert.Equal(1, requests[0].RequestId);
        Assert.Equal(2, requests[1].RequestId);
    }

    [Fact]
    public async Task DispatchLoop_KeepsWorkingAcrossConsecutiveBatches()
    {
        // Nothing is listening on this port, so every batch fails at the transport. That still drives
        // the whole dispatch loop: rent the buffer, drain the inbox, run the batch, fail the requests,
        // then clear and reuse the buffer on the next round. A buffer whose lifetime was wrong would
        // show up here as a request that never completes.
        const string url = "https://127.0.0.1:9";

        try
        {
            GrpcBatcher batcher = new(url, TimeSpan.FromSeconds(10));

            for (int round = 0; round < 3; round++)
            {
                Task<GrpcBatcherResponse> first = batcher.Enqueue(new GrpcTryLockRequest());
                Task<GrpcBatcherResponse> second = batcher.Enqueue(new GrpcTryLockRequest());

                await Assert.ThrowsAnyAsync<Exception>(() => first);
                await Assert.ThrowsAnyAsync<Exception>(() => second);
            }
        }
        finally
        {
            // The streaming pool is process-wide, so this entry must not outlive the test.
            GrpcBatcher.RemoveTestSharedStreaming(url);
        }
    }

    private static GrpcBatcherItem CompletedItem(int requestId)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new();
        promise.SetResult(new GrpcBatcherResponse(new GrpcTryLockResponse { Type = GrpcLockResponseType.LockResponseTypeLocked }));

        return new GrpcBatcherItem(
            GrpcBatcherItemType.Locks,
            requestId,
            new GrpcBatcherRequest(new GrpcTryLockRequest()),
            promise);
    }
}
