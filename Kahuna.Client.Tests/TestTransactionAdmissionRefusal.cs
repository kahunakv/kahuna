using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;

namespace Kahuna.Client.Tests;

/// <summary>
/// The admission refusal as a remote client sees it. This is the case the distinct response code exists
/// for: the refusal is produced on whichever node owns the coordinator partition, so it crosses a real
/// forwarding hop and a real transport before the client reads it. A mapper that rounded it back to
/// <c>MustRetry</c> anywhere along that path would reinstate the ambiguity the code was added to remove —
/// and the client would spin against a saturated node instead of backing off.
///
/// <para><b>Requires a cluster with the session gate enabled</b>, which is off by default. Bring one up with
/// <c>KAHUNA_EXTRA_ARGS="--max-concurrent-sessions 1" docker compose -f docker/local.yml up -d</c>. Against
/// a stock cluster there is no ceiling to saturate, so these tests skip rather than fail — a false green is
/// worse than an honest skip, and silently passing here would suggest coverage that does not exist.</para>
///
/// <para><b>The ceiling is per node, and a session lands on whichever node leads its coordinator
/// partition.</b> Opening one session therefore saturates one node, not the cluster, and a second session
/// is refused only if it happens to hash to the same leader. These tests open sessions until every node is
/// full rather than assuming the first collision, which is what makes them deterministic instead of a coin
/// flip on the coordinator-key hash.</para>
/// </summary>
public class TestTransactionAdmissionRefusal
{
    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    /// <summary>Enough attempts to fill every node's queue at any sane ceiling, and still bounded.</summary>
    private const int MaxSessionsToSaturate = 24;

    [Theory, CombinatorialData]
    public async Task TestSaturatedNodeRefusesAdmissionDistinctly(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType
    )
    {
        KahunaClient client = GetClient(communicationType);

        List<KahunaTransactionSession> held = [];

        try
        {
            (KahunaException? refusal, _) = await SaturateUntilRefused(client, held, admissionWaitMs: 1_000);

            if (refusal is null)
                Assert.Skip($"no admission refusal after {MaxSessionsToSaturate} sessions; the cluster has no session ceiling configured");

            // Named for what it is, and specifically neither the transient code that invites an immediate
            // retry nor the abort code that would claim the transaction ran and conflicted.
            Assert.Equal(KeyValueResponseType.AdmissionRefused, refusal!.KeyValueErrorCode);
            Assert.NotEqual(KeyValueResponseType.MustRetry, refusal.KeyValueErrorCode);
            Assert.NotEqual(KeyValueResponseType.Aborted, refusal.KeyValueErrorCode);
        }
        finally
        {
            await ReleaseAll(held);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestAdmissionWaitIsHonouredRatherThanTheSessionTimeout(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType
    )
    {
        KahunaClient client = GetClient(communicationType);

        List<KahunaTransactionSession> held = [];

        try
        {
            // An hour-long session that will wait one second at the door. If the two clocks were still one —
            // or if the budget were dropped on the forwarding hop and the leader fell back to its own
            // default — this would take far longer than the bound asserted below.
            (KahunaException? refusal, TimeSpan waited) = await SaturateUntilRefused(
                client, held, admissionWaitMs: 1_000, timeout: 3_600_000);

            if (refusal is null)
                Assert.Skip($"no admission refusal after {MaxSessionsToSaturate} sessions; the cluster has no session ceiling configured");

            Assert.Equal(KeyValueResponseType.AdmissionRefused, refusal!.KeyValueErrorCode);

            // Generous upper bound: the point is that it returned on the admission clock, orders of
            // magnitude below the session clock it would otherwise have waited on.
            Assert.True(
                waited.TotalSeconds < 20,
                $"expected the refusal on the admission clock, waited {waited.TotalSeconds:F1}s");
        }
        finally
        {
            await ReleaseAll(held);
        }
    }

    /// <summary>
    /// Opens sessions until one is refused, returning the refusal and how long that final attempt took.
    /// Successfully opened sessions are added to <paramref name="held"/> so the caller can release them.
    /// Returns a null refusal when the cluster admitted every attempt, which means no ceiling is set.
    /// </summary>
    private static async Task<(KahunaException?, TimeSpan)> SaturateUntilRefused(
        KahunaClient client, List<KahunaTransactionSession> held, int admissionWaitMs, int timeout = 30_000)
    {
        for (int attempt = 0; attempt < MaxSessionsToSaturate; attempt++)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            try
            {
                // AutoCommit off so releasing these is an explicit rollback and never a surprise write.
                KahunaTransactionSession session = await client.StartTransactionSession(
                    new KahunaTransactionOptions { Timeout = timeout, AdmissionWaitMs = admissionWaitMs, AutoCommit = false },
                    TestContext.Current.CancellationToken
                );

                held.Add(session);
            }
            catch (KahunaException ex)
            {
                stopwatch.Stop();

                return (ex, stopwatch.Elapsed);
            }
        }

        return (null, TimeSpan.Zero);
    }

    private static async Task ReleaseAll(List<KahunaTransactionSession> held)
    {
        foreach (KahunaTransactionSession session in held)
        {
            try
            {
                await session.Rollback(TestContext.Current.CancellationToken);
            }
            catch (KahunaException)
            {
                // Releasing is best-effort cleanup; a session the server already reclaimed is not a failure
                // of the behaviour under test.
            }

            await session.DisposeAsync();
        }
    }

    private KahunaClient GetClient(KahunaCommunicationType communicationType)
    {
        IKahunaCommunication communication = communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(new() { AllowInsecureCertificateValidation = true }, null),
            KahunaCommunicationType.Rest => new RestCommunication(null, new() { AllowInsecureCertificateValidation = true }),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType))
        };

        return new KahunaClient(urls, communication: communication);
    }
}
