
using System.Text.Json;
using Kahuna.Shared.Communication.Rest;
using Microsoft.AspNetCore.Mvc;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Provides REST endpoints over the key-range map: which contiguous ranges exist, which partition
/// serves each, how the answering node routes each key space, and the registration that puts a key
/// space under key-range routing in the first place.
/// </summary>
public static class RangesHandlers
{
    public static void MapRangesRoutes(WebApplication app)
    {
        // The range map as this node has applied it. Deliberately ungated by leadership: the
        // descriptors are replicated, so any node can answer, and a follower answering with an older
        // generation than the leader is information — it is how a caller sees replication lag rather
        // than being redirected away from it.
        //
        // No coverage verdict is served alongside the descriptors on purpose. A caller checking that
        // ranges tile their key space with no gap and no overlap must compute that from the bounds
        // it received; a server-computed "valid" flag would only restate the server's own view, and
        // an external checker that trusts it is no longer checking anything.
        app.MapGet("/v1/ranges", (IKahuna keyValues, string? keySpace) => Results.Text(
            JsonSerializer.Serialize(
                keyValues.GetRangeMap(string.IsNullOrEmpty(keySpace) ? null : keySpace),
                KahunaJsonContext.Default.KahunaRangeMapResponse),
            "application/json"));

        // Puts a key space under key-range routing. Two halves, only one of which is replicated:
        //
        //   * the routing-mode flip is node-local in-memory state, so this call must be sent to
        //     EVERY node. Sending it to one node yields a cluster where that node routes the space
        //     by key range while the rest still hash it — which this endpoint cannot fix on the
        //     caller's behalf, only report (the response's routingMode is this node's view).
        //   * the whole-space seed descriptor is a single replicated meta-partition write. The
        //     manager forwards it to that partition's leader, so this is NOT leader-only: a
        //     follower answering here is expected to succeed.
        app.MapPost("/v1/ranges/register", async (
            [FromBody] KahunaKeyRangeRequest request, IKahuna keyValues, HttpContext httpContext) =>
        {
            KahunaRegisterKeyRangeResponse response = await keyValues
                .RegisterKeyRangeWithOutcomeAsync(request.KeySpace, httpContext.RequestAborted)
                .ConfigureAwait(false);

            return Results.Text(
                JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaRegisterKeyRangeResponse),
                "application/json",
                statusCode: ToStatusCode(response.Success, response.Status));
        });

        // Teardown: drops the space's descriptors from the replicated map, clearing the routing mode
        // on every node through the normal replication path. Same forwarding contract as register.
        app.MapPost("/v1/ranges/unregister", async (
            [FromBody] KahunaKeyRangeRequest request, IKahuna keyValues, HttpContext httpContext) =>
        {
            KahunaRemoveKeyRangeResponse response = await keyValues
                .RemoveKeyRangeWithOutcomeAsync(request.KeySpace, httpContext.RequestAborted)
                .ConfigureAwait(false);

            return Results.Text(
                JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaRemoveKeyRangeResponse),
                "application/json",
                statusCode: ToStatusCode(response.Success, response.Status));
        });

        // Splits the range covering splitKey at exactly that key. Unlike register, this IS
        // leader-only — the range map and the partition lifecycle are both owned by one partition,
        // and a node that does not lead it refuses without attempting anything.
        //
        // The body's `determinate` flag is the part a fault-injection harness must read: a 409 can
        // mean "refused, nothing happened" or "failed mid-cutover, the map may still change", and
        // the status code alone cannot tell those apart. An operator escape hatch and a test hook —
        // not a rebalancing feature; the auto-splitter still owns routine boundaries.
        app.MapPost("/v1/ranges/split", async (
            [FromBody] KahunaSplitRangeRequest request, IKahuna keyValues, HttpContext httpContext) =>
        {
            KahunaSplitRangeResponse response = await keyValues
                .SplitRangeAtKeyWithOutcomeAsync(request.KeySpace, request.SplitKey, httpContext.RequestAborted)
                .ConfigureAwait(false);

            return Results.Text(
                JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaSplitRangeResponse),
                "application/json",
                statusCode: ToStatusCode(response.Success, response.Status));
        });

        // Runs the merge pass on demand: the same work the periodic checker does, without waiting
        // for its cadence. Leader-only for the same reason as split, and refusing rather than
        // returning 0 is the point — the underlying trigger answers 0 on a non-leader, which reads
        // exactly like "nothing was eligible".
        //
        // No request body and no key-space filter: the pass scans every key-range space, and the
        // minimum size it enforces is configuration (--range-merge-min-size), not a per-request
        // knob. Accepting one would let a caller fold ranges the running policy considers too large.
        app.MapPost("/v1/ranges/merge", async (IKahuna keyValues, HttpContext httpContext) =>
        {
            KahunaMergeRangesResponse response = await keyValues
                .MergeRangesWithOutcomeAsync(httpContext.RequestAborted)
                .ConfigureAwait(false);

            return Results.Text(
                JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaMergeRangesResponse),
                "application/json",
                statusCode: ToStatusCode(response.Success, response.Status));
        });
    }

    /// <summary>
    /// Maps an outcome to a status code. Malformed input is the caller's to fix (400); everything
    /// else that did not succeed is a condition of the cluster the caller may be able to act on
    /// (409), and the body's status says which — a code alone cannot distinguish "never going to
    /// work" from "not visible here yet".
    /// </summary>
    public static int ToStatusCode(bool success, string status) => success
        ? StatusCodes.Status200OK
        : status == "InvalidInput"
            ? StatusCodes.Status400BadRequest
            : StatusCodes.Status409Conflict;
}
