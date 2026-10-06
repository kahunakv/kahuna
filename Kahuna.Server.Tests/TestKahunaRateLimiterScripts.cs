using System.Text;
using Kahuna.Client;
using Kahuna.Client.RateLimiting;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Runs the scripts behind the Kahuna-backed limiters directly, without a limiter in front of them,
/// and checks the raw answer and the state each one leaves behind.
///
/// The limiter tests check what a caller sees: grants, refusals and shared budgets. They cannot see
/// the exact <c>"1:&lt;n&gt;"</c> / <c>"0:&lt;ms&gt;"</c> answer, the stored state or its expiry, and they
/// run most limiters over the ephemeral key space only. Here every script runs in both durabilities,
/// because the persistent form is a text rewrite of the ephemeral one.
///
/// The clock cannot be moved from a test, so the time arithmetic is checked by seeding a state whose
/// timestamps lie in the past, relative to an <c>hlc()</c> reading taken just before. Windows and
/// periods are an hour long, so the script's own reading stays in the window of that earlier reading.
/// Every bound on a time value allows for the time between the reading before and the reading after.
/// </summary>
public class TestKahunaRateLimiterScripts
{
    private const long Hour = 3_600_000;

    private readonly ILoggerFactory loggerFactory;

    public TestKahunaRateLimiterScripts(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static string NewKey() => "rate-limit/script/" + Guid.NewGuid().ToString("N")[..8];

    private async Task<EmbeddedKahunaNode> StartNode()
    {
        EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(Token);

        return node;
    }

    private static KahunaClient ClientFor(EmbeddedKahunaNode node) =>
        new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

    private static KahunaRateLimiterScript FixedWindow(KeyValueDurability durability) =>
        KahunaRateLimiterScripts.Select(durability, KahunaRateLimiterScripts.EphemeralFixedWindow, KahunaRateLimiterScripts.PersistentFixedWindow);

    private static KahunaRateLimiterScript SlidingWindow(KeyValueDurability durability) =>
        KahunaRateLimiterScripts.Select(durability, KahunaRateLimiterScripts.EphemeralSlidingWindow, KahunaRateLimiterScripts.PersistentSlidingWindow);

    private static KahunaRateLimiterScript TokenBucket(KeyValueDurability durability) =>
        KahunaRateLimiterScripts.Select(durability, KahunaRateLimiterScripts.EphemeralTokenBucket, KahunaRateLimiterScripts.PersistentTokenBucket);

    private static KahunaRateLimiterScript ConcurrencyAcquire(KeyValueDurability durability) =>
        KahunaRateLimiterScripts.Select(durability, KahunaRateLimiterScripts.EphemeralConcurrencyAcquire, KahunaRateLimiterScripts.PersistentConcurrencyAcquire);

    private static KahunaRateLimiterScript ConcurrencyRenew(KeyValueDurability durability) =>
        KahunaRateLimiterScripts.Select(durability, KahunaRateLimiterScripts.EphemeralConcurrencyRenew, KahunaRateLimiterScripts.PersistentConcurrencyRenew);

    private static KahunaRateLimiterScript ConcurrencyRelease(KeyValueDurability durability) =>
        KahunaRateLimiterScripts.Select(durability, KahunaRateLimiterScripts.EphemeralConcurrencyRelease, KahunaRateLimiterScripts.PersistentConcurrencyRelease);

    private static List<KeyValueParameter> Parameters(params (string Name, object Value)[] parameters) =>
        parameters.Select(p => new KeyValueParameter { Key = p.Name, Value = p.Value.ToString() }).ToList();

    /// <summary>
    /// Runs a script, and runs it again while the cluster answers that the transaction aborted or
    /// must be retried, as the limiters do. Neither outcome changed the state.
    /// </summary>
    private static async Task<KahunaKeyValueTransactionResult> Run(KahunaClient client, KahunaRateLimiterScript script, params (string Name, object Value)[] parameters)
    {
        List<KeyValueParameter> list = Parameters(parameters);

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return await client.ExecuteKeyValueTransactionScript(script.Bytes, script.Hash, list, Token);
            }
            catch (KahunaException ex) when (ex.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry && attempt < 20)
            {
                await Task.Delay(10 * (attempt + 1), Token);
            }
        }
    }

    /// <summary>
    /// Runs a decision script and reads its answer. Every answer must be <c>"1:&lt;n&gt;"</c> or
    /// <c>"0:&lt;n&gt;"</c> with n not negative: a grant never reports fewer than zero permits left,
    /// and a refusal never asks the caller to wait for a time that already passed.
    /// </summary>
    private static async Task<(bool Granted, long Value)> Decide(KahunaClient client, KahunaRateLimiterScript script, params (string Name, object Value)[] parameters)
    {
        KahunaKeyValueTransactionResult result = await Run(client, script, parameters);

        string answer = Encoding.UTF8.GetString(result.FirstValue ?? []);

        Assert.Matches("^[01]:[0-9]+$", answer);

        return (answer[0] == '1', long.Parse(answer.AsSpan(2)));
    }

    /// <summary>The physical part of the cluster clock, as the scripts read it with <c>hlc()</c>.</summary>
    private static async Task<long> Now(KahunaClient client)
    {
        KahunaKeyValueTransactionResult result = await client.ExecuteKeyValueTransactionScript("RETURN to_string(hlc())", cancellationToken: Token);

        return long.Parse(result.FirstValueAsString!);
    }

    private static string ForDurability(string script, KeyValueDurability durability) =>
        durability == KeyValueDurability.Ephemeral
            ? script
            : script
                .Replace("EGET ", "GET ", StringComparison.Ordinal)
                .Replace("ESET ", "SET ", StringComparison.Ordinal)
                .Replace("EDELETE ", "DELETE ", StringComparison.Ordinal);

    /// <summary>Writes a limiter state as a script would, with an expiry far enough away.</summary>
    private static async Task Seed(KahunaClient client, KeyValueDurability durability, string key, string value)
    {
        KahunaKeyValueTransactionResult result = await client.ExecuteKeyValueTransactionScript(
            ForDurability("ESET @key @value EX to_int(@ttl)", durability),
            parameters: Parameters(("@key", key), ("@value", value), ("@ttl", 4 * Hour)),
            cancellationToken: Token
        );

        Assert.Equal(KeyValueResponseType.Set, result.Type);
    }

    /// <summary>Reads a state and its expiry, or null when the key does not exist.</summary>
    private static async Task<(string Value, long Expires)?> ReadState(KahunaClient client, KeyValueDurability durability, string key)
    {
        const string script = """
        LET s = EGET @key
        IF s == null THEN
          RETURN ""
        END
        RETURN concat(to_string(s), concat("|", to_string(expires(s))))
        """;

        KahunaKeyValueTransactionResult result = await client.ExecuteKeyValueTransactionScript(
            ForDurability(script, durability),
            parameters: Parameters(("@key", key)),
            cancellationToken: Token
        );

        string answer = result.FirstValueAsString ?? "";

        if (answer.Length == 0)
            return null;

        int bar = answer.LastIndexOf('|');

        return (answer[..bar], long.Parse(answer.AsSpan(bar + 1)));
    }

    private static void AssertBetween(long low, long high, long actual) =>
        Assert.True(actual >= low && actual <= high, $"Expected {actual} in [{low}, {high}]");

    // ---------------------------------------------------------------------------------------------
    // Fixed window
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// Permits are counted in the current window, a grant reports the permits left, and a refusal
    /// reports the time until the window ends. The state is the window start and the count, and it
    /// expires one second after the window ends.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestFixedWindowCountsPermitsAndRefusesUntilTheWindowEnds(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string key = NewKey();
        KahunaRateLimiterScript script = FixedWindow(durability);

        (string, object)[] Request(int permits) => [("@key", key), ("@window_ms", Hour), ("@limit", 5), ("@permits", permits)];

        long before = await Now(client);

        Assert.Equal((true, 3L), await Decide(client, script, Request(2)));
        Assert.Equal((true, 0L), await Decide(client, script, Request(3)));

        (bool granted, long reset) = await Decide(client, script, Request(1));

        long after = await Now(client);
        long end = (before / Hour) * Hour + Hour;

        Assert.False(granted);
        AssertBetween(end - after, end - before, reset);

        (string Value, long Expires)? state = await ReadState(client, durability, key);

        Assert.NotNull(state);
        Assert.Equal($"{end - Hour}:5", state.Value.Value);
        AssertBetween(end + 1000, end + 1000 + (after - before), state.Value.Expires);
    }

    /// <summary>
    /// Zero permits asks whether a permit is free. It reports the permits left and writes nothing,
    /// and once the window is spent it is refused like a request for one permit.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestFixedWindowZeroPermitsAsksWithoutSpending(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string key = NewKey();
        KahunaRateLimiterScript script = FixedWindow(durability);

        (string, object)[] Request(int permits) => [("@key", key), ("@window_ms", Hour), ("@limit", 2), ("@permits", permits)];

        Assert.Equal((true, 2L), await Decide(client, script, Request(0)));
        Assert.Null(await ReadState(client, durability, key));

        Assert.Equal((true, 0L), await Decide(client, script, Request(2)));

        (bool granted, long reset) = await Decide(client, script, Request(0));
        Assert.False(granted);
        Assert.InRange(reset, 1, Hour);

        (string Value, long Expires)? state = await ReadState(client, durability, key);
        Assert.EndsWith(":2", state!.Value.Value);
    }

    /// <summary>
    /// A count from an earlier window is read as empty, even a full one, so the budget resets on the
    /// boundary. A count from the current window is carried on.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestFixedWindowIgnoresTheCountOfAnEarlierWindow(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string earlier = NewKey();
        string current = NewKey();
        KahunaRateLimiterScript script = FixedWindow(durability);

        long start = (await Now(client) / Hour) * Hour;

        await Seed(client, durability, earlier, $"{start - Hour}:5");
        await Seed(client, durability, current, $"{start}:4");

        Assert.Equal((true, 4L), await Decide(client, script, ("@key", earlier), ("@window_ms", Hour), ("@limit", 5), ("@permits", 1)));
        Assert.Equal($"{start}:1", (await ReadState(client, durability, earlier))!.Value.Value);

        Assert.Equal((true, 0L), await Decide(client, script, ("@key", current), ("@window_ms", Hour), ("@limit", 5), ("@permits", 1)));
        Assert.False((await Decide(client, script, ("@key", current), ("@window_ms", Hour), ("@limit", 5), ("@permits", 1))).Granted);
        Assert.Equal($"{start}:5", (await ReadState(client, durability, current))!.Value.Value);
    }

    // ---------------------------------------------------------------------------------------------
    // Sliding window
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// Permits are counted in the newest segment. A refusal names the time at which enough of the
    /// oldest segments leave the window: here only the newest segment holds permits, so that is when
    /// it leaves, three segments later. The state expires one second after a whole window.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSlidingWindowCountsInTheNewestSegmentAndWaitsForItToLeave(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string key = NewKey();
        KahunaRateLimiterScript script = SlidingWindow(durability);

        (string, object)[] Request(int permits) => [("@key", key), ("@segment_ms", Hour), ("@segments", 3), ("@limit", 4), ("@permits", permits)];

        long before = await Now(client);

        Assert.Equal((true, 1L), await Decide(client, script, Request(3)));

        (bool granted, long wait) = await Decide(client, script, Request(2));

        long after = await Now(client);
        long cur = before / Hour;

        Assert.False(granted);
        AssertBetween((cur + 3) * Hour - after, (cur + 3) * Hour - before, wait);

        (string Value, long Expires)? state = await ReadState(client, durability, key);

        Assert.Equal($"{cur}:0:0:3", state!.Value.Value);
        AssertBetween(before + 3 * Hour + 1000, after + 3 * Hour + 1000, state.Value.Expires);

        // Zero permits reports what is left and writes nothing.
        Assert.Equal((true, 1L), await Decide(client, script, Request(0)));
        Assert.Equal($"{cur}:0:0:3", (await ReadState(client, durability, key))!.Value.Value);
    }

    /// <summary>
    /// The counts shift left by the segments that passed since the state was written, so the
    /// oldest permits leave the window. A refusal then waits only for the oldest segment that
    /// still holds permits.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSlidingWindowShiftsOutThePassedSegments(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string key = NewKey();
        KahunaRateLimiterScript script = SlidingWindow(durability);

        (string, object)[] Request(int permits) => [("@key", key), ("@segment_ms", Hour), ("@segments", 3), ("@limit", 4), ("@permits", permits)];

        long before = await Now(client);
        long cur = before / Hour;

        // Written one segment ago: its oldest segment, with two permits, has left the window since.
        await Seed(client, durability, key, $"{cur - 1}:2:1:1");

        Assert.Equal((true, 0L), await Decide(client, script, Request(2)));
        Assert.Equal($"{cur}:1:1:2", (await ReadState(client, durability, key))!.Value.Value);

        (bool granted, long wait) = await Decide(client, script, Request(1));

        long after = await Now(client);

        Assert.False(granted);
        AssertBetween((cur + 1) * Hour - after, (cur + 1) * Hour - before, wait);
    }

    /// <summary>
    /// A state older than a whole window, or one written with a different segment count, is read
    /// as empty.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSlidingWindowReadsAStaleOrForeignStateAsEmpty(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string stale = NewKey();
        string foreign = NewKey();
        KahunaRateLimiterScript script = SlidingWindow(durability);

        long cur = await Now(client) / Hour;

        await Seed(client, durability, stale, $"{cur - 3}:4:4:4");
        await Seed(client, durability, foreign, $"{cur}:4");

        Assert.Equal((true, 3L), await Decide(client, script, ("@key", stale), ("@segment_ms", Hour), ("@segments", 3), ("@limit", 4), ("@permits", 1)));
        Assert.Equal($"{cur}:0:0:1", (await ReadState(client, durability, stale))!.Value.Value);

        Assert.Equal((true, 3L), await Decide(client, script, ("@key", foreign), ("@segment_ms", Hour), ("@segments", 3), ("@limit", 4), ("@permits", 1)));
        Assert.Equal($"{cur}:0:0:1", (await ReadState(client, durability, foreign))!.Value.Value);
    }

    // ---------------------------------------------------------------------------------------------
    // Token bucket
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// A missing state is a full bucket. A grant reports the tokens left, and a refusal reports the
    /// time until enough whole periods refill the bucket. The state expires one second after the
    /// bucket would be full again, so an expired state and a full bucket are the same.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestTokenBucketStartsFullAndExpiresWhenFullAgain(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string key = NewKey();
        KahunaRateLimiterScript script = TokenBucket(durability);

        (string, object)[] Request(int permits) => [("@key", key), ("@limit", 5), ("@period_ms", Hour), ("@per_period", 2), ("@permits", permits)];

        Assert.Equal((true, 5L), await Decide(client, script, Request(0)));
        Assert.Null(await ReadState(client, durability, key));

        long before = await Now(client);

        Assert.Equal((true, 2L), await Decide(client, script, Request(3)));

        long after = await Now(client);

        (string Value, long Expires)? state = await ReadState(client, durability, key);
        string[] parts = state!.Value.Value.Split(':');

        Assert.Equal("2", parts[0]);
        long last = long.Parse(parts[1]);
        AssertBetween(before, after, last);

        // Two periods add four tokens, which fills the bucket from two.
        AssertBetween(last + 2 * Hour + 1000, after + 2 * Hour + 1000, state.Value.Expires);

        // Three tokens need one period of two tokens.
        (bool granted, long wait) = await Decide(client, script, Request(3));

        long end = await Now(client);

        Assert.False(granted);
        AssertBetween(Hour - (end - last), Hour, wait);
    }

    /// <summary>
    /// Every whole period since the last replenishment adds its tokens, up to the limit. The last
    /// replenishment moves forward by whole periods only, so the phase of the periods is kept.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestTokenBucketRefillIsCappedAndKeepsThePhase(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string capped = NewKey();
        string partial = NewKey();
        KahunaRateLimiterScript script = TokenBucket(durability);

        long before = await Now(client);

        // Ten periods ago with an empty bucket: twenty tokens would be added, five fit.
        long cappedLast = before - 10 * Hour - 1234;
        await Seed(client, durability, capped, $"0:{cappedLast}");

        Assert.Equal((true, 4L), await Decide(client, script, ("@key", capped), ("@limit", 5), ("@period_ms", Hour), ("@per_period", 2), ("@permits", 1)));

        (string Value, long Expires)? state = await ReadState(client, durability, capped);
        long phased = cappedLast + 10 * Hour;

        Assert.Equal($"4:{phased}", state!.Value.Value);

        // One token short of full: one period refills it, counted from the kept phase.
        long after = await Now(client);
        AssertBetween(phased + Hour + 1000, phased + Hour + 1000 + (after - before), state.Value.Expires);

        // Two periods ago with one token, at one token per period: three tokens.
        long partialLast = before - 2 * Hour - 5;
        await Seed(client, durability, partial, $"1:{partialLast}");

        (string, object)[] Request(int permits) => [("@key", partial), ("@limit", 5), ("@period_ms", Hour), ("@per_period", 1), ("@permits", permits)];

        Assert.Equal((true, 3L), await Decide(client, script, Request(0)));
        Assert.Equal($"1:{partialLast}", (await ReadState(client, durability, partial))!.Value.Value);

        Assert.Equal((true, 0L), await Decide(client, script, Request(3)));
        Assert.Equal($"0:{partialLast + 2 * Hour}", (await ReadState(client, durability, partial))!.Value.Value);
    }

    /// <summary>
    /// A refusal counts the part of the current period that already passed, so the caller waits
    /// only for the rest of it.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestTokenBucketRefusalWaitsForTheRestOfThePeriod(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string key = NewKey();
        KahunaRateLimiterScript script = TokenBucket(durability);

        long before = await Now(client);

        await Seed(client, durability, key, $"0:{before - Hour / 2}");

        (bool granted, long wait) = await Decide(client, script, ("@key", key), ("@limit", 5), ("@period_ms", Hour), ("@per_period", 1), ("@permits", 0));

        long after = await Now(client);

        Assert.False(granted);
        AssertBetween(Hour / 2 - (after - before), Hour / 2, wait);
    }

    // ---------------------------------------------------------------------------------------------
    // Concurrency
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// The live leases of a bucket are summed. A grant writes a lease that holds the permits it took
    /// and reports the permits left. A refusal cannot tell when a permit comes back, so it answers
    /// zero. Zero permits asks whether a permit is free and writes no lease.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestConcurrencyAcquireSumsTheLiveLeases(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string bucket = NewKey();
        KahunaRateLimiterScript script = ConcurrencyAcquire(durability);

        string LeaseKey() => bucket + "/" + Guid.NewGuid().ToString("N");

        (string, object)[] Request(string leaseKey, int permits) =>
            [("@bucket", bucket), ("@lease_key", leaseKey), ("@limit", 3), ("@permits", permits), ("@lease_ms", 60_000)];

        string probe = LeaseKey();
        Assert.Equal((true, 3L), await Decide(client, script, Request(probe, 0)));
        Assert.Null(await ReadState(client, durability, probe));

        string first = LeaseKey();
        long before = await Now(client);

        Assert.Equal((true, 1L), await Decide(client, script, Request(first, 2)));

        long after = await Now(client);

        (string Value, long Expires)? lease = await ReadState(client, durability, first);
        Assert.Equal("2", lease!.Value.Value);
        AssertBetween(before + 60_000, after + 60_000, lease.Value.Expires);

        string refused = LeaseKey();
        Assert.Equal((false, 0L), await Decide(client, script, Request(refused, 2)));
        Assert.Null(await ReadState(client, durability, refused));

        Assert.Equal((true, 0L), await Decide(client, script, Request(LeaseKey(), 1)));
        Assert.Equal((false, 0L), await Decide(client, script, Request(LeaseKey(), 0)));
    }

    /// <summary>
    /// A lease that expired is not counted, even while other leases of its bucket are live. That is
    /// how the permits of a process that died come back.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestConcurrencyAcquireSkipsExpiredLeases(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string bucket = NewKey();
        KahunaRateLimiterScript script = ConcurrencyAcquire(durability);

        string LeaseKey() => bucket + "/" + Guid.NewGuid().ToString("N");

        (string, object)[] Request(string leaseKey, int permits, long leaseMs) =>
            [("@bucket", bucket), ("@lease_key", leaseKey), ("@limit", 3), ("@permits", permits), ("@lease_ms", leaseMs)];

        string shortLived = LeaseKey();

        // Long enough that the refusal below runs while this lease is still live.
        Assert.Equal((true, 1L), await Decide(client, script, Request(shortLived, 2, 3000)));
        Assert.Equal((true, 0L), await Decide(client, script, Request(LeaseKey(), 1, 60_000)));
        Assert.Equal((false, 0L), await Decide(client, script, Request(LeaseKey(), 2, 60_000)));
        Assert.NotNull(await ReadState(client, durability, shortLived));

        for (int i = 0; i < 200 && await ReadState(client, durability, shortLived) is not null; i++)
            await Task.Delay(50, Token);

        Assert.Null(await ReadState(client, durability, shortLived));

        Assert.Equal((true, 0L), await Decide(client, script, Request(LeaseKey(), 2, 60_000)));
    }

    /// <summary>
    /// The renewal and the release answer nothing, so the outcome is the type of the result. A
    /// renewal of a live lease is Set and pushes its expiry out. A renewal of a lease that is gone
    /// is NotSet and does not write the lease again. The release deletes the lease, and a second
    /// release of it does not fail.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestConcurrencyRenewAndReleaseReportThroughTheResultType(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        KahunaClient client = ClientFor(node);

        string bucket = NewKey();
        string leaseKey = bucket + "/" + Guid.NewGuid().ToString("N");

        Assert.Equal((true, 1L), await Decide(client, ConcurrencyAcquire(durability),
            ("@bucket", bucket), ("@lease_key", leaseKey), ("@limit", 3), ("@permits", 2), ("@lease_ms", 60_000)));

        long acquiredExpiry = (await ReadState(client, durability, leaseKey))!.Value.Expires;

        KahunaKeyValueTransactionResult renewed = await Run(client, ConcurrencyRenew(durability),
            ("@lease_key", leaseKey), ("@permits", 2), ("@lease_ms", 120_000));

        Assert.Equal(KeyValueResponseType.Set, renewed.Type);

        (string Value, long Expires)? lease = await ReadState(client, durability, leaseKey);
        Assert.Equal("2", lease!.Value.Value);
        Assert.True(lease.Value.Expires >= acquiredExpiry + 60_000, $"{lease.Value.Expires} < {acquiredExpiry} + 60000");

        KahunaKeyValueTransactionResult released = await Run(client, ConcurrencyRelease(durability), ("@lease_key", leaseKey));

        Assert.Equal(KeyValueResponseType.Deleted, released.Type);
        Assert.Null(await ReadState(client, durability, leaseKey));

        KahunaKeyValueTransactionResult lapsed = await Run(client, ConcurrencyRenew(durability),
            ("@lease_key", leaseKey), ("@permits", 2), ("@lease_ms", 120_000));

        Assert.Equal(KeyValueResponseType.NotSet, lapsed.Type);
        Assert.Null(await ReadState(client, durability, leaseKey));

        KahunaKeyValueTransactionResult releasedAgain = await Run(client, ConcurrencyRelease(durability), ("@lease_key", leaseKey));

        Assert.NotEqual(KeyValueResponseType.Deleted, releasedAgain.Type);
        Assert.Null(await ReadState(client, durability, leaseKey));
    }
}
