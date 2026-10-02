using Xunit;

namespace Kahuna.Server.Tests;

// Groups the tests that observe the process-global "kahuna.snapshot_floor.missing_protected_version_total"
// counter. That counter lives on a static Meter shared by every node, so a test that deliberately drives
// it non-zero (defective-prune injection) must never run concurrently with a test that asserts it stayed
// zero. Assigning all such tests to one collection makes xUnit run them sequentially relative to each
// other; the collection itself still runs in parallel with the rest of the suite, because no test outside
// this group ever pushes that counter above zero under correct behavior.
[CollectionDefinition("SnapshotFloorMetrics")]
public sealed class SnapshotFloorMetricsCollection { }

// Groups the tests that observe the process-global "kahuna.kv.materialization_intent_missing" and
// "kahuna.kv.restore_by_reference_unresolved" counters. They live on the same static Meter, and a test of
// this group either drives them non-zero on purpose (a by-reference record with no source) or asserts an
// exact total over a window in which a node restarts. Two such tests that overlap read each other's
// increments, so all of them share one collection and run sequentially relative to each other; the
// collection still runs in parallel with the rest of the suite, because no test outside this group pushes
// those counters above zero under correct behavior.
[CollectionDefinition("MaterializationMissMetrics")]
public sealed class MaterializationMissMetricsCollection { }
