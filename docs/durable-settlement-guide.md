# Durable transaction settlement guide

A durable transaction's **decision** and its **settlement** are different events. The canonical
transaction record decides commit or abort. Settlement installs committed values into the key/value
projection and removes the participant's prepared intents. With `DurableDeferredSettlement = true`
(the default), commit can return before settlement; reads resolve the remaining intents through the
canonical record. See the [transaction lifecycle guide](transaction-lifecycle-guide.md#8-deferred-settlement-the-default)
for visibility and the [coordinator guide](reusable-transaction-coordinator-guide.md) for retries.

## Materialization records (default)

`DurableMaterializeOnResolve` defaults to `false`. For a committed transaction, the finalizer writes
one key/value materialization record per modified key before it submits the partition's settlement
delta. A failed materialization leaves that intent available for recovery rather than removing the
only durable copy of the value.

`DurableMaterializeByReference` defaults to `true`: the materialization record identifies the prepared
intent instead of repeating its value bytes. Every replica installs from its own intent. Setting it
to `false` produces value-carrying materialization records. Both encodings remain supported by live
apply and WAL replay. Logged type `30` from the older build that shifted `MaterializeIntent` is
also decoded as by-reference materialization (the actor-only `DropLeaderState` is never logged); PITR reconstructs a by-reference mutation from prepares in the replayed history.

## Materialization at settlement apply (opt-in)

Set `DurableMaterializeOnResolve = true` on `KahunaConfiguration`, or on `EmbeddedKahunaOptions`:

```csharp
var options = new EmbeddedKahunaOptions
{
    DurableMaterializeOnResolve = true,
    DurableDeferredSettlement = true
};
```

These settings describe settlement, not disk storage: an embedded host still needs persistent backend
and WAL configuration for process-loss durability. `Kahuna.Server` currently exposes neither
`DurableMaterializeOnResolve` nor `DurableMaterializeByReference` as a command-line option.

With this option enabled, no separate materialization record is proposed. The settlement delta contains
commit resolves with `MaterializeOnResolve` and the commit HLC, followed by intent removals. Each replica's
ordered apply installs each value from its local prepared intent **before** applying its removal. The
finalizer and participant recovery use the same settlement builder. `DurableMaterializeByReference` has
no effect while this path is enabled.

The installation belongs to settlement, not to the one-phase commit decision. Settlement is submitted
after the canonical commit is established and the abort fence passes; PITR can therefore replay it
without reconstructing the one-phase decision's apply-time validation gate. A leader-local apply before
settlement is only a best-effort head start and does not gate the materializing settlement.

This saves per-key materialization entries. It does not remove prepare/decision work or combine
independent transactions into a new atomic application operation. Deferred versus synchronous
settlement remains controlled independently by `DurableDeferredSettlement`.

## Durability, replay and recovery

A materializing settlement can produce multiple backend rows at one Raft index, plus completion
receipts. The application-durability floor holds that index until **every derived row is flushed** and
the required store snapshots, including receipts, are durable. Applying the intent removal in memory
alone is insufficient to release WAL retention. A settled intent whose row is still awaiting flush is
retained for restart replay; it is also included in the durable intent snapshot.

Duplicate settlement is idempotent. Live apply does not install an already removed intent again;
restart replay can reconstruct from the retained settled intent while its row is not yet durable.
A crash after the decision but before settlement leaves committed prepared intents for the recovery
sweep to settle and install. Whole-partition snapshot seeding also carries the intent state needed for
a subsequent materializing settlement.

PITR expands a materializing resolve from its replayed prepare and filters the resulting row by the
transaction's **commit HLC**, not the time settlement ran. A recent duplicate settlement is tolerated.
If a materializing resolve has neither a replayed prepare nor recognized duplicate history, restore
fails closed instead of silently omitting a committed value. See the
[backup and PITR guide](backups-and-point-in-time-recovery-guide.md).

## Rolling upgrades

Every replica must understand an encoding **before any producer enables it**:

- For by-reference materialization, use `DurableMaterializeByReference = false` during a rollout from
  builds that do not apply by-reference records. Enable it after all nodes support them.
- For materializing settlement, keep `DurableMaterializeOnResolve = false` until all nodes install values
  on materializing resolves. An older node can read such a resolve as a plain settlement and remove the
  intent without installing its value. This is silent data loss on that replica.

Turning either producer option off is safe for nodes that support both encodings; existing records
remain readable. Once new records have been written, turning the option off does not make a downgrade
to an older reader safe.

`kahuna.kv.write.stage_entries{stage}` counts dispatched entries by producing stage. Use it to distinguish
materialization entries from decision/prepare/settlement entries; entry reduction is not by itself a
latency or throughput guarantee.
