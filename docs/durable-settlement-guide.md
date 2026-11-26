# Durable transaction settlement guide

A durable transaction's **decision** and its **settlement** are different events. The canonical
transaction record decides commit or abort. Settlement installs committed values into the key/value
projection and removes the participant's prepared intents. With `DurableDeferredSettlement = true`
(the default), commit can return before settlement; reads resolve the remaining intents through the
canonical record. See the [transaction lifecycle guide](transaction-lifecycle-guide.md#8-deferred-settlement-the-default)
for visibility and the [coordinator guide](reusable-transaction-coordinator-guide.md) for retries.

## Materialization records

With `DurableMaterializeOnResolve = false`, the finalizer writes one key/value materialization record
per modified key of a committed transaction before it submits the partition's settlement delta. This
was the default before materialization at settlement apply became the default. A failed materialization leaves that intent available for recovery rather than removing the
only durable copy of the value.

`DurableMaterializeByReference` defaults to `true`: the materialization record identifies the prepared
intent instead of repeating its value bytes. Every replica installs from its own intent. Setting it
to `false` produces value-carrying materialization records. Both encodings remain supported by live
apply and WAL replay. Logged type `30` from the older build that shifted `MaterializeIntent` is
also decoded as by-reference materialization (the actor-only `DropLeaderState` is never logged); PITR reconstructs a by-reference mutation from prepares in the replayed history.

## Materialization at settlement apply (default)

`DurableMaterializeOnResolve` defaults to `true` on `KahunaConfiguration` and on `EmbeddedKahunaOptions`.
Set it to `false` to write materialization records:

```csharp
var options = new EmbeddedKahunaOptions
{
    DurableMaterializeOnResolve = false,
    DurableDeferredSettlement = true
};
```

These settings describe settlement, not disk storage: an embedded host still needs persistent backend
and WAL configuration for process-loss durability. `Kahuna.Server` exposes the option as
`--durable-materialize-on-resolve true|false`. It does not expose `DurableMaterializeByReference`.

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

## Merged settlement entries

Set `DurableMergeAdjacentSettles = true` on `KahunaConfiguration` or `EmbeddedKahunaOptions`, or start
`Kahuna.Server` with `--durable-merge-adjacent-settles true`. The option is off by default.

With the option on, the write scheduler of a partition leader puts the settlements that are adjacent in
one batch into one log entry. The merge happens at dispatch, after the scheduler selected the batch:

- A settlement joins a merged entry when it is one auto-commit prepared-intent entry at generation zero,
  with no range fence, with resolves and removals only, and with the term fence of the other members.
  A submission that fails a condition ends the group and keeps its own entry.
- The merged entry is serialized from the commands of its members, in queue order. Each command carries
  the identity of its own transaction, so the commands of several transactions can share one entry. The
  order of the commands in the log does not change. Only the borders of the entries move.
- A merged entry has at most 512 commands. A longer group continues in a second entry.
- Each settlement keeps its own admission and its own class. The merged entry waits one time for its
  ordered apply on the leader, and its answer goes to every settlement that it carries. A batch that does
  not commit, or an apply that the leader does not observe, answers each producer as not committed, and
  each producer sends its settlement again. A second copy of a settlement is a no-op at apply.

Every build that applies a settlement delta reads a merged one: the settlement that a one-phase bundle
carries for a committed predecessor already holds the commands of several transactions. So the option
needs no upgrade order, and turning it off is safe at any time.

All rows that a merged entry installs are open on one log index, so the durability floor of the
partition moves in larger steps. An exception in the apply of a merged entry stops the commands of every
transaction in it, and the command bound limits that number.

`kahuna.kv.write.settle_runs` records the groups of adjacent settlements in each batch, and
`kahuna.kv.write.settles_merged` counts the settlements that merged entries carried. A settlement stays
one submission when it is merged, so `kahuna.kv.write.admitted` does not change.

## Durability, replay and recovery

A materializing settlement can produce multiple backend rows at one Raft index, plus completion
receipts. The application-durability floor holds that index until **every derived row is flushed** and
the required store snapshots, including receipts, are durable. Applying the intent removal in memory
alone is insufficient to release WAL retention. A settled intent whose row is still awaiting flush is
retained for restart replay; it is also included in the durable intent snapshot.

Duplicate settlement is idempotent, and it is routine with materialization at settlement apply: a
one-phase bundle that writes a key of a committed and unsettled transaction carries that transaction's
settlement (see the [transaction lifecycle guide](transaction-lifecycle-guide.md), section 6.3.1), and the
transaction's own resolution proposes it too. Live apply does not install an already removed intent again.
It checks that the value is on the node: a queued write of the key or a committed head at or after the
commit timestamp of the resolve, or else the durable row, which is read off the apply path. A resolve that
fails the check names a committed value that this replica never held, because its apply refused the prepare
that the peers installed. The replica counts `kahuna.kv.materialization_intent_missing` and gates the
partition for a re-seed (see the
[leadership fencing guide](leadership-fencing-and-apply-fingerprint-guide.md#containment)).
Restart replay can reconstruct from the retained settled intent while its row is not yet durable.
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
- For materializing settlement: every build from 1.10.1 installs values on materializing resolves. A
  node of an older build can read such a resolve as a plain settlement and remove the intent without
  installing its value. This is silent data loss on that replica. A group that comes from a build before
  1.10.1 sets `DurableMaterializeOnResolve = false` (server option
  `--durable-materialize-on-resolve false`) on every node, runs a build from 1.10.1 on every node, and
  then removes the setting.
- For the one-phase bundle entry: keep `DurableOnePhaseBundleEntry = false` until every node runs a
  build that applies the `onephasebundle` log type. An older node skips the entry with no error, so it
  has no record and no intent of the transaction. The leader also proposes three entries while a
  replica did not confirm the type (see the
  [transaction lifecycle guide](transaction-lifecycle-guide.md), section 6.3.1).
- Merged settlement entries (`DurableMergeAdjacentSettles`) need no upgrade order.

Turning either producer option off is safe for nodes that support both encodings; existing records
remain readable. Once new records have been written, turning the option off does not make a downgrade
to an older reader safe.

`kahuna.kv.write.stage_entries{stage}` counts dispatched entries by producing stage. Use it to distinguish
materialization entries from decision/prepare/settlement entries; entry reduction is not by itself a
latency or throughput guarantee.
