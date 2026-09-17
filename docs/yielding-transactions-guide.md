# Yielding transactions

Maintenance work — bulk rewrites, backfills, compaction-like sweeps — runs as ordinary transactions.
That work must never make a foreground transaction fail. A **yielding** transaction is one that steps
aside: when a foreground writer meets one of its write intents, the foreground writer takes the key and
the yielding transaction loses it.

This guide explains the option, its one hard guarantee, its limits, and the trade-off you accept when you
use it.

## The option

Start an interactive transaction with `ConflictPolicy = Yield`:

```csharp
KahunaTransactionOptions options = new()
{
    Locking = KeyValueTransactionLocking.Pessimistic,
    ConflictPolicy = TransactionConflictPolicy.Yield,
};
```

The default is `TransactionConflictPolicy.Normal`, which is the behaviour that existed before this option.
A cluster where no transaction yields behaves exactly as before, byte for byte on the wire.

The option applies to interactive sessions only. A script transaction has no way to set it, so a script
never yields.

## What yielding does

A `Normal` transaction, or a plain write with no transaction, that meets a live write intent owned by a
`Yield` transaction **takes the key over** instead of being denied:

- The foreground writer gets `Locked` (or its write proceeds), not `AlreadyLocked` or `MustRetry`.
- The yielding transaction's staged write for that key is dropped.
- The yielding transaction learns it lost the key on its next operation on that key, which answers
  `Aborted`, and, if it never touches the key again, at commit, which answers `Aborted`.

Two yielding transactions do not steal from each other. They conflict exactly as two normal transactions
do.

## The one hard guarantee

**A yielding transaction never commits a write to a key it lost.**

Every finalize of a yielding transaction first claims — *pins* — every intent it still holds. A key that
was taken over answers the pin with a conflict, and the whole transaction aborts. This runs before every
finalize shape: one-phase durable commit, two-phase durable commit, and the in-memory ephemeral path. A
normal transaction's finalize is unchanged and sends no pin.

Once an intent is pinned, a foreground writer that meets it no longer takes it over. Instead the writer is
told to wait and retry under its existing bounded wait, because the pinned intent's owner is already
committing and its decision is on the way. If that decision never lands — a coordinator crash — the pinned
intent is released by its lease or by the liveness ceiling, exactly as any other intent is, and the waiter
then proceeds.

## Limits

A yielding transaction may **not** hold a prefix lock or a range lock. The takeover rule covers point-key
intents only, so a prefix or range lock would silently keep the old, non-yielding behaviour. An acquire of
either from a yielding transaction is refused with `Errored`. A consumer that needs yielding must not take
those locks in a yielding transaction.

## The trade-off: starvation

A yielding transaction that keeps losing its keys to a steady stream of foreground writers keeps aborting.
This is by design: foreground work always wins. A consumer that runs maintenance work in small, bounded
batches and retries the aborted ones makes progress in the gaps between foreground writes. A consumer that
runs one large yielding transaction over hot keys may never commit it.

Watch `kahuna.transactions.yield_aborts` to see how often yielding work is losing, and shrink the batch or
back off when the rate is high.

## Priority is a different thing

`TransactionPriority` and `ConflictPolicy` are separate and independent. Priority governs **admission
order** only — which of several waiting transactions starts first when a node is at its concurrency
ceiling — and it never changes locking, MVCC, 2PC, or commit semantics. Yielding changes what happens to a
running transaction's intents. A transaction can set either, both, or neither.

## Observability

| Metric | Meaning |
|---|---|
| `kahuna.transactions.yielded_intents` | Yielding intents taken over by a foreground writer, tagged by operation (`lock`, `set`, `delete`, `extend`). |
| `kahuna.transactions.yield_aborts` | Yielding transactions aborted after losing a key, tagged by where the loss was caught (`follow_up`, `pin`). |
| `kahuna.transactions.pinned_waits` | Foreground requests told to wait because the key's yielding intent is pinned for its owner's commit. |

A debug log line records each takeover with the key, the losing owner's id, and the taking requester's id.
