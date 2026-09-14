
namespace Kahuna.Extensibility;

/// <summary>
/// A user-defined function the Kahuna script language can call.
///
/// <para>Six rules bind every implementation. Kahuna cannot detect a violation, so a violation is a
/// bug in the calling application.</para>
///
/// <list type="number">
/// <item><description><b>Be synchronous and fast.</b> Do no network, no disk, no lock acquisition, no
/// <c>Task.Wait()</c>, no <c>.Result</c> and no <c>Thread.Sleep</c>. The call blocks a request path
/// while the transaction holds its locks and write intents. Aim for microseconds. A call slower than
/// the configured warning threshold is logged.</description></item>
/// <item><description><b>Do not call back into Kahuna.</b> Do not use <c>IKahuna</c>, a
/// <c>KahunaClient</c>, or any other Kahuna API from inside a function. Re-entry can deadlock
/// against the actor mailbox and against the calling transaction's own write intents.</description></item>
/// <item><description><b>Be thread-safe.</b> One registration serves the whole node, and many
/// transactions can call it at the same time. Prefer a function that holds no state. State that is
/// captured must be immutable, because synchronization under contention breaks rule 1.</description></item>
/// <item><description><b>Be idempotent.</b> A script transaction can run more than once. Kahuna
/// promises nothing about how many times a function is invoked for one logical transaction, so a
/// function must not send mail, publish to a queue, or move any other external counter.</description></item>
/// <item><description><b>Prefer determinism.</b> A non-deterministic function is safe to replicate,
/// because Raft carries the result and not the call. It does mean a retried transaction can store a
/// different value than its first attempt produced. Read
/// <see cref="KahunaFunctionContext.ReadTimestamp"/> rather than the wall clock.</description></item>
/// <item><description><b>Bound the output.</b> A returned string or byte buffer can become a key's
/// value and travels through Raft. Keep it inside the size limits any write obeys.</description></item>
/// </list>
/// </summary>
/// <param name="context">Identity and diagnostics for this call.</param>
/// <param name="args">The evaluated arguments, in script order. Valid for the duration of the call only.</param>
public delegate KahunaValue KahunaFunctionDelegate(in KahunaFunctionContext context, ReadOnlySpan<KahunaValue> args);
