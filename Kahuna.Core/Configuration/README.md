# Configuration

Configuration contains the core runtime settings and validation logic used before constructing Kahuna managers and actors.

`KahunaConfiguration` describes storage, worker counts, Raft partition settings, and other server-side values. `ConfigurationValidator` normalizes and rejects unsupported combinations early so the rest of the core can assume a valid configuration.

Keep validation rules here when they apply to process-wide setup. Component-specific request validation should stay in the relevant manager or handler.

Configuration surfaces differ: `KahunaCommandLineOptions` maps server flags to both
`KahunaConfiguration` and Kommander's `RaftConfiguration`; embedded hosts map
`EmbeddedKahunaOptions` separately. A core setting is not necessarily a CLI flag, and a server Raft
flag is not necessarily an embedded option. For current receive-staging, timeout, scheduling and
retention defaults, see the [snapshot and Raft recovery guide](../../docs/snapshot-and-raft-recovery-guide.md).
Transaction settlement defaults and upgrade gates are in the
[durable settlement guide](../../docs/durable-settlement-guide.md).
