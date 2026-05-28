# Configuration

Configuration contains the core runtime settings and validation logic used before constructing Kahuna managers and actors.

`KahunaConfiguration` describes storage, worker counts, Raft partition settings, and other server-side values. `ConfigurationValidator` normalizes and rejects unsupported combinations early so the rest of the core can assume a valid configuration.

Keep validation rules here when they apply to process-wide setup. Component-specific request validation should stay in the relevant manager or handler.
