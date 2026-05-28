# Utils

Utils contains small infrastructure helpers that are shared by multiple core components.

Current examples include:

- `BTree`, used by actors for ordered in-memory state and prefix-style lookups.
- `PathUtils`, used by storage and runtime setup paths.

Keep this folder limited to generic helpers. Component-specific utilities should stay near the component that owns the behavior.
