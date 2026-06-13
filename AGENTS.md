# AGENTS.md

Guidance for AI coding agents working in this repository.

## ⛔ Dependency source: read it from disk, never disassemble

The full source for the three sibling dependencies is checked out on this machine. To inspect
how Kahuna / Kommander / Nixie behave, **read these directories with the normal file tools**
(`Read`, `Grep`, `Glob`):

- **Kommander** — `/Users/andresgutierrez/kommander`
- **CamusDB** — `/Users/andresgutierrez/camusdb`
- **Nixie** — `/Users/andresgutierrez/nixie`

You **MUST NOT** decompile, disassemble, or otherwise inspect the compiled assemblies. Do not
run `ilspycmd`, `ildasm`, `monodis`, `ikdasm`, `dotnet-ildasm`, `strings`/`grep` over `*.dll`,
or any decompiler, and do not go digging in `~/.nuget` or `bin/`/`obj/` for the DLLs. The `.cs`
source at the paths above is the canonical, authoritative reference — the DLLs are just its
build output. If a type/method isn't where you expect, `grep` the source tree, don't reach for
a disassembler.

**If the right fix belongs in Kommander / Nixie, say so and stop — do not work around
it in Kahuna.** When a bug or missing capability is genuinely in the dependency (e.g. a missing
`IRaft` hook, a Kommander catch-up gap, a Nixie API that should behave differently), describe
the change you'd make in the sibling repo and **pause for the user's decision**. Do **not**
patch the dependency's source yourself, and do **not** bury a workaround/shim/reflection hack in
Kahuna to route around it. A clear "this needs a change in `<repo>`: …" plus stopping is the
correct outcome — surfacing it is far more valuable than a hidden workaround.

## ⛔ No spec/task references in code

**Specs and task documents must never be mentioned in the code** — not in comments, method names,
class names, variable names, log messages, or test names. Documents like
`specs/spec-shared-range-locks.md`, `…-tasks.md`, or task identifiers such as "T5b", "K1", "Task 4",
"option A/B" are planning artifacts; they are renamed, split, and deleted as work evolves, so any
reference to them rots immediately and leaks process noise into the source.

Describe **what the code does and why**, not which document or task asked for it. Replace
`// T5b: clamp the lock` with `// Clamp the lock to the destination range`; replace `// see K3 option B`
with the actual invariant or trade-off being made. If a design decision needs justification, state the
decision itself in the comment — never defer to an external doc the reader can't see.

## Document layout: specs, archived specs, and docs

Keep planning and reference material in three well-defined places:

- **`specs/`** — every spec and its companion `…-tasks.md` lives here while the work is **in
  progress** (design not yet fully implemented).
- **`specs/archived/`** — once a spec is **fully implemented** (the feature code exists and its
  acceptance is met), move the spec and its task file here. Keep them as a historical record; do
  not delete them.
- **`docs/`** — **user-facing / maintainer documentation** (developer guides, how-tos, reference
  material for the shipped system). These are not specs and are not archived; they evolve with the
  code. Do not put specs in `docs/`, and do not put guides in `specs/`.

When you finish implementing a spec, archive it. When you write a new spec, put it in `specs/`.

## Test Execution Rules

**Never run multiple `dotnet test` commands in parallel or in the background.**
Run one test suite at a time and wait for it to finish before proceeding.
The test suite starts embedded Kahuna/Kommander instances and timing-sensitive transaction state; concurrent runs interfere with each other and produce false failures.

**The full test suite cannot be run without a running Docker cluster.** Most tests require a
multi-node cluster brought up via Docker. The only tests that can be run without Docker are
`Kahuna.Tests.Server.*` — restrict test runs to that namespace when no cluster is available.

**The full test suite currently takes ~20 minutes to run.** Be deliberate before launching a
full run — prefer scoping to the specific tests affected by your change, and only run everything
when it's genuinely warranted.
