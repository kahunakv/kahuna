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
