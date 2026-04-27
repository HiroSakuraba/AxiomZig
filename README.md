# AxiomZig

**A resource-safety language for systems programming, compiler research, and ML pre-pretraining.**

AxiomZig is a small, statically typed systems language with explicit allocation, mandatory effect summaries, lexical borrows, and protocol-based resource state machines. It is designed so that every lifecycle obligation — open a file, close a file; begin a transaction, commit or roll it back — is enforced at compile time and expressed in the type system, not in documentation or convention.

---

## Philosophy

### Resources have states. States have obligations.

Most languages let you use a file handle after it has been closed, a transaction object after it has been committed, a lock after it has been released. The language does not know that these are mistakes. It has no model of resource lifecycle at all.

AxiomZig makes lifecycle a first-class concern. Every resource type is paired with a *protocol* that defines its states, the transitions between them, and which states are terminal. The checker verifies, on every control-flow path, that every resource reaches a terminal state before it leaves scope.

```axiomzig
protocol FileProtocol {
    states { Open, Closed }
    init Open;
    terminal { Closed }
    transitions {
        close: Open -> Closed;
    }
}

resource File {
    protocol: FileProtocol;
    fields { fd: I32; }
}
```

Now `f.close()` is not just a method call — it is a *state transition*. The checker knows the protocol. If you forget to close, close twice, or close in only one branch of an `if`, it tells you exactly which diagnostic applies.

### Effects are explicit and checked

Every function declares a summary of what it can do. Calling a function that performs I/O without declaring `io` in your effects clause is a static error. Calling a recursive function without declaring `recurse` is a static error. Declaring `trap` as an effect is itself an error — traps are not a capability a function may claim, they are runtime failures.

```axiomzig
fn read_file(path: Str) -> Str!IoError effects(io, error) {
    // ...
}

fn pure_transform(s: Str) -> Str effects() {
    // this function may not call read_file
}
```

Effects propagate upward through call chains. A callee's declared effects must be a subset of its caller's. This makes effect discipline auditable by reading the signature, not the body.

### Ownership is lexical and explicit

AxiomZig uses *lexical* borrows — a borrow ends when its enclosing block exits. No non-lexical lifetime inference. This is a deliberate restriction: predictable lifetime rules produce predictable diagnostics. When the checker says a borrow is live, you can find exactly where it started and where it ends by reading the code structure.

Move semantics apply to resources. Once you move a resource to another function, the original binding is invalid. The checker tracks this. Use-after-move is a static error, not undefined behavior.

### Defer and errdefer are protocol-aware

`defer` and `errdefer` integrate with ownership tracking. A `defer r.close()` is not just a cleanup call — it is a *discharge* of the resource `r`. The checker understands that if `r` is moved inside the deferred expression, the resource obligation is satisfied on the deferred path.

```axiomzig
fn open_and_process(path: Str) -> I64!IoError effects(io, error, protocol_step) {
    let f: File = File.open(path)?;
    defer f.close();            // discharges f on normal exit
    errdefer f.close_early();   // discharges f on error exit
    return process(borrow f)?;
}
```

### The source is the authority

The formatter is not a style guide — it is a canonical form. `format(parse(format(parse(x)))) == format(parse(x))` by construction. All tooling operates on canonical source. There is no debate about formatting and no way for two correctly-formatted programs to differ only in whitespace.

---

## Language at a glance

```axiomzig
module example.socket;

// Protocol definition
protocol SocketProtocol {
    states { Created, Connected, Closed }
    init Created;
    terminal { Closed }
    transitions {
        connect: Created -> Connected;
        close:   Connected -> Closed;
        abort:   Created -> Closed;
    }
}

resource Socket {
    protocol: SocketProtocol;
    fields { fd: I32; }
}

// Error set
error_set NetError { Refused; Unreachable; TimedOut; }

// Annotated public function
@since("1.0.0")
pub fn dial(host: Str, port: I64) -> Socket!NetError effects(io, error, protocol_step) {
    let s: Socket = Socket { fd: 0 };
    errdefer s.abort();    // on any error path: transition to Closed via abort
    try connect_syscall(borrow s, host, port);
    return s;              // caller now owns an Open socket
}

pub fn close(s: Socket) -> Unit effects(protocol_step) {
    s.close();
    return unit;
}
```

### Types

| Type | Description |
|------|-------------|
| `I64`, `I32`, `I8` | Signed integers |
| `U64`, `U32`, `U8` | Unsigned integers |
| `Bool` | Boolean |
| `Str` | String |
| `Unit` | Unit / void |
| `Option<T>` | Optional value (`some(v)` / `none`) |
| `T!E` | Result type (ok value or error from set `E`) |
| `Slice<T>` | Slice of T |
| `Array<T, N>` | Fixed-size array |
| `Ref<T>` | Mutable reference |
| `RefConst<T>` | Shared (immutable) reference |

### Effects

| Effect | Meaning |
|--------|---------|
| `alloc` | May allocate memory |
| `free` | May free memory |
| `io` | May perform I/O |
| `error` | May return an error value |
| `protocol_step` | May perform resource protocol transitions |
| `recurse` | Is recursive |

### Declarations

```axiomzig
struct Point { x: I64; y: I64; }

enum Direction { North; South; East; West; }

error_set ParseError { Invalid; Truncated; Overflow; }

protocol P { ... }

resource R { protocol: P; fields { ... } }

fn f(x: I64) -> I64 effects() { return x + 1; }

pub fn g() -> Unit effects() { return unit; }
```

### Control flow

```axiomzig
// if / else
if (cond) { ... } else { ... }

// while
while (n > 0) { n = n - 1; }

// for-range
for (i: I64 in 0..10) { ... }

// match
match x {
    some(v)  -> v + 1,
    none     -> 0,
}

// error propagation
let result: I64 = risky_call()?;

// catch with binding
let value: I64 = risky_call() catch |e| { return default; };

// defer / errdefer
defer cleanup();
errdefer rollback();
```

### Ownership

```axiomzig
// move
take(move resource);

// borrow (exclusive)
mutate(borrow resource);

// borrow (shared)
read(borrow resource);

// discard non-Unit value
_ = compute_something();
```

---

## Toolchain

AxiomZig is a Python reference implementation (~10 000 lines). All passes are exposed as CLI subcommands and can be composed:

```
lex → parse → resolve → elaborate → typecheck → cfg → owncheck → interpret
```

### Installation

```bash
git clone https://github.com/your-org/axiomzig
cd axiomzig
export PYTHONPATH=$(pwd)
python -m axiomzig.cli --help
```

No dependencies beyond the Python standard library.

### Common commands

```bash
# Check a source file (static analysis: types, effects, ownership, protocols)
python -m axiomzig.cli check src/main.az

# Check with automatic workspace discovery (imports resolved from sibling files)
python -m axiomzig.cli check src/main.az
# Use --no-workspace-discovery to skip
python -m axiomzig.cli check --no-workspace-discovery src/main.az

# Explicit module root for multi-file projects
python -m axiomzig.cli check --module-root src/ src/main.az

# Format (canonical idempotent form)
python -m axiomzig.cli format src/main.az

# Interpret (execute via reference interpreter)
python -m axiomzig.cli interpret src/main.az

# Export ownership signatures for cross-module analysis
python -m axiomzig.cli ownsig src/util.az > util.ownsig.json
python -m axiomzig.cli check src/main.az --import-ownership util.ownsig.json

# Emit package interface (public API surface)
python -m axiomzig.cli emit-package-interface src/lib.az \
    --package-name mylib --package-version 1.0.0 --out mylib.package.json

# Conformance manifest and publication manifest
python -m axiomzig.cli conform src/lib.az \
    --package-interface mylib.package.json --publication-out mylib.pub.json

# Publication diff (API compatibility check between two versions)
python -m axiomzig.cli pubdiff old.pub.json new.pub.json

# Canonical publication digest (integrity and signing support)
python -m axiomzig.cli pubdigest mylib.pub.json --out mylib.digest.json
python -m axiomzig.cli pubdigest mylib.pub.json --verify mylib.digest.json
```

### Diagnostic codes

Every error emitted by the checker carries a stable diagnostic code:

| Code | Meaning |
|------|---------|
| `E_OWN_MOVED` | Use of a moved resource |
| `E_OWN_UNDISCHARGED` | Resource leaves scope without reaching terminal state |
| `E_OWN_BORROW_LIVE` | Operation blocked by live borrow |
| `E_OWN_BORROW_EXCL` | Exclusive borrow conflicts with existing borrow |
| `E_OWN_BORROW_ALIAS` | Aliased references to incompatible parameters |
| `E_OWN_MERGE_CONFLICT` | Resource state differs across control-flow branches |
| `E_PROTO_TRANSITION` | Illegal protocol state transition |
| `E_PROTO_TERMINAL` | Resource not in terminal state at scope exit |
| `E_EFFECT_UNDER` | Callee requires an effect not declared by caller |
| `E_EFFECT_RECURSE` | Recursive function missing `recurse` effect |
| `E_EFFECT_TRAP` | `trap` declared as a function effect (not permitted) |
| `E_TYPE_MISMATCH` | Type mismatch |
| `E_TYPE_NONUNIT` | Non-Unit expression statement (use `_ = expr`) |
| `E_TYPE_NO_EXHAUST` | Non-exhaustive match |
| `E_DEFER_FLOW` | `defer` body contains `return`/`break`/`continue` |
| `E_DEFER_NESTED` | Nested `defer` inside `defer` body |
| `E_RESOLVE_SHADOW` | Forbidden name shadowing |
| `E_RESOLVE_UNBOUND` | Unresolved name |
| `R_TRAP_OVERFLOW` | Integer overflow |
| `R_TRAP_DIV_ZERO` | Division by zero |
| `R_TRAP_CAST` | Cast value out of range |
| `R_TRAP_BOUNDS` | Index out of bounds |

Codes are stable across versions. Prose messages may change; codes will not.

---

## Publication and package compatibility

AxiomZig has a first-class model of package API surfaces and their compatibility over time. This is not bolted on — it flows from the same ownership and protocol machinery that drives the static checker.

### How it works

1. **Emit a package interface** — the public functions, their signatures, their effect summaries, and any resource/protocol shapes they expose.
2. **Run `conform`** — produces a conformance manifest and a publication manifest with a `manifest_hash`, `workspace_source_digest`, and `dependency_publication_closure`.
3. **Run `pubdiff`** — compares two publication manifests and classifies every change:

| Change | Classification |
|--------|---------------|
| Public function removed | `incompatible` |
| Public function added | `compatible` |
| Resource field type changed | `incompatible` |
| Resource field added | `compatible` |
| Protocol transition removed | `incompatible` |
| Protocol transition added | `compatible` |
| Lifecycle annotation changed | `metadata-only` |
| Transitive dependency API removed | `incompatible` (via closure) |

4. **Run `pubdigest`** — compute a canonical SHA-256 digest over the publication manifest, excluding signature placeholder fields. Attach a detached signature externally, then verify with `pubdigest --verify`.

---

## Conformance suite

The conformance suite lives in `conformance/` and contains 368 test cases across 12 categories, each with a `.az` source file and a `.json` sidecar declaring the expected outcome and diagnostic code.

```bash
# Run the full conformance suite
PYTHONPATH=. pytest tests/test_conformance_runner.py

# Coverage summary
PYTHONPATH=. pytest tests/test_conformance_runner.py::test_conformance_coverage_summary -s
```

| Category | Tests | Min |
|----------|------:|----:|
| canonical\_roundtrip | 15 | 15 |
| defer | 29 | 25 |
| effects | 31 | 25 |
| formatter | 20 | 20 |
| lexer | 20 | — |
| name\_resolution | 24 | 20 |
| ownership | 46 | 40 |
| parser | 35 | 35 |
| protocols | 37 | 30 |
| runtime | 40 | 35 |
| typing | 62 | 50 |

The negative test ratio is ≥ 40%: at least 40% of tests assert that an invalid program is correctly rejected with the appropriate diagnostic code.

---

## ML pre-pretraining corpus

AxiomZig was designed with a secondary purpose: generating structured labeled examples for training language models on static reasoning tasks.

Programs and their checker outcomes form natural (source, label) pairs:

```
Program: resource r never closed
Label:   E_OWN_UNDISCHARGED

Program: function calls risky() without declaring error effect
Label:   E_EFFECT_UNDER

Program: r.close() called after r was already moved
Label:   E_OWN_MOVED

Program: both branches of if close the resource
Label:   pass
```

Because the checker is deterministic and the diagnostic codes are stable, every generated program can be automatically labeled. The corpus generator (`corpus_gen.py`) produces numbered `.zip` bundles, each containing `.az` source files paired with `.json` label sidecars. The ledger tracks which programs have been generated (by content hash) so subsequent runs produce only fresh content.

See the [corpus README](corpus/README.md) for the bundle format, label schema, and expansion roadmap.

---

## Process

### How AxiomZig was designed

The language was developed in a series of numbered implementation sessions, each incrementally extending a Python reference front-end. Every session began by running the full test suite to confirm the baseline, made a bounded change, and confirmed no regressions before shipping.

The implementation is organised into layers that mirror the language design:

```
lexer → parser → resolver → elaborator → typechecker → CFG lowerer
    → ownership analyser → checker → interpreter → publication toolchain
```

Each layer has its own test coverage. The ownership analyser has ~85 tests; the checker aggregates diagnostics from all passes; the publication toolchain has end-to-end pipeline tests. New features are added to the conformance suite at the same time they are added to the implementation.

### Design constraints

Several constraints were held throughout:

- **Lexical borrows, not NLL.** Non-lexical lifetime inference is explicitly excluded from v1.1. Lexical rules are predictable; predictable rules produce predictable diagnostics.
- **No generics, no floats, no concurrency in v1.** Each of these was identified as a scope item for a future version, not a v1.1 concern. Keeping the core small keeps the checker's invariants auditable.
- **One check, one diagnostic.** The checker deduplicates issues and assigns stable codes. A downstream consumer can test for `E_OWN_UNDISCHARGED` without depending on the exact prose.
- **The formatter is a canonical form, not a style preference.** `format(parse(source)) == format(parse(format(parse(source))))` always.
- **The source is the authority.** No downstream tool re-parses raw source to recover public API facts. The pipeline is: source → package interface → conformance manifest → publication manifest → pubdiff.

### Versioning

The implementation follows a `v0.47.x` lineage. Significant slices:

- `v0.47.23`: interpreter arrived
- `v0.47.38`: normalised imported-contract layer
- `v0.47.39`: diagnostic code stability (`E_*`, `R_TRAP_*`)
- `v0.47.47`: match-expression CFG branching (T2-A)
- `v0.47.53–54`: publication manifests, package interfaces, pubdiff
- `v0.47.57`: public resource/protocol shapes in package interfaces
- `v0.47.58`: transitive dependency publication closure
- `v0.47.59`: `catch |e| block` binding form; canonical publication digest (`pubdigest`)

---

## Running the tests

```bash
# Full unit and integration suite
PYTHONPATH=. pytest tests/ --ignore=tests/test_conformance_runner.py -q

# Conformance suite (slower, subprocess-based)
PYTHONPATH=. pytest tests/test_conformance_runner.py -q

# Targeted
PYTHONPATH=. pytest tests/test_interpreter_traps.py -q
PYTHONPATH=. pytest tests/test_dependency_closure_v47_58.py -q
```

All 600 unit/integration tests and 368 conformance tests should pass on a clean checkout.

---

## Status

AxiomZig v1.1 is a **reference front-end** — not a production compiled language. It is strongest as:

- A resource-safety / protocol-state language suitable for writing small systems API specifications
- A compiler/conformance target for testing static analysis tools
- A pre-pretraining corpus language for training models on ownership reasoning, protocol obligations, effect summaries, and package-API compatibility

What is not yet present: a native code backend, a real standard library with host I/O, generics, floats, concurrency. These are v2 items.

---

## Deferred design

Explicitly deferred to v2:

- `pub module` / `pub struct` / `export` — module-level visibility beyond functions and resources
- Non-lexical lifetime inference
- Generics and type parameters
- Floating-point types
- Concurrency primitives
- Self-hosted compiler

---

## License

MIT. See [LICENSE](LICENSE).
