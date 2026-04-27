# AxiomZig 500-Bundle Pre-Pretraining Corpus Design

**Document status:** living specification  
**Target corpus family:** AxiomZig pre-pretraining / semantic compiler corpus  
**Baseline compiler target:** `v47.59` and forward  
**Design principle:** build a Dolma-like curated data ecosystem — not a pile of `.az` files.

---

## 1. Purpose

The AxiomZig corpus trains language models on structured semantic reasoning before or alongside conventional code and natural-language pretraining. Every example is grounded in a real, deterministic compiler — not written by hand or approximated. The label is the compiler output.

The target model capabilities, in ascending order of difficulty:

1. Parse and format a strict systems-language syntax.
2. Track ownership, moves, borrows, aliases, and resource lifetimes.
3. Reason over resource protocols as finite-state machines.
4. Propagate effect summaries through local and whole-program control flow.
5. Interpret deterministic programs and predict runtime traps.
6. Use import summaries, dependency manifests, and package interfaces.
7. Classify public API compatibility and publication manifest diffs.
8. Recognize sharp semantic boundaries from contrastive examples.
9. Compose reasoning across long, multi-module, multi-feature programs.

The corpus must support models from roughly **300M to 2T parameters**. It therefore includes small examples, medium semantic tasks, long compositional cases, and artifact-prediction tasks. Small models learn syntax and local diagnostics; large models learn whole-program reasoning and publication-level API semantics.

---

## 2. Core Lesson from Dolma

Dolma treats a corpus as a **curation pipeline**, not a static artifact. Raw documents flow through taggers, deduplication, filters, and mixer recipes to produce versioned training shards. The key infrastructure decisions from Dolma that apply here:

- **Documents are atoms.** Every example is a JSONL record with a stable ID, a `text` field, a `source` field, and a `metadata` block. The pipeline operates on records, not on files in a zip.
- **Attributes are separate from documents.** What a tagger or validator *measures* about a document lives in an `attributes/` layer alongside the document. Labels (what the generator *intended*) and attributes (what the compiler *observed*) are explicitly separated.
- **Filters are executable expressions.** Mix recipes are not prose descriptions — they are jsonpath or jq expressions evaluated against attributes, producing include/exclude decisions.
- **IDs are stable.** A document ID assigned in bundle 001 refers to that exact document forever. Regeneration of a variant produces a new ID, not a replacement.
- **Deduplication is a first-class pass.** Exact, formatted-source, and semantic-skeleton deduplication run before any example enters a training shard.

The AxiomZig corpus adopts all of these conventions.

---

## 3. Canonical Document Format

Every example in the corpus is represented as a JSONL record conforming to the Dolma document schema:

```json
{
  "id": "axiomzig_0042__ownership_join_017",
  "text": "<full contents of the .az source file>",
  "source": "axiomzig_prepretrain_v47_59",
  "added": "2025-04-27",
  "created": "2025-04-27",
  "metadata": {
    "bundle_id": "axiomzig_prepretrain_corpus_0042",
    "stream": "ownership_moves",
    "label": { ... },
    "view": "source_only"
  }
}
```

The `text` field always contains the AxiomZig source file. For multi-artifact views (e.g. `old_new_manifest_to_pubdiff`), the `text` field contains the concatenated input and the `metadata.target` field contains the expected output. See section 10 for view construction rules.

### 3.1 ID Stability Policy

Once an example ships in any bundle with a given ID, that ID refers to that exact document forever. The ID is constructed as:

```
{bundle_id}__{example_slug}
```

where `example_slug` is derived from the source SHA-256 (first 16 hex characters) and a human-readable descriptor:

```
axiomzig_0042__own_join_a3f7b2c91d4e8f01
```

If a generator produces a variant of an existing example — even a one-token change — it receives a new slug. No example is ever silently replaced. The source hash deduplication pass (section 12) catches accidental re-entry.

---

## 4. Labels vs. Attributes

This is the most important structural distinction in the corpus design.

```
labels/      what the generator intended
attributes/  what the compiler and analyzers actually measured
```

Generator labels are *declarations of intent*. They can be aspirational, or wrong due to generator bugs. Attributes are *ground truth* — the actual output of the AxiomZig compiler, formatter, checker, interpreter, and publication toolchain run against each example at a specific compiler version.

Training should filter on attributes, not labels. Labels are useful provenance metadata. They should never be used as the final authority on an example's validity.

### 4.1 Label Schema (v2)

```json
{
  "schema_version": "axiomzig-corpus-label-v2",
  "corpus_id": "axiomzig_prepretrain_corpus_0042",
  "example_id": "axiomzig_0042__own_join_a3f7b2c91d4e8f01",
  "source_path": "src/ownership/ownership_join_017.az",
  "source_sha256": "a3f7b2c91d4e8f0112345678...",
  "generator": {
    "name": "axiomzig_corpus_gen_v0042",
    "version": "v47.59",
    "lineage": ["stream_ownership_v3", "template_branch_join"],
    "template_id": "branch_join_resource_state",
    "parameters": {
      "protocol": "file",
      "branch_count": 2,
      "resource_count": 1
    }
  },
  "semantics": {
    "areas": ["ownership", "protocols"],
    "concepts": ["branch_join", "resource_terminal_state", "if_else"]
  },
  "task": {
    "phase": "check",
    "expected_outcome": "fail",
    "expected_diagnostic_codes": ["E_OWN_UNDISCHARGED"]
  },
  "training": {
    "curriculum": "ownership_protocol_join_v1",
    "difficulty": "medium",
    "split": "train",
    "contrastive_pair_id": "join_pair_017",
    "positive": false
  },
  "validation_status": {
    "mode": "current_v47_59",
    "validated_by": "checker",
    "notes": ""
  }
}
```

### 4.2 Attribute Schema (v1)

Attributes are written by the validator, not the generator. They record what the compiler actually did.

```json
{
  "schema_version": "axiomzig-validation-attribute-v1",
  "corpus_id": "axiomzig_prepretrain_corpus_0042",
  "example_id": "axiomzig_0042__own_join_a3f7b2c91d4e8f01",
  "source_path": "src/ownership/ownership_join_017.az",
  "compiler": {
    "version": "v47.59",
    "commit": null
  },
  "parse": {
    "ok": true,
    "diagnostics": []
  },
  "format": {
    "idempotent": true,
    "formatted_sha256": "b4a9c1..."
  },
  "check": {
    "ok": false,
    "diagnostics": ["E_OWN_UNDISCHARGED"],
    "diagnostic_count": 1
  },
  "interpret": {
    "ran": false,
    "outcome": null,
    "output": null,
    "trap": null
  },
  "conform": {
    "ran": false,
    "ok": null
  },
  "ownership": {
    "resource_count": 1,
    "borrow_count": 0,
    "move_count": 0,
    "join_points": 1
  },
  "quality": {
    "line_count": 38,
    "source_token_estimate": 190,
    "semantic_skeleton_hash": "f9e3a2...",
    "near_duplicate_cluster": null,
    "semantic_skeleton_cluster_size": 1
  },
  "label_agreement": {
    "phase_matches": true,
    "outcome_matches": true,
    "diagnostic_codes_match": true,
    "agreement": "full"
  }
}
```

The `label_agreement` block is the poison detector. `agreement: "full"` means the compiler confirmed what the generator promised. `agreement: "diagnostic_mismatch"` means the generator said `E_OWN_UNDISCHARGED` but the compiler emitted `E_OWN_MOVED` — the label is wrong and the example is quarantined. `agreement: "outcome_mismatch"` means the generator said `fail` but the checker passed — the most dangerous case, quarantined immediately.

---

## 5. Validation Status Modes

Every example carries a `validation_status.mode`:

| Mode | Meaning |
|------|---------|
| `current_v47_59` | Validated against the active compiler. Ground truth. |
| `future_target` | Intended future semantics; not yet compiler-grounded. |
| `sidecar_required` | Requires additional import/package/pubdiff sidecars to validate. |
| `schema_only` | Structurally valid JSON/AZ but compiler was not run. |
| `rejected` | Generated but excluded. Reason recorded in `rejected_candidates.json`. |

**For production training mixes, use `current_v47_59` only.**

### 5.1 Future-Target Graduation

`future_target` examples graduate to `current_v47_59` when:

1. The validator runs the active compiler against the example.
2. The compiler confirms the expected outcome.
3. The compiler version is recorded in the attribute.
4. The attribute `label_agreement.agreement` is `full`.

Graduation is triggered by a compiler version bump. Any bundle containing `future_target` examples must be re-validated when the compiler advances. Examples that do not graduate within two compiler versions are reclassified as `obsolete_semantics`.

---

## 6. Semantic Skeleton Hash

The semantic skeleton hash is the corpus's primary structural deduplication tool. It catches programs that differ only in cosmetic ways — different constants, different variable names, same logical structure.

### 6.1 Algorithm

1. Format the source using the active compiler formatter.
2. Strip the module declaration line.
3. Replace all identifier tokens (function names, variable names, type parameters, field names) with positional placeholders `$0`, `$1`, `$2`, ... in order of first appearance. Protocol names, resource names, and error set names are replaced together with their usage sites.
4. Replace all integer literals with `N`.
5. Replace all string literals with `S`.
6. Remove all comments.
7. Normalize whitespace to single spaces.
8. SHA-256 the result.

### 6.2 Near-Duplicate Policy

| Condition | Policy |
|-----------|--------|
| Same skeleton + same label + same outcome | Structural duplicate. Keep one, quarantine rest. |
| Same skeleton + different outcome | Contrastive candidate. Keep both, tag as pair. |
| Same skeleton + different public artifact | Publication candidate. Keep both, tag as pair. |
| Skeleton cluster size > 20 | Over-generated template. Flag for review. Retain at most 20 per cluster. |

The `semantic_skeleton_cluster_size` attribute enables the mixer to filter by cluster size.

---

## 7. Trainable Views

A single source file produces multiple training records — one per view. Each view has a specific text construction and a specific prediction target. The view is recorded in `metadata.view`.

| View | `text` field | `metadata.target` |
|------|-------------|-------------------|
| `source_only` | Source text | *(none)* |
| `source_plus_label` | Source + label JSON | *(none)* |
| `source_to_diagnostic` | Source text | `{"phase": "check", "outcome": "fail", "codes": [...]}` |
| `source_to_checker_result` | Source text | `{"ok": true/false, "diagnostics": [...]}` |
| `source_to_interpreter_output` | Source text | `{"output": "42"}` |
| `source_to_runtime_trap` | Source text | `{"trap": "R_TRAP_OVERFLOW"}` |
| `source_to_ownership_summary` | Source text | `<ownsig JSON output>` |
| `source_to_package_interface` | Source text | `<package interface JSON>` |
| `source_to_publication_manifest` | Source text | `<publication manifest JSON>` |
| `old_new_manifest_to_pubdiff` | `<old manifest>\n---\n<new manifest>` | `{"classification": "incompatible", "issue_codes": [...]}` |
| `contrastive_pair_to_boundary` | `<program A>\n---\n<program B>` | `{"differs_in": "...", "a_outcome": "...", "b_outcome": "..."}` |
| `workspace_to_dependency_closure` | Concatenated workspace sources | `<closure JSON>` |

Views are generated by the attribute extractor after the compiler runs. A source file that fails to parse produces only `source_only` and `source_to_diagnostic` views — the other views require a valid parse tree.

---

## 8. Release Layout

A complete release has this structure:

```
axiomzig-corpus-v47_59/
  documents/               Dolma-format JSONL records (per-stream, gzipped)
    ownership_moves/
      0001.jsonl.gz
      ...
    protocols_resources/
      ...

  attributes/              Measured attributes (parallel to documents/)
    ownership_moves/
      axiomzig_validator_v1/
        0001.jsonl.gz
      axiomzig_skeleton_v1/
        0001.jsonl.gz

  bundles/                 Self-contained numbered zips (delivery format)
    corpus001.zip
    corpus002.zip
    ...
    corpus500.zip

  mixes/                   Executable mixer recipes
    phase0_structural.yaml
    phase1_semantic.yaml
    phase2_code_blend.yaml
    phase3_late_reasoning.yaml
    eval_heldout.yaml

  global/
    dedup_index.bloom       Bloom filter of all known source hashes
    skeleton_index.jsonl    Skeleton hash → cluster size mapping
    token_count.jsonl       Per-stream token counts
    compiler_version_matrix.jsonl

  reports/
    corpus_card.md
    mixture_card.md
    heldout_eval_card.md
    generation_ledger.md

  schemas/
    label_v2.schema.json
    validation_attribute_v1.schema.json
    mix_recipe.schema.json
    bundle_manifest.schema.json
    document.schema.json
```

The `documents/` tree is the Dolma-format output consumed by the mixer and tokenizer. The `bundles/` tree is the delivery format — self-contained zips for individual download and inspection.

### 8.1 Bundle Internal Layout

Every numbered bundle zip contains:

```
src/                         AxiomZig source files
labels/                      Generator-authored labels
attributes/                  Measured compiler/analyzer attributes
packages/                    Package-interface / publication / pubdiff artifacts
ledgers/ledger.jsonl
ledgers/ledger.csv
ledgers/source_hashes.sha256.txt
validation/validation_report.json
validation/rejected_candidates.json
reports/quality_report.md
reports/expansion_strategy.md
schemas/label_v2.schema.json
schemas/validation_attribute_v1.schema.json
tools/validate_corpus_vXXXX.py
tools/axiomzig_corpus_gen_vXXXX.py
MANIFEST.json
README.md
```

A bundle does not count toward the 500-bundle target unless it passes all validation gates.

---

## 9. 500-Bundle Macro-Plan

Generate 500 bundles as **10 streams × variable bundle allocation**, weighted by pedagogical importance.

| Stream | Bundles | Purpose |
|--------|--------:|---------|
| S1. Syntax + formatting | 35 | Parser, formatter, canonicalization, malformed examples |
| S2. Types + name resolution | 45 | Structs, enums, options, errors, imports, module graphs |
| S3. Ownership + moves | 65 | Moves, borrows, aliasing, reinit, partial moves, joins |
| S4. Protocol / resource state | 55 | Terminal states, multi-terminal, illegal transitions |
| S5. Effects + summaries | 45 | Declared effects, propagation, imported effects, CFG |
| S6. Runtime / interpreter | 45 | Output, traps, stubs, runtime mutation |
| S7. Whole-program / import graphs | 55 | Cross-module summaries, dependencies, stale/conflict |
| S8. Publication / package / pubdiff | 65 | Source → interface → manifest → pubdiff |
| S9. Contrastive semantic boundaries | 55 | One-token/one-line semantic boundary cases |
| S10. Long compositional programs | 35 | 100–500 line multi-feature programs |
| **Total** | **500** | |

This is a **generation target**, not a training mix. Section 11 specifies mixing.

---

## 10. Generation Distribution

Use this allocation when generating raw examples:

| Stream | Target share |
|--------|-------------:|
| Syntax/formatting | 7% |
| Types/resolution | 9% |
| Ownership/moves | 13% |
| Protocols/resources | 11% |
| Effects/summaries | 9% |
| Runtime/interpreter | 9% |
| Whole-program/import graphs | 11% |
| Publication/package/pubdiff | 13% |
| Contrastive overlays | 11% |
| Long compositional programs | 7% |

Generation needs breadth and coverage. Training mixes using the recipes in section 11 can later oversample or undersample independent of generation ratios.

**Coverage enforcement:** a bundle generator must refuse to close a bundle until every spec diagnostic code (listed in appendix A) has at least 8 examples in the bundle or the bundle explicitly annotates the gap in its quality report. Templates that generate the same structural pattern with only constants varying are counted as one example for coverage purposes.

---

## 11. Stream Specifications

### S1. Syntax + Formatting

Generate:
- Valid minimal modules
- Invalid module headers (reserved-word segments, malformed braces)
- Formatter idempotence probes
- Comments and whitespace normalization
- Bad annotations (`@experimental` on non-pub fn, duplicate `@since`)
- Parser recovery examples (malformed then valid)

Primary phases: `parse`, `format`

Views: `source_only`, `source_to_parse_outcome`, `source_to_formatter_output`, `malformed_to_diagnostic`

---

### S2. Types + Name Resolution

Generate:
- Struct, enum, option, error union declarations
- Name shadowing (rejected), duplicate symbols (rejected)
- Type mismatch cases: wrong return type, wrong argument type, wrong match arm type
- Non-exhaustive match (option missing `none`, enum missing variant)
- Unresolved names, unresolved types, unresolved module imports
- Cyclic imports, missing module

Primary phases: `parse`, `resolve`, `typecheck`, `check`

Views: `source_to_type_diagnostic`, `source_to_resolved_symbol_map`, `contrastive_type_boundary`

---

### S3. Ownership + Moves

Generate:
- Simple move, double move, use-after-move
- Borrow-while-moved, move-while-borrowed
- Exclusive borrow blocking shared borrow
- Partial move from struct (field-level ownership)
- Field reinitialization (move-then-reinit pattern)
- `Ref<T>` mutation, `RefConst<T>` observation
- Join-state ownership: both branches discharge, one branch misses
- Loop-carried ownership: resource created and discharged per iteration
- `errdefer` ownership cleanup: discharges on error, explicit on success
- Missing discharge in early-return paths

Primary phases: `check`, `owncheck`, `conform`

Views: `source_to_ownership_diagnostic`, `source_to_ownership_summary`, `valid_invalid_contrastive_pair`

Target: one of the two largest streams. At least 40% of examples should be negative (checker rejects).

---

### S4. Protocol / Resource State

Generate:
- Single terminal state, multiple terminal states
- Illegal transition: double-close, skip-intermediate-state
- Branch terminal mismatch: one branch discharges, one does not
- Loop resource discharge: each iteration creates and discharges
- Resource inside struct (both fields tracked)
- Resource inside `Option<T>`
- Protocol state through imported call (Ref<T> holder pattern)
- Multi-step protocols: A → B → C required, A → C illegal

Primary phases: `check`, `owncheck`, `conform`

Views: `source_to_protocol_state_trace`, `source_to_terminal_state_prediction`, `source_to_diagnostic`

---

### S5. Effects + Summaries

Generate:
- Pure function, declared single effect, declared multiple effects
- Missing effect: caller lacking `protocol_step`, `io`, `error`, `free`, `recurse`
- Transitive effect: leaf → mid → top propagation
- Imported effect: cross-module effect summary consumed
- Effect at CFG join: both branches declare, one branch lacks
- Missing `recurse` in direct and mutual recursion
- `trap` declared as effect (always rejected)
- Effect mismatch in public API: old had `io`, new does not

Primary phases: `check`, `conform`, `pubdiff`

Views: `source_to_effect_summary`, `source_to_effect_diagnostic`, `old_new_effect_signature_diff`

---

### S6. Runtime / Interpreter

Generate:
- Deterministic output: arithmetic, recursion, loops, match, option, enum dispatch
- Integer traps: overflow (I64, I32, U8), underflow, unary minus on MIN
- Division-by-zero, modulo-by-zero
- Cast out-of-range: I64 → I8, I64 → U8, negative → U64
- Protocol transition trap at runtime: double-close, skip-step
- Read-of-moved-value trap
- Stubbed imported call: correct stub returns expected value, missing stub traps
- `Ref<T>` mutation through runtime stub

**Target runtime negative ratio: ≥ 35% trap examples.** Current corpus (0003) is 5% — this must be corrected immediately. Trap examples are uniquely valuable because they require the model to reason about *when* safety invariants are violated, not just *that* they can be.

Primary phases: `interpret`, `check`

Views: `source_to_output`, `source_to_runtime_trap`, `source_to_trace`, `stubbed_import_to_runtime_result`

---

### S7. Whole-Program / Import Graphs

Generate:
- Multi-module import graphs (linear chain, diamond, star)
- Dependency manifests: stale summary, conflicting summary, missing module
- Duplicate module name, import cycle detection
- External opaque import vs. internal module import
- Cross-module ownership summary: resource returned across boundary
- Cross-module effect summary: callee's effects propagate to caller
- Auto-discovery mode (no `--module-root`) vs. explicit mode

This stream is where AxiomZig becomes substantially more valuable than toy examples. A model trained on cross-module reasoning can generalize to real multi-file codebases.

Primary phases: `check`, `conform`, `emit-package-interface`

Views: `workspace_to_manifest`, `source_to_import_graph`, `source_to_dependency_diagnostic`, `source_to_cross_module_summary`

---

### S8. Publication / Package / Pubdiff

Generate pairs of old and new publication manifests representing:
- Public function added → `compatible`
- Public function removed → `incompatible`
- Signature changed (parameter type) → `incompatible`
- Effect set narrowed in public function → `incompatible`
- Effect set widened in public function → `compatible`
- Ownership signature changed → `incompatible`
- Resource field added → `compatible`
- Resource field removed → `incompatible`
- Protocol terminal state added → `compatible`
- Protocol transition removed → `incompatible`
- Dependency lock changed → recorded, not classified
- Dependency public API removed transitively → `incompatible` (via closure)
- `@deprecated` annotation added → `metadata-only`
- `@since` annotation added → `metadata-only`
- Metadata-only change → `compatible`

For each: generate the full source → emit-package-interface → conform → pubdiff pipeline. Label the pubdiff classification. This stream requires the publication toolchain to be fully exercised, not just the checker.

Primary phases: `emit-package-interface`, `conform`, `pubdiff`, `pubdigest`

Views: `source_to_package_interface`, `source_to_publication_manifest`, `old_new_manifest_to_pubdiff`, `old_new_manifest_to_issue_codes`

---

### S9. Contrastive Semantic Boundaries

Contrastive examples appear in every stream, but this stream concentrates them. Every example here is a pair where a minimal change (ideally one token, at most one line) flips validity, changes a diagnostic, or changes the pubdiff classification.

Generate pairs for:
- One missing `r.close()` call on one branch → undischarged vs. discharged
- One missing effect declaration → `E_EFFECT_UNDER` vs. pass
- One extra move → `E_OWN_MOVED` vs. pass
- One borrow retained past its natural end → borrow conflict vs. pass
- One transition removed from protocol → illegal transition vs. pass
- One missing `?` on error return → missing error effect vs. propagated
- One type changed (I64 → Bool) → type mismatch vs. pass
- Old manifest with function removed vs. same function present → incompatible vs. compatible
- Same skeleton, different ownership outcome → documents a precise semantic boundary

Do not treat contrastive pairs as merely extra examples. They are the highest-value examples per token in the entire corpus. A model that can predict both members of a contrastive pair has internalized the precise boundary of a semantic rule. A model that can only predict one member has learned a tendency.

Primary phases: all

Views: `contrastive_pair_to_boundary`, `pair_to_validity_boundary`, `pair_to_minimal_edit_explanation`

---

### S10. Long Compositional Programs

Generate programs of 100–500 lines that combine multiple features:

- Resource protocol + effects + error propagation in one function
- Multi-module workspace: leaf defines resource, mid provides helpers, app closes
- `defer`/`errdefer` cleanup across multiple resources with different protocols
- Runtime program that uses structs, enums, option chaining, and recursive functions
- Full package graph: source → package interface → publication manifest → pubdiff

Long examples prevent the model from learning only local, narrow patterns. A model that has only seen 10-line programs will fail to reason about a 200-line program even if every sub-pattern is familiar.

Target: at least 5k long programs (≥ 100 lines) in the complete corpus.

Primary phases: `parse`, `check`, `owncheck`, `interpret`, `conform`, `pubdiff`

Views: `source_only`, `source_to_summary`, `source_to_manifest`, `source_to_diagnostic`, `workspace_to_publication_bundle`

---

## 12. Mixer Recipes

Mix recipes are executable YAML configurations evaluated against the attribute layer. They are not prose proportions.

### 12.1 Phase 0: Structural Warmup

Establish syntax, formatting, basic types. Heavy on parse/format examples.

```yaml
mix_name: axiomzig_v47_59_phase0_structural
streams:
  - name: s1_syntax_formatting
    documents: documents/syntax_formatting/**/*.jsonl.gz
    attributes:
      - axiomzig_validator_v1
      - axiomzig_skeleton_v1
    output:
      path: mixes/phase0/syntax_formatting
      max_size_in_bytes: 268435456
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
        - "$.attributes[?(@.axiomzig_validator_v1.validation_status == 'current_v47_59')]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 20)]"
        - "$.attributes[?(@.axiomzig_validator_v1.quality.source_token_estimate < 10)]"

  - name: s2_types_resolution
    documents: documents/types_resolution/**/*.jsonl.gz
    attributes:
      - axiomzig_validator_v1
      - axiomzig_skeleton_v1
    output:
      path: mixes/phase0/types_resolution
      max_size_in_bytes: 268435456
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
        - "$.attributes[?(@.axiomzig_validator_v1.validation_status == 'current_v47_59')]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 20)]"

  - name: s6_runtime_small
    documents: documents/runtime_interpreter/**/*.jsonl.gz
    attributes:
      - axiomzig_validator_v1
    output:
      path: mixes/phase0/runtime_small
      max_size_in_bytes: 134217728
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
        - "$.attributes[?(@.axiomzig_validator_v1.quality.line_count < 25)]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 10)]"
```

### 12.2 Phase 1: Semantic Core

Emphasize ownership, protocols, effects. Raise negative ratio. Require deduplication at skeleton level.

```yaml
mix_name: axiomzig_v47_59_phase1_semantic
streams:
  - name: s3_ownership_moves
    documents: documents/ownership_moves/**/*.jsonl.gz
    attributes:
      - axiomzig_validator_v1
      - axiomzig_skeleton_v1
    output:
      path: mixes/phase1/ownership_moves
      max_size_in_bytes: 536870912
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
        - "$.attributes[?(@.axiomzig_validator_v1.validation_status == 'current_v47_59')]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 5)]"
        - "$.attributes[?(@.axiomzig_validator_v1.quality.source_token_estimate < 15)]"

  - name: s4_protocols_resources
    documents: documents/protocols_resources/**/*.jsonl.gz
    attributes:
      - axiomzig_validator_v1
      - axiomzig_skeleton_v1
    output:
      path: mixes/phase1/protocols_resources
      max_size_in_bytes: 536870912
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
        - "$.attributes[?(@.axiomzig_validator_v1.validation_status == 'current_v47_59')]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 5)]"

  - name: s5_effects_summaries
    documents: documents/effects_summaries/**/*.jsonl.gz
    attributes:
      - axiomzig_validator_v1
    output:
      path: mixes/phase1/effects_summaries
      max_size_in_bytes: 268435456
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 8)]"

  - name: s9_contrastive
    documents: documents/contrastive_boundaries/**/*.jsonl.gz
    attributes:
      - axiomzig_validator_v1
    output:
      path: mixes/phase1/contrastive
      max_size_in_bytes: 268435456
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
        - "$.metadata[?(@.training.contrastive_pair_id != null)]"
```

### 12.3 Phase 2: Code Blend

Introduce whole-program and publication reasoning. Broaden to long programs.

```yaml
mix_name: axiomzig_v47_59_phase2_code_blend
streams:
  - name: s7_whole_program
    documents: documents/whole_program_imports/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1, axiomzig_skeleton_v1]
    output:
      path: mixes/phase2/whole_program
      max_size_in_bytes: 536870912
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 3)]"

  - name: s8_publication
    documents: documents/publication_pubdiff/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1]
    output:
      path: mixes/phase2/publication
      max_size_in_bytes: 536870912
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"

  - name: s6_runtime_traps
    documents: documents/runtime_interpreter/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1]
    output:
      path: mixes/phase2/runtime_traps
      max_size_in_bytes: 268435456
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.interpret.trap != null)]"
```

### 12.4 Phase 3: Late Reasoning

Long compositional programs, high contrastive density, minimal syntax warmup.

```yaml
mix_name: axiomzig_v47_59_phase3_late_reasoning
streams:
  - name: s10_long_programs
    documents: documents/long_compositional/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1]
    output:
      path: mixes/phase3/long_programs
      max_size_in_bytes: 1073741824
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.quality.line_count >= 100)]"
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"

  - name: s9_contrastive_hard
    documents: documents/contrastive_boundaries/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1]
    output:
      path: mixes/phase3/contrastive_hard
      max_size_in_bytes: 268435456
    filter:
      include:
        - "$.metadata[?(@.training.difficulty == 'hard')]"
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"

  - name: s8_publication_reasoning
    documents: documents/publication_pubdiff/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1]
    output:
      path: mixes/phase3/publication_reasoning
      max_size_in_bytes: 536870912
    filter:
      include:
        - "$.metadata[?(@.view == 'old_new_manifest_to_pubdiff')]"
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
```

---

## 13. Deduplication

Three levels, applied in sequence.

### 13.1 Exact Source Hash

Reject exact duplicates. SHA-256 of raw source bytes, compared against a global Bloom filter (`global/dedup_index.bloom`). The filter is updated after each bundle is accepted.

### 13.2 Formatted Source Hash

Format the source using the active compiler formatter. SHA-256 the result. Reject if the formatted hash matches any existing entry. This catches programs that are logically identical but formatted differently.

### 13.3 Semantic Skeleton Hash

Apply the skeleton algorithm from section 6. Compare against the skeleton index (`global/skeleton_index.jsonl`). Reject if the cluster already has 20+ members. Tag with `cluster_size` for use in mixer filters.

---

## 14. Validation Gates

Every bundle must pass all gates before acceptance.

### 14.1 Structural Gates

```
zip integrity (zipfile.testzip() == None)
all required files present
all JSON files parse without error
all labels validate against label_v2.schema.json
all attributes validate against validation_attribute_v1.schema.json
source hash uniqueness (no collision with global dedup index)
formatted-source hash uniqueness
```

### 14.2 Compiler-Grounding Gates

```
parse result matches labels for all current_v47_59 examples
check result matches labels for all current_v47_59 examples
interpret result matches labels for all current_v47_59 examples
conform result matches labels for all current_v47_59 examples
pubdiff result matches labels for all current_v47_59 examples
label_agreement.agreement == 'full' for all accepted examples
```

### 14.3 Quality Gates

```
no semantic skeleton cluster size > 20
near-duplicate rate < 30% of bundle
diagnostic code coverage: all 37 spec codes covered or gap explicitly reported
heldout examples not present in training split (skeleton-hash check against eval index)
no outcome_mismatch or diagnostic_mismatch entries in attributes
rejected_candidates.json exists and lists all quarantined examples
```

### 14.4 Validation Report Format

```json
{
  "corpus_id": "axiomzig_prepretrain_corpus_0042",
  "compiler_version": "v47.59",
  "source_records": 640,
  "parse":     {"pass": 640, "fail": 0},
  "format":    {"idempotent": 640, "non_idempotent": 0},
  "check":     {"pass": 440, "fail": 180, "quarantined": 20},
  "interpret": {"output": 100, "trap": 60, "unsupported": 10, "not_run": 470},
  "conform":   {"pass": 200, "fail": 0, "not_run": 440},
  "pubdiff":   {"pass": 120, "fail": 0, "not_run": 520},
  "label_agreement": {
    "full": 620,
    "outcome_mismatch": 0,
    "diagnostic_mismatch": 0,
    "quarantined": 20
  },
  "deduplication": {
    "exact_duplicates_rejected": 3,
    "formatted_duplicates_rejected": 1,
    "skeleton_clusters_capped": 2,
    "skeleton_cluster_sizes": {"1": 580, "2–5": 55, "6–20": 5}
  },
  "diagnostic_coverage": {
    "codes_present": 35,
    "codes_missing": ["E_OWN_ALIAS_MISSING", "E_TYPE_CAST"],
    "coverage_percent": 94.6
  },
  "accepted": true,
  "acceptance_notes": "Two diagnostic codes below minimum; recorded in quality_report.md."
}
```

---

## 15. Bundle Acceptance and Poison Policy

### 15.1 Acceptance

A bundle is accepted when all structural, compiler-grounding, and quality gates pass.

### 15.2 Quarantine

A bundle is quarantined (not rejected outright) when:

- Compiler-grounded labels disagree with actual compiler outputs (`outcome_mismatch` or `diagnostic_mismatch`)
- Unknown diagnostics appear not recorded in the quality report
- Near-duplicate rate exceeds 30%
- Heldout leakage is detected

Quarantined bundles are not counted toward the 500-bundle target. Their source files are preserved, their labels are corrected against the actual compiler output, and they are re-submitted as new bundles.

### 15.3 Poison Example Definition

A **poison example** is any record where the attribute `label_agreement.agreement` is not `full` and the record was not quarantined before acceptance. This is the most dangerous failure mode — a training example with wrong ground truth. The validator must make this impossible: `label_agreement.agreement != 'full'` is a hard gate that blocks acceptance.

Additional poison patterns to detect:

- `expected_outcome: fail` but `check.ok: true` — the label says reject but the compiler accepted. Most dangerous: trains the model to predict errors on valid programs.
- `expected_outcome: pass` but `check.ok: false` — trains the model to accept erroneous programs.
- Two validation modes conflict: `check.ok: true` but `interpret.trap != null` — structurally valid but runtime-unsafe. Tag as `runtime_trap_despite_clean_check` rather than poisoning; these are valuable examples if labeled correctly.

---

## 16. Token Budget and Oversampling

### 16.1 Raw Token Estimates

Bundle 0006 measured approximately:
- Source-only: ~33k BPE-approximate tokens
- Full artifacts: ~550k BPE-approximate tokens

Projecting to 500 bundles with increasing example length in later stages:

| Measure | Estimate |
|---------|---------|
| Source-only | 15M–25M tokens |
| Full artifacts | 150M–300M tokens |

### 16.2 Oversampling Strategy

These numbers are small relative to a standard pretraining run. The corpus is designed to be **oversampled** in a dedicated pre-pretraining phase before standard data is introduced, or oversampled within a code-blend phase.

Recommended oversampling by phase:

| Phase | Streams | Effective token target | Oversampling factor |
|-------|---------|----------------------|---------------------|
| Phase 0 | S1, S2, S6 (small) | 300M tokens | 10–20× |
| Phase 1 | S3, S4, S5, S9 | 1B–2B tokens | 15–30× |
| Phase 2 | S7, S8, S6 (traps) | 1B–2B tokens | 10–20× |
| Phase 3 | S10, S9 (hard), S8 | 500M–1B tokens | 5–10× |

The oversampling factor is applied within a curriculum window, not globally. A 2T parameter model trained with a 15T-token run might see AxiomZig for 0.2–1% of total tokens, oversampled within a code-specific phase. A 300M parameter model trained specifically for static-analysis tasks might weight AxiomZig at 20–40% of all pretraining data.

---

## 17. Compiler Drift Policy

The corpus is version-anchored. When the compiler advances:

1. Re-run the validator against all `current_v47_59` examples.
2. For each example, produce a migration record:

```json
{
  "example_id": "axiomzig_0042__own_join_a3f7b2c91d4e8f01",
  "old_version": "v47.59",
  "new_version": "v47.60",
  "status": "still_valid | changed_diagnostic | changed_output | parser_rejected | obsolete_semantics",
  "old_attribute_sha256": "...",
  "new_attribute_sha256": "..."
}
```

3. Update `validation_status.mode` in the attribute.
4. Emit a `compiler_version_matrix.jsonl` entry recording which examples changed.
5. Do not silently relabel old bundles. Old bundles are immutable. The migration layer is additive.

This turns the corpus into a regression suite for the compiler itself, in addition to a training dataset.

---

## 18. Evaluation Suite

The eval suite is built from day one and never contaminated with training data.

### 18.1 Construction

Hold out 5% of all generated examples before they enter any training bundle. Selection is stratified by stream and difficulty. Heldout examples are stored in `mixes/eval_heldout.yaml` and their skeleton hashes are added to the eval index. The mixer poison-checks every training bundle against this index.

### 18.2 Eval Tasks

| Task | Input | Target | Metric |
|------|-------|--------|--------|
| Parse validity | Source text | pass / fail | accuracy |
| Formatter idempotence | Source text | formatted source | exact match |
| Ownership diagnosis | Source text | diagnostic code(s) | code-level F1 |
| Protocol-state prediction | Source text + resource | terminal / non-terminal | accuracy |
| Effect summary prediction | Source text | effect set | set F1 |
| Runtime output | Source text | output value | exact match |
| Runtime trap prediction | Source text | trap type or none | accuracy |
| Import summary reasoning | Workspace sources | diagnostic or pass | accuracy |
| Package interface prediction | Source text | interface JSON | field-level match |
| Pubdiff classification | Old + new manifest | compatible / incompatible | accuracy |
| Pubdiff issue codes | Old + new manifest | issue code(s) | code-level F1 |
| Contrastive discrimination | Program A + Program B | which is valid, why | accuracy + explanation |
| Long-program summary | 200+ line source | checker result + top diagnostics | recall |

### 18.3 Decontamination

Before any training bundle is accepted, the validator computes skeleton hashes for all its examples and checks them against the eval index. Any example whose skeleton hash appears in the eval index is quarantined and excluded. This prevents a generator from accidentally producing a training example that is structurally equivalent to an eval example.

---

## 19. Implementation Sequence

### Stage A: Stabilize the Standard

1. Freeze bundle directory layout.
2. Freeze label schema v2 and attribute schema v1.
3. Implement semantic skeleton hashing algorithm exactly as specified in section 6.1.
4. Initialize global dedup Bloom filter and skeleton index.
5. Initialize eval index.
6. Make bundle 0006 the template bundle.
7. Write the corpus card template (section 20).

### Stage B: Build the Generator Framework

1. Factor generator into stream modules (S1–S10).
2. Require every generator to emit: source, label, expected phase, expected outcome, semantic concepts, difficulty, split.
3. Enforce diagnostic coverage per bundle at generation time.
4. Reject exact duplicates at generation time via Bloom filter.

### Stage C: Build the Validator / Attribute Extractor

1. Run parse, format, check, owncheck, interpret, conform, pubdiff.
2. Emit attributes per example.
3. Compute `label_agreement` for every example.
4. Compute skeleton hash and cluster membership.
5. Produce validation report.
6. Quarantine mismatches. Record in `rejected_candidates.json`.

### Stage D: Bundles 001–050 (Structural foundation)

Focus: syntax/formatting, types/resolution, ownership/protocol basics, small runtime examples, initial contrastive examples.

Goal: establish pipeline reliability and baseline diagnostic coverage.

### Stage E: Bundles 051–150 (Semantic core)

Focus: ownership/moves (full coverage), protocol/resource state (multi-terminal, loops), effects/summaries (CFG paths), runtime traps (raise to ≥ 35%).

Goal: build the corpus's most distinctive content.

### Stage F: Bundles 151–300 (Whole-program awareness)

Focus: whole-program imports, module graphs, dependency manifests, imported ownership and effect summaries, stale/conflict sidecars.

Goal: make the corpus whole-program aware.

### Stage G: Bundles 301–425 (Publication reasoning)

Focus: source → package interface → publication manifest → pubdiff compatibility → dependency closure → API lifecycle mutations.

Goal: train publication-level reasoning.

### Stage H: Bundles 426–500 (Robustness)

Focus: long compositional programs (100–500 lines), multi-module packages, mixed static/runtime/publication cases, adversarial heldout examples, dense contrastive suites.

Goal: make the corpus non-toy.

---

## 20. Corpus Card Template

Every release must include a `reports/corpus_card.md` with the following sections:

**Dataset name and version.** Corpus name, compiler version, date of release.

**Intended use.** What model capabilities this corpus is designed to teach. What scale of model it targets. What training phases it is designed for.

**Out-of-scope use.** This corpus is not a natural-language text corpus and should not be used as one. It is not a general-purpose code corpus — it contains one language. It is not a benchmark for human programming skill.

**Diagnostic code coverage.** A table of all 37 spec codes and the number of examples present for each. Any code with fewer than 8 examples is flagged.

**Deduplication statistics.** Exact duplicate rejections, formatted duplicate rejections, skeleton cluster size distribution, clusters capped at 20.

**Compiler version anchoring.** The compiler version against which all `current_v47_59` examples were validated. The date of validation.

**Stream composition.** Number of examples per stream, positive/negative ratio per stream.

**Known gaps.** Any structural patterns not yet represented. Any diagnostic codes with thin coverage. Any stream below its bundle target.

**Eval task definitions.** Brief specification of each eval task in section 18.2, including the metric and the heldout example count.

---

## 21. Summary

The 500-bundle AxiomZig corpus is a compiler-grounded, curated, versioned semantic data ecosystem.

The core commitments:

| Commitment | Why |
|-----------|-----|
| Compiler-grounded validation on every example | Labels can be wrong; the compiler cannot |
| Labels and attributes are separate | Intent and measurement are different things |
| Stable document IDs | Reproducibility and regression tracking require it |
| Dedup at source, formatted, and skeleton levels | Structural near-duplicates are noise, not signal |
| Executable mixer recipes with jsonpath filters | Prose proportions cannot be run; filters can |
| Diagnostic code coverage enforcement | A corpus missing 70% of spec codes teaches 30% of the language |
| ≥ 35% negative ratio per stream | Positive-only training produces models that never predict errors |
| ≥ 35% trap examples in runtime stream | The runtime stream's unique value is trap prediction |
| Contrastive pairs throughout | The highest-value examples per token in the corpus |
| Eval suite from day one, never contaminated | You cannot measure what you trained on |
| Compiler drift handled explicitly | The corpus is also a compiler regression suite |

The goal is not to teach AxiomZig syntax. The goal is to teach models to internalize compiler-like semantic structure: ownership, effects, protocols, runtime behavior, imports, package surfaces, and compatibility over time. Every design decision is in service of that goal.

---

## Appendix A: Spec Diagnostic Codes

All 37 codes that must appear in the corpus. Minimum 8 examples per code in a complete release.

| Code | Area | Description |
|------|------|-------------|
| `E_PARSE_UNEXPECTED` | syntax | Unexpected token |
| `E_PARSE_EXPECTED` | syntax | Expected token not found |
| `E_RESOLVE_SHADOW` | name_resolution | Forbidden name shadowing |
| `E_RESOLVE_UNBOUND` | name_resolution | Unresolved name |
| `E_RESOLVE_DUPLICATE` | name_resolution | Duplicate declaration |
| `E_RESOLVE_UNKNOWN_TYPE` | name_resolution | Unknown type name |
| `E_TYPE_MISMATCH` | typing | Type mismatch |
| `E_TYPE_NONUNIT` | typing | Non-Unit expression statement |
| `E_TYPE_NO_EXHAUST` | typing | Non-exhaustive match |
| `E_TYPE_ARM_INCOMPAT` | typing | Incompatible match arm types |
| `E_TYPE_ASSIGN_CONST` | typing | Assignment to immutable binding |
| `E_TYPE_ASSIGN_LVALUE` | typing | Non-lvalue assignment target |
| `E_TYPE_SLICE_INDEX` | typing | Slice index type error |
| `E_TYPE_ITER` | typing | Non-iterable in for-range |
| `E_TYPE_CAST` | typing | Invalid cast target type |
| `E_EFFECT_UNDER` | effects | Missing declared effect |
| `E_EFFECT_TRAP` | effects | `trap` declared as effect |
| `E_EFFECT_RECURSE` | effects | Recursive call without `recurse` |
| `E_EFFECT_UNKNOWN` | effects | Unknown effect name |
| `E_OWN_MOVED` | ownership | Use of moved resource |
| `E_OWN_UNDISCHARGED` | ownership | Resource not discharged |
| `E_OWN_BORROW_LIVE` | ownership | Operation blocked by live borrow |
| `E_OWN_BORROW_EXCL` | ownership | Exclusive borrow conflict |
| `E_OWN_BORROW_ALIAS` | ownership | Aliased incompatible references |
| `E_OWN_MERGE_CONFLICT` | ownership | Branch join state conflict |
| `E_OWN_ALIAS_MISSING` | ownership | Missing ownership alias record |
| `E_OWN_IMPORT_BAD_PATH` | ownership | Malformed import ownership path |
| `E_OWN_IMPORT_SUMMARY` | ownership | Import summary mismatch |
| `E_PROTO_TRANSITION` | protocols | Illegal protocol transition |
| `E_PROTO_TERMINAL` | protocols | Non-terminal state at scope exit |
| `E_DEFER_FLOW` | defer | Control flow in defer body |
| `E_DEFER_NESTED` | defer | Nested defer inside defer |
| `R_TRAP_OVERFLOW` | runtime | Integer overflow |
| `R_TRAP_DIV_ZERO` | runtime | Division by zero |
| `R_TRAP_BOUNDS` | runtime | Index out of bounds |
| `R_TRAP_CAST` | runtime | Cast value out of range |
| `R_TRAP_IMPORT` | runtime | Missing runtime import stub |
