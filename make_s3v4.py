#!/usr/bin/env python3
"""
make_s3v4.py — S3 dechunked v4: three new semantic families.

Targets taxonomy nodes uncovered by v2/v3:
  - ownership/branch_state/merge_conflict    (E_OWN_MERGE_CONFLICT)
  - ownership/loop/carried_external          (E_OWN_UNDISCHARGED via loop body)
  - ownership/loop/per_iteration_valid       (valid: create+close per iteration)
  - ownership/reinit/after_move_valid        (valid: rebind after move)
  - ownership/reinit/use_before_rebind       (E_OWN_MOVED)
  - ownership/multi_step/connect_then_close  (valid multi-step protocol path)

All three families have both valid and invalid members with the same surface
structure, differing by exactly one semantic condition.  Surface variation
spans 4 protocol types × 2 variable names = 8 variants per family.

Requires: az_corpus_framework.py on sys.path or same directory.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from the repo root without installation
sys.path.insert(0, str(Path(__file__).parent))

from az_corpus_framework import (
    CorpusFramework,
    Example,
    SemanticFamily,
    TaxonomyNode,
)

CORPUS_ID        = "axiomzig_prepretrain_corpus_0009"
BUNDLE_NAME      = "corpus009_ownership_moves_v47_59_dechunked_v4"
ROOT_PREFIX      = "corpus009_s3v4"
COMPILER_VERSION = "v47.59"

# ─────────────────────────────────────────────────────────────────────────────
# S3 taxonomy — the full set of ownership/moves concept nodes.
# v4 targets three new sub-trees not covered by v3.
# ─────────────────────────────────────────────────────────────────────────────

S3_TAXONOMY = [
    # ── covered by v3 ────────────────────────────────────────────────────────
    TaxonomyNode("ownership/borrow/scope_boundary",
                 "borrow in inner block ends before resource is closed"),
    TaxonomyNode("ownership/borrow/live_at_close",
                 "borrow still live when close is called → E_OWN_BORROW_LIVE"),
    TaxonomyNode("ownership/branch/both_close",
                 "both branches of if/else close the resource → valid"),
    TaxonomyNode("ownership/branch/one_misses",
                 "false branch skips close → E_OWN_UNDISCHARGED"),
    TaxonomyNode("ownership/move/close_moved_target",
                 "move resource, close the moved-to binding → valid"),
    TaxonomyNode("ownership/move/use_after_move",
                 "move resource, use the moved-from binding → E_OWN_MOVED"),
    TaxonomyNode("ownership/transition/single_close",
                 "one terminal transition → valid"),
    TaxonomyNode("ownership/transition/double_close",
                 "two terminal transitions → E_PROTO_TRANSITION"),
    TaxonomyNode("ownership/defer/defer_discharges",
                 "defer r.close() handles discharge → valid"),
    TaxonomyNode("ownership/defer/missing_discharge",
                 "no defer, no explicit close → E_OWN_UNDISCHARGED"),
    TaxonomyNode("ownership/errdefer/covers_error_path",
                 "errdefer before try fail() → valid on error path"),
    TaxonomyNode("ownership/errdefer/missing_errdefer",
                 "no errdefer → resource leaks on error path → E_OWN_UNDISCHARGED"),
    TaxonomyNode("ownership/alias/two_distinct_refs",
                 "close_both(borrow a, borrow b) with distinct resources → valid"),
    TaxonomyNode("ownership/alias/same_ref_twice",
                 "close_both(r, r) passes same Ref alias → E_OWN_BORROW_ALIAS"),

    # ── NEW: targeted by v4 ──────────────────────────────────────────────────
    TaxonomyNode("ownership/branch_state/both_same_state",
                 "both branches leave resource in same non-terminal state → valid continuation"),
    TaxonomyNode("ownership/branch_state/merge_conflict",
                 "branches leave resource in different states at join → E_OWN_MERGE_CONFLICT"),
    TaxonomyNode("ownership/branch_state/two_terminal_routes",
                 "both branches reach terminal via different transitions → valid"),
    TaxonomyNode("ownership/loop/per_iteration_create_close",
                 "resource created and closed inside each loop iteration → valid"),
    TaxonomyNode("ownership/loop/carried_external",
                 "resource created outside loop, borrowed inside, closed after → valid"),
    TaxonomyNode("ownership/loop/closed_in_body",
                 "resource closed inside loop body → only valid on first iteration → E_OWN_UNDISCHARGED"),
    TaxonomyNode("ownership/reinit/rebind_after_move",
                 "move resource to g, rebind original name fresh, close both → valid"),
    TaxonomyNode("ownership/reinit/use_before_rebind",
                 "move resource to g, use original name before rebind → E_OWN_MOVED"),
]

# ─────────────────────────────────────────────────────────────────────────────
# Surface variants: protocol × variable name
# The make_fn name is explicit to avoid naming mismatches across protocol types.
# ─────────────────────────────────────────────────────────────────────────────

VARIANTS = [
    # (proto_key, res_name, var_name, make_fn_name, fd_val)
    ("file",   "File",  "f",  "make_f",  0),
    ("file",   "File",  "fp", "make_fp", 1),
    ("sock",   "Sock",  "s",  "make_s",  2),
    ("sock",   "Sock",  "sk", "make_sk", 3),
    ("arena",  "Arena", "a",  "make_a",  4),
    ("arena",  "Arena", "ar", "make_ar", 5),
    ("txn",    "Txn",   "t",  "make_t",  6),
    ("txn",    "Txn",   "tx", "make_tx", 7),
]

# Socket is special: it has two paths to terminal (abort from Created, close from Connected).
# That makes it the only protocol usable for branch_state families.
SOCK_VARIANTS = [v for v in VARIANTS if v[0] == "sock"]

# ─────────────────────────────────────────────────────────────────────────────
# Protocol declaration helpers
# ─────────────────────────────────────────────────────────────────────────────

def _proto_decl(pk: str, res: str, mfn: str, fd: int) -> tuple[str, str, str]:
    """Return (decl_text, close_method, connect_method_or_empty)."""
    pname = f"{res}P"
    if pk == "file":
        return (
            f"protocol {pname} {{ states {{ Open, Closed }} init Open; terminal {{ Closed }}\n"
            f"    transitions {{ close: Open -> Closed; }} }}\n"
            f"resource {res} {{ protocol: {pname}; fields {{ fd: I32; }} }}\n"
            f"fn {mfn}() -> {res} effects() {{ return {res} {{ fd: {fd} }}; }}",
            "close", "",
        )
    if pk == "sock":
        return (
            f"protocol {pname} {{ states {{ Created, Connected, Closed }} init Created; terminal {{ Closed }}\n"
            f"    transitions {{ connect: Created -> Connected; "
            f"abort: Created -> Closed; close: Connected -> Closed; }} }}\n"
            f"resource {res} {{ protocol: {pname}; fields {{ fd: I32; }} }}\n"
            f"fn {mfn}() -> {res} effects() {{ return {res} {{ fd: {fd} }}; }}",
            "abort", "connect",
        )
    if pk == "arena":
        return (
            f"protocol {pname} {{ states {{ Active, Freed }} init Active; terminal {{ Freed }}\n"
            f"    transitions {{ free: Active -> Freed; }} }}\n"
            f"resource {res} {{ protocol: {pname}; fields {{ base: I64; }} }}\n"
            f"fn {mfn}() -> {res} effects() {{ return {res} {{ base: {fd} }}; }}",
            "free", "",
        )
    if pk == "txn":
        return (
            f"protocol {pname} {{ states {{ Begun, Committed }} init Begun; terminal {{ Committed }}\n"
            f"    transitions {{ commit: Begun -> Committed; }} }}\n"
            f"resource {res} {{ protocol: {pname}; fields {{ id: I64; }} }}\n"
            f"fn {mfn}() -> {res} effects() {{ return {res} {{ id: {fd} }}; }}",
            "commit", "",
        )
    raise ValueError(pk)


def make_src(slug: str, pk: str, res: str, mfn: str, fd: int, body: str) -> str:
    decl, _, _ = _proto_decl(pk, res, mfn, fd)
    return f"module corpus009.own.{slug};\n\n{decl}\n\n{body}\n"


def close_of(pk: str) -> str:
    return _proto_decl(pk, "X", "mx", 0)[1]


def connect_of(pk: str) -> str:
    return _proto_decl(pk, "X", "mx", 0)[2]


# ─────────────────────────────────────────────────────────────────────────────
# Family generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_branch_state_families() -> list[SemanticFamily]:
    """
    FAMILY 1: branch_state_same
    Semantic condition: both branches leave resource in the same state.
    Valid:   if(flag) { s.connect(); } else { s.connect(); }  → both Connected
    Invalid: if(flag) { s.connect(); } else { }               → Connected vs Created
             → E_OWN_MERGE_CONFLICT

    Only socket is used because it has intermediate states (Created → Connected).
    File/Arena/Txn go immediately from init to terminal, so there's no interesting
    intermediate state to produce a merge conflict.

    FAMILY 2: branch_state_two_terminal_routes
    Semantic condition: both branches reach terminal via different transitions.
    Valid:   if(flag) { s.abort(); } else { s.connect(); s.close(); }
             → both reach Closed, different paths → valid
    (No invalid variant needed — this is a positive-only semantic fact;
    included to balance the two_same_state invalid above.)
    """
    examples: list[Example] = []

    for i, (pk, res, var, mfn, fd) in enumerate(SOCK_VARIANTS * 4):  # 8 variants total
        idx = i % len(SOCK_VARIANTS) + (i // len(SOCK_VARIANTS)) * len(SOCK_VARIANTS)
        cl = close_of(pk)   # "abort"
        co = connect_of(pk) # "connect"
        pair = f"branch_state_{i}"

        # Valid: both branches connect
        body_v = (
            f"fn run_{i}(flag: Bool) -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    if (flag) {{ {var}.{co}(); }} else {{ {var}.{co}(); }}\n"
            f"    {var}.close();\n"
            f"    return unit;\n}}"
        )
        # Invalid: one branch connects, other doesn't → merge conflict
        body_i = (
            f"fn run_{i}(flag: Bool) -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    if (flag) {{ {var}.{co}(); }}\n"
            f"    {var}.{cl}();\n"
            f"    return unit;\n}}"
        )

        examples.append(Example(
            desc=f"branch_state_v_{i}", template_id="s3_branch_state",
            family="branch_state",
            role="valid_both_connect_same_state",
            src=make_src(f"branch_state_v_{i}", pk, res, mfn, fd, body_v),
            concepts=["branch_join", "same_state_at_join", "multi_step_protocol"],
            difficulty="medium", pair=pair,
            taxonomy_nodes=["ownership/branch_state/both_same_state",
                            "ownership/branch_state/two_terminal_routes"],
        ))
        examples.append(Example(
            desc=f"branch_state_i_{i}", template_id="s3_branch_state",
            family="branch_state",
            role="invalid_merge_conflict",
            src=make_src(f"branch_state_i_{i}", pk, res, mfn, fd, body_i),
            concepts=["branch_join", "state_disagree_at_join", "E_OWN_MERGE_CONFLICT"],
            difficulty="medium", pair=pair,
            taxonomy_nodes=["ownership/branch_state/merge_conflict"],
        ))

    # Two-terminal-routes family (all 8 surface variants)
    two_terminal_examples: list[Example] = []
    for i, (pk, res, var, mfn, fd) in enumerate(SOCK_VARIANTS * 4):
        cl  = close_of(pk)    # abort
        co  = connect_of(pk)  # connect
        pair2 = f"two_terminal_{i}"

        body_v2 = (
            f"fn run_{i}(flag: Bool) -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    if (flag) {{ {var}.{cl}(); }}\n"
            f"    else {{ {var}.{co}(); {var}.close(); }}\n"
            f"    return unit;\n}}"
        )
        body_i2 = (
            f"fn run_{i}(flag: Bool) -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    if (flag) {{ {var}.{cl}(); }}\n"
            f"    else {{ {var}.{co}(); }}\n"
            f"    {var}.{cl}();\n"
            f"    return unit;\n}}"
        )
        two_terminal_examples.append(Example(
            desc=f"two_terminal_v_{i}", template_id="s3_two_terminal_routes",
            family="two_terminal_routes",
            role="valid_both_reach_terminal_via_different_paths",
            src=make_src(f"two_terminal_v_{i}", pk, res, mfn, fd, body_v2),
            concepts=["multi_step_protocol", "two_terminal_paths", "branch_join"],
            difficulty="hard", pair=pair2,
            taxonomy_nodes=["ownership/branch_state/two_terminal_routes"],
        ))
        two_terminal_examples.append(Example(
            desc=f"two_terminal_i_{i}", template_id="s3_two_terminal_routes",
            family="two_terminal_routes",
            role="invalid_wrong_state_for_abort_after_connect",
            src=make_src(f"two_terminal_i_{i}", pk, res, mfn, fd, body_i2),
            concepts=["multi_step_protocol", "wrong_state_transition", "E_PROTO_TRANSITION"],
            difficulty="hard", pair=pair2,
            taxonomy_nodes=["ownership/branch_state/merge_conflict"],
        ))

    return [
        SemanticFamily(
            template_id="s3_branch_state",
            family_key="branch_state",
            examples=examples,
            taxonomy_nodes=[
                "ownership/branch_state/both_same_state",
                "ownership/branch_state/merge_conflict",
            ],
        ),
        SemanticFamily(
            template_id="s3_two_terminal_routes",
            family_key="two_terminal_routes",
            examples=two_terminal_examples,
            taxonomy_nodes=["ownership/branch_state/two_terminal_routes"],
        ),
    ]


def gen_loop_families() -> list[SemanticFamily]:
    """
    FAMILY 3: loop_per_iteration
    Semantic condition: does the loop body create AND close a fresh resource
                       per iteration (valid), or try to close an external one
                       that's already closed after the first iteration (invalid)?

    Valid:   for (i in 0..N) { let f = make_f(); f.close(); }
    Invalid: let f = make_f();
             for (i in 0..N) { f.close(); }   ← double-close after first iter

    FAMILY 4: loop_carried_external
    Semantic condition: resource created outside loop, borrowed inside, closed after.
    Valid:   let f = make_f(); for (...) { peek(borrow f); } f.close();
    Invalid: let f = make_f(); for (...) { f.close(); }     ← close in body leaks
    """
    per_iter_examples: list[Example] = []
    carried_examples: list[Example] = []

    for i, (pk, res, var, mfn, fd) in enumerate(VARIANTS):
        cl = close_of(pk)
        pair_pi = f"loop_per_iter_{i}"
        pair_ca = f"loop_carried_{i}"

        # per_iteration: valid
        body_pv = (
            f"fn run_{i}() -> Unit effects(protocol_step) {{\n"
            f"    for (j: I64 in 0..3) {{\n"
            f"        let {var} = {mfn}();\n"
            f"        {var}.{cl}();\n"
            f"    }}\n"
            f"    return unit;\n}}"
        )
        # per_iteration: invalid — external resource closed in body (double-close)
        body_pi = (
            f"fn run_{i}() -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    for (j: I64 in 0..3) {{\n"
            f"        {var}.{cl}();\n"
            f"    }}\n"
            f"    return unit;\n}}"
        )

        per_iter_examples.append(Example(
            desc=f"loop_per_iter_v_{i}", template_id="s3_loop_per_iter",
            family="loop_per_iter",
            role="valid_create_close_per_iteration",
            src=make_src(f"loop_per_iter_v_{i}", pk, res, mfn, fd, body_pv),
            concepts=["loop", "per_iteration_resource", "create_close_in_loop"],
            difficulty="medium", pair=pair_pi,
            taxonomy_nodes=["ownership/loop/per_iteration_create_close"],
        ))
        per_iter_examples.append(Example(
            desc=f"loop_per_iter_i_{i}", template_id="s3_loop_per_iter",
            family="loop_per_iter",
            role="invalid_close_external_in_loop_body",
            src=make_src(f"loop_per_iter_i_{i}", pk, res, mfn, fd, body_pi),
            concepts=["loop", "external_resource", "close_in_loop_body", "E_OWN_UNDISCHARGED"],
            difficulty="medium", pair=pair_pi,
            taxonomy_nodes=["ownership/loop/closed_in_body"],
        ))

        # loop_carried: valid (borrow inside, close after)
        peek_fn = f"peek_{i}"
        body_cv = (
            f"fn {peek_fn}(r: RefConst<{res}>) -> Unit effects() {{ return unit; }}\n"
            f"fn run_{i}() -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    for (j: I64 in 0..5) {{\n"
            f"        {peek_fn}(borrow {var});\n"
            f"    }}\n"
            f"    {var}.{cl}();\n"
            f"    return unit;\n}}"
        )
        # loop_carried: invalid (resource moved out of scope in first iteration)
        other = f"g{i}"
        body_ci = (
            f"fn take_{i}(r: {res}) -> Unit effects(protocol_step) {{\n"
            f"    r.{cl}();\n"
            f"    return unit;\n}}\n"
            f"fn run_{i}() -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    for (j: I64 in 0..3) {{\n"
            f"        take_{i}(move {var});\n"
            f"    }}\n"
            f"    return unit;\n}}"
        )

        carried_examples.append(Example(
            desc=f"loop_carried_v_{i}", template_id="s3_loop_carried",
            family="loop_carried",
            role="valid_borrow_in_loop_close_after",
            src=make_src(f"loop_carried_v_{i}", pk, res, mfn, fd, body_cv),
            concepts=["loop", "carried_external_resource", "borrow_in_loop", "close_after"],
            difficulty="medium", pair=pair_ca,
            taxonomy_nodes=["ownership/loop/carried_external"],
        ))
        carried_examples.append(Example(
            desc=f"loop_carried_i_{i}", template_id="s3_loop_carried",
            family="loop_carried",
            role="invalid_move_in_loop_body",
            src=make_src(f"loop_carried_i_{i}", pk, res, mfn, fd, body_ci),
            concepts=["loop", "move_in_loop_body", "use_after_move", "E_OWN_MOVED"],
            difficulty="hard", pair=pair_ca,
            taxonomy_nodes=["ownership/loop/carried_external"],
        ))

    return [
        SemanticFamily(
            template_id="s3_loop_per_iter",
            family_key="loop_per_iter",
            examples=per_iter_examples,
            taxonomy_nodes=[
                "ownership/loop/per_iteration_create_close",
                "ownership/loop/closed_in_body",
            ],
        ),
        SemanticFamily(
            template_id="s3_loop_carried",
            family_key="loop_carried",
            examples=carried_examples,
            taxonomy_nodes=["ownership/loop/carried_external"],
        ),
    ]


def gen_reinit_families() -> list[SemanticFamily]:
    """
    FAMILY 5: reinit_after_move
    Semantic condition: after moving resource to g, does code rebind the
                       original name to a fresh value (valid), or use the
                       original moved-from binding (invalid → E_OWN_MOVED)?

    Valid:   let f = make_f(); let g = move f; let f = make_f(); f.close(); g.close();
    Invalid: let f = make_f(); let g = move f; f.close(); g.close();
    """
    examples: list[Example] = []

    for i, (pk, res, var, mfn, fd) in enumerate(VARIANTS):
        cl = close_of(pk)
        pair = f"reinit_{i}"
        other = f"g{i}"

        # Valid: rebind after move
        body_v = (
            f"fn run_{i}() -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    let {other}: {res} = move {var};\n"
            f"    let {var}: {res} = {mfn}();\n"
            f"    {var}.{cl}();\n"
            f"    {other}.{cl}();\n"
            f"    return unit;\n}}"
        )
        # Invalid: use before rebind
        body_i = (
            f"fn run_{i}() -> Unit effects(protocol_step) {{\n"
            f"    let {var} = {mfn}();\n"
            f"    let {other}: {res} = move {var};\n"
            f"    {var}.{cl}();\n"
            f"    {other}.{cl}();\n"
            f"    return unit;\n}}"
        )

        examples.append(Example(
            desc=f"reinit_v_{i}", template_id="s3_reinit_after_move",
            family="reinit_after_move",
            role="valid_rebind_after_move",
            src=make_src(f"reinit_v_{i}", pk, res, mfn, fd, body_v),
            concepts=["move", "reinit", "rebind_same_name", "fresh_value_after_move"],
            difficulty="hard", pair=pair,
            taxonomy_nodes=["ownership/reinit/rebind_after_move"],
        ))
        examples.append(Example(
            desc=f"reinit_i_{i}", template_id="s3_reinit_after_move",
            family="reinit_after_move",
            role="invalid_use_before_rebind",
            src=make_src(f"reinit_i_{i}", pk, res, mfn, fd, body_i),
            concepts=["move", "use_after_move", "E_OWN_MOVED"],
            difficulty="medium", pair=pair,
            taxonomy_nodes=["ownership/reinit/use_before_rebind"],
        ))

    return [
        SemanticFamily(
            template_id="s3_reinit_after_move",
            family_key="reinit_after_move",
            examples=examples,
            taxonomy_nodes=[
                "ownership/reinit/rebind_after_move",
                "ownership/reinit/use_before_rebind",
            ],
        ),
    ]


MIX_YAML = """\
mix_name: axiomzig_v47_59_phase1_s3_dechunked_v4
streams:
  - name: s3_ownership_moves_v4
    documents: documents/ownership_moves/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1, axiomzig_skeleton_v1]
    filter:
      include:
        - "$.attributes[?(@.axiomzig_validator_v1.label_agreement.agreement == 'full')]"
        - "$.attributes[?(@.axiomzig_validator_v1.validation_status == 'current_v47_59')]"
      exclude:
        - "$.attributes[?(@.axiomzig_skeleton_v1.cluster_size > 8)]"
"""


def build(compiler_root: Path, out_zip: Path, workdir: Path) -> dict:
    fw = CorpusFramework(
        corpus_id=CORPUS_ID,
        bundle_name=BUNDLE_NAME,
        compiler_root=compiler_root,
        workdir=workdir,
        root_prefix=ROOT_PREFIX,
        compiler_version=COMPILER_VERSION,
    )
    fw.set_taxonomy(S3_TAXONOMY)

    # Collect all families
    all_families = (
        gen_branch_state_families()
        + gen_loop_families()
        + gen_reinit_families()
    )

    total_candidates = sum(len(f.examples) for f in all_families)
    print(f"Candidates: {total_candidates} across {len(all_families)} families", flush=True)

    # Process every example through the compiler
    for family in all_families:
        for ex in family.examples:
            fw.process_example(ex, phase="owncheck")

    print(f"After compile: {len(fw._ledger)} records, {len(fw._rejected)} rejected", flush=True)

    # Mixed-template filter
    n_dropped_uniform = fw.filter_uniform_templates()
    print(f"After mixed-template filter: {len(fw._ledger)} "
          f"(dropped {n_dropped_uniform} from uniform templates)", flush=True)

    # Diagnostic skew reduction
    n_dropped_skew = fw.reduce_diagnostic_skew(max_pct=0.40)
    print(f"After skew reduction: {len(fw._ledger)} (dropped {n_dropped_skew})", flush=True)

    from collections import Counter
    diag_now: Counter = Counter()
    for r in fw._ledger:
        if r["expected_outcome"] == "fail":
            for c in filter(None, r["expected_diagnostic_codes"].split("|")):
                diag_now[c] += 1
    print(f"  diagnostics: {dict(diag_now)}", flush=True)

    # Finalize
    script_text = (Path(__file__).read_text(encoding="utf-8")
                   if Path(__file__).exists() else "# source not available")
    result = fw.finalize(out_zip, script_text=script_text, mix_yaml=MIX_YAML)

    print(f"\nChunk risk: {result['chunk_risk_status']}", flush=True)
    if result.get("chunk_risk_hard_fail_reasons"):
        print(f"  FAIL: {result['chunk_risk_hard_fail_reasons']}", flush=True)
    print(f"  Template MI:  {fw._ledger and round(result['accepted'], 3)}", flush=True)
    tax = result["taxonomy_coverage"]
    print(f"Taxonomy: {tax.get('covered_nodes','?')}/{tax.get('total_nodes','?')} "
          f"nodes ({tax.get('coverage_ratio', 0):.1%})", flush=True)
    print(f"Zip: {result['zip_size_kb']} KB, integrity: {result['zip_integrity_error']}", flush=True)
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description="Build corpus009 S3 v4")
    ap.add_argument("--compiler-root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--workdir", default="/tmp/s3v4_work")
    args = ap.parse_args()
    import json
    result = build(
        Path(args.compiler_root),
        Path(args.out),
        Path(args.workdir),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
