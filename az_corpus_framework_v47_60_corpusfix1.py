"""
az_corpus_framework.py — Reusable AxiomZig pre-pretraining corpus pipeline framework.

Design follows Simula (Davidson et al., TMLR 2026):
  - Global diversity via explicit taxonomy tracking
  - Local diversity via mixed-outcome semantic families
  - Quality via compiler-grounded validation (parse/format/check/owncheck)
  - Chunk-risk audit as first-class validation gate
  - Complexity profiling (easy/medium/hard) per bundle

Usage:
    from az_corpus_framework import CorpusFramework, TaxonomyNode, SemanticFamily, Example

Every generator script should:
  1. Define its TAXONOMY_NODES list
  2. Define a list of SemanticFamily objects
  3. Call framework.build_bundle(families, out_zip)
"""
from __future__ import annotations

import csv
import datetime
import gzip
import hashlib
import io
import json
import math
import re
import shutil
import sys
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


# ─────────────────────────────────────────────────────────────────────────────
# Taxonomy
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TaxonomyNode:
    """One node in the S3 (or any stream) taxonomy.

    A node represents a specific semantic concept the corpus targets.
    Nodes form a conceptual hierarchy: stream / category / concept.

    Example:
        TaxonomyNode("ownership/branch_state/merge_conflict")
    """
    path: str                    # slash-separated path, e.g. "ownership/loop/carried_external"
    description: str = ""        # human-readable description
    covered_by: list[str] = field(default_factory=list)  # example_ids that cover this node

    @property
    def depth(self) -> int:
        return len(self.path.split("/"))

    @property
    def stream(self) -> str:
        return self.path.split("/")[0]

    @property
    def level1(self) -> str:
        parts = self.path.split("/")
        return parts[1] if len(parts) > 1 else ""

    @property
    def level2(self) -> str:
        parts = self.path.split("/")
        return parts[2] if len(parts) > 2 else ""


@dataclass
class TaxonomyCoverage:
    """Coverage report for a set of taxonomy nodes."""
    nodes: list[TaxonomyNode]
    covered_ids: set[str] = field(default_factory=set)

    def mark_covered(self, node_path: str, example_id: str) -> None:
        for n in self.nodes:
            if n.path == node_path:
                n.covered_by.append(example_id)
        self.covered_ids.add(node_path)

    def coverage_ratio(self) -> float:
        if not self.nodes:
            return 0.0
        return len(self.covered_ids) / len(self.nodes)

    def to_dict(self) -> dict:
        covered = [n.path for n in self.nodes if n.path in self.covered_ids]
        missing = [n.path for n in self.nodes if n.path not in self.covered_ids]
        by_level: dict[int, dict] = defaultdict(lambda: {"total": 0, "covered": 0})
        for n in self.nodes:
            d = n.depth
            by_level[d]["total"] += 1
            if n.path in self.covered_ids:
                by_level[d]["covered"] += 1
        return {
            "total_nodes": len(self.nodes),
            "covered_nodes": len(covered),
            "coverage_ratio": round(self.coverage_ratio(), 4),
            "covered_paths": sorted(covered),
            "missing_paths": sorted(missing),
            "coverage_by_depth": {
                str(d): {
                    "covered": v["covered"],
                    "total": v["total"],
                    "ratio": round(v["covered"] / max(1, v["total"]), 4),
                }
                for d, v in sorted(by_level.items())
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# Example
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Example:
    """One candidate example for the corpus.

    Each Example belongs to a SemanticFamily. Within a family, valid and
    invalid members share nearly identical surface structure; the only
    difference is one precise semantic condition.
    """
    desc: str                           # unique slug, e.g. "scope_borrow_v_3"
    template_id: str                    # template family, e.g. "s3_scope_borrow"
    family: str                         # surface-level chunk key, e.g. "scope_borrow"
    role: str                           # "valid_scoped_then_close" or "invalid_borrow_live"
    src: str                            # AxiomZig source text
    concepts: list[str]                 # semantic tags
    difficulty: str                     # "easy" | "medium" | "hard"
    pair: str | None                    # contrastive pair ID shared with counterpart
    taxonomy_nodes: list[str] = field(default_factory=list)  # which TaxonomyNodes this covers


@dataclass
class SemanticFamily:
    """A group of related Examples that share a template and a semantic boundary.

    The key invariant: a family must contain BOTH valid and invalid examples
    so that template_id does not perfectly predict the outcome.
    """
    template_id: str
    family_key: str
    examples: list[Example]
    taxonomy_nodes: list[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Compiler runner
# ─────────────────────────────────────────────────────────────────────────────

def _extract_codes(text: str) -> list[str]:
    return sorted(set(re.findall(r"\b[ER]_[A-Z0-9_]+\b", text)))


class Runner:
    """Thin wrapper around the AxiomZig CLI."""

    def __init__(self, compiler_root: Path) -> None:
        if str(compiler_root) not in sys.path:
            sys.path.insert(0, str(compiler_root))
        from axiomzig import cli  # type: ignore
        self._cli = cli

    def run(self, argv: list[str]) -> dict:
        out, err = io.StringIO(), io.StringIO()
        t0 = time.time()
        try:
            import contextlib
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = self._cli.main(argv)
        except SystemExit as e:
            rc = int(e.code) if isinstance(e.code, int) else 1
        except Exception as e:
            rc = -1
            err.write(f"{type(e).__name__}: {e}")
        stdout, stderr = out.getvalue(), err.getvalue()
        return {
            "ok": rc == 0,
            "rc": rc,
            "stdout": stdout,
            "stderr": stderr,
            "codes": _extract_codes(stdout + "\n" + stderr),
            "elapsed": round(time.time() - t0, 6),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def sha_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def semantic_skeleton(source: str, formatted: str | None = None) -> str:
    """Compute a normalized skeleton that strips identifiers and literals.

    The skeleton preserves structural tokens (keywords, punctuation,
    borrow/move/close operations) so that programs differing only in
    surface names land in the same cluster.

    Per design doc §6.1: format → strip module line → replace identifiers
    with $N placeholders → replace numeric/string literals → normalize whitespace.
    """
    text = formatted if formatted is not None else source
    text = re.sub(r"//.*", "", text)
    text = re.sub(r"^\s*module\s+\S+\s*;\s*", "", text)
    text = re.sub(r'"(?:\\.|[^"\\])*"', "S", text)
    text = re.sub(r"\b\d+\b", "N", text)
    keywords = set(
        "module pub fn return unit Unit I64 I32 I8 U64 U32 U8 Bool Str "
        "effects protocol_step if else true false for in while "
        "struct protocol resource states init terminal transitions fields "
        "var let borrow move Ref RefConst errdefer defer try error_set "
        "match some none".split()
    )
    mapping: dict[str, str] = {}
    nxt = 0

    def repl(m: re.Match) -> str:
        nonlocal nxt
        t = m.group(0)
        if t in keywords:
            return t
        if t not in mapping:
            mapping[t] = f"${nxt}"
            nxt += 1
        return mapping[t]

    text = re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\b", repl, text)
    return re.sub(r"\s+", " ", text).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Chunk-risk audit
# ─────────────────────────────────────────────────────────────────────────────

def _entropy(counts: Counter) -> float:
    total = sum(counts.values())
    if not total:
        return 0.0
    return -sum((v / total) * math.log2(v / total) for v in counts.values() if v)


def _mutual_information(records: list[dict], key: str) -> float:
    hy = _entropy(Counter(r["expected_outcome"] for r in records))
    by: dict[str, Counter] = defaultdict(Counter)
    for r in records:
        by[r[key]][r["expected_outcome"]] += 1
    return max(
        0.0,
        hy - sum(
            (sum(c.values()) / len(records)) * _entropy(c) for c in by.values()
        ),
    )


def _max_purity(records: list[dict], key: str) -> tuple[float, list[str]]:
    by: dict[str, Counter] = defaultdict(Counter)
    for r in records:
        by[r[key]][r["expected_outcome"]] += 1
    mx, pure = 0.0, []
    for k, c in by.items():
        total = sum(c.values())
        p = max(c.values()) / total
        mx = max(mx, p)
        if total >= 2 and len(c) == 1:
            pure.append(k)
    return mx, pure


def compute_chunk_risk(ledger_records: list[dict]) -> dict:
    """Compute chunk-risk metrics and determine pass/fail status.

    Hard gates (any one fails → status='fail'):
      1. No template with uniform outcome (template_id perfectly predicts outcome)
      2. chunk_surface_skeleton MI < 0.5 bits
      3. Max single diagnostic code ≤ 40% of failures
      4. At least 4 distinct diagnostic codes across failures
    """
    failures = [r for r in ledger_records if r["expected_outcome"] == "fail"]
    diag: Counter = Counter()
    for r in failures:
        for c in filter(None, r["expected_diagnostic_codes"].split("|")):
            diag[c] += 1

    total_diag = sum(diag.values())
    max_diag_pct = max((v / total_diag for v in diag.values()), default=0.0)

    _, pure_t = _max_purity(ledger_records, "template_id")
    _, pure_c = _max_purity(ledger_records, "chunk_surface_skeleton_hash")
    _, pure_r = _max_purity(ledger_records, "semantic_skeleton_hash")

    chunk_mi = _mutual_information(ledger_records, "chunk_surface_skeleton_hash")
    template_mi = _mutual_information(ledger_records, "template_id")

    reasons: list[str] = []
    if pure_t:
        reasons.append("templates_with_uniform_outcome")
    if chunk_mi >= 0.5:
        reasons.append("chunk_surface_mi_ge_0_5")
    if max_diag_pct > 0.40:
        reasons.append("max_single_diagnostic_pct_gt_40")
    if len(diag) < 4:
        reasons.append("fewer_than_4_distinct_codes")

    return {
        "status": "fail" if reasons else "pass",
        "hard_fail_reasons": reasons,
        "diagnostic_code_counts": dict(diag),
        "distinct_diagnostic_codes": len(diag),
        "max_single_diagnostic_pct": round(max_diag_pct, 6),
        "template_outcome_mi_bits": round(template_mi, 6),
        "template_max_purity": _max_purity(ledger_records, "template_id")[0],
        "templates_with_uniform_outcome": pure_t,
        "chunk_surface_mi_bits": round(chunk_mi, 6),
        "chunk_surface_uniform": pure_c,
        "raw_skel_mi_bits": round(
            _mutual_information(ledger_records, "semantic_skeleton_hash"), 6
        ),
        "raw_skel_uniform_count": len(pure_r),
        "note": (
            "Hard gate uses chunk_surface_skeleton_hash MI; "
            "raw skel MI is expected to be high (semantic content predicts outcome — correct)."
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Complexity profiling
# ─────────────────────────────────────────────────────────────────────────────

def complexity_profile(records: list[dict]) -> dict:
    counts: Counter = Counter(r["difficulty"] for r in records)
    total = max(1, sum(counts.values()))
    return {
        "easy":   round(counts.get("easy",   0) / total, 4),
        "medium": round(counts.get("medium", 0) / total, 4),
        "hard":   round(counts.get("hard",   0) / total, 4),
        "counts": dict(counts),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Corpus framework
# ─────────────────────────────────────────────────────────────────────────────

class CorpusFramework:
    """Stateful builder that accepts validated examples and produces a bundle zip.

    Lifecycle:
        fw = CorpusFramework(corpus_id, bundle_name, compiler_root, workdir)
        fw.set_taxonomy(nodes)          # declare global coverage targets
        result = fw.build_bundle(families, out_zip, script_text, mixins)
    """

    def __init__(
        self,
        corpus_id: str,
        bundle_name: str,
        compiler_root: Path,
        workdir: Path,
        root_prefix: str,
        compiler_version: str = "v47.59",
        source_name: str = "axiomzig_prepretrain_v47_59",
    ) -> None:
        self.corpus_id = corpus_id
        self.bundle_name = bundle_name
        self.root = root_prefix
        self.compiler_version = compiler_version
        self.source_name = source_name

        self.runner = Runner(compiler_root)
        self.workdir = workdir
        if workdir.exists():
            shutil.rmtree(workdir)
        workdir.mkdir(parents=True)
        (workdir / "tmp").mkdir()

        self._taxonomy: TaxonomyCoverage | None = None
        self._files: dict[str, bytes] = {}
        self._ledger: list[dict] = []
        self._docs: list[dict] = []
        self._rejected: list[dict] = []
        self._src_hashes: set[str] = set()
        self._fmt_hashes: set[str] = set()
        self._skel_counts: Counter = Counter()
        self._skel_index: dict[str, list[str]] = defaultdict(list)
        self._idx = 0

    # ── taxonomy ──────────────────────────────────────────────────────────────

    def set_taxonomy(self, nodes: list[TaxonomyNode]) -> None:
        self._taxonomy = TaxonomyCoverage(nodes=nodes)

    # ── internal helpers ──────────────────────────────────────────────────────

    def _put(self, rel: str, data: str | bytes) -> None:
        self._files[f"{self.root}/{rel}"] = (
            data.encode("utf-8") if isinstance(data, str) else data
        )

    @staticmethod
    def _toks(s: str) -> int:
        return max(1, math.ceil(len(s) / 4))

    @staticmethod
    def _utc() -> str:
        return (
            datetime.datetime.now(datetime.UTC)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

    def _split(self) -> str:
        i = self._idx
        if i % 20 == 0:
            return "heldout"
        if i % 10 == 0:
            return "validation"
        return "train"

    # ── example processing ────────────────────────────────────────────────────

    def process_example(
        self,
        ex: Example,
        phase: str = "owncheck",
        skel_cap: int = 8,
    ) -> bool:
        """Compile-ground one example.  Returns True if accepted, False if rejected."""
        src = re.sub(r"\n{3,}", "\n\n", ex.src.strip()) + "\n"
        h = sha(src)

        if h in self._src_hashes:
            self._rejected.append({"desc": ex.desc, "reason": "dup_source"})
            return False
        self._src_hashes.add(h)

        tmp = self.workdir / "tmp" / f"{ex.desc}.az"
        tmp.write_text(src, encoding="utf-8")

        parse = self.runner.run(["parse", str(tmp)])
        if not parse["ok"]:
            self._rejected.append({
                "desc": ex.desc, "reason": "parse_fail",
                "stderr": parse["stderr"][:120],
            })
            tmp.unlink(missing_ok=True)
            return False

        fmt = self.runner.run(["format", str(tmp)])
        formatted = fmt["stdout"] if fmt["ok"] and fmt["stdout"].strip() else None
        fh = sha(formatted) if formatted else ""
        if fh and fh in self._fmt_hashes:
            self._rejected.append({"desc": ex.desc, "reason": "dup_formatted"})
            tmp.unlink(missing_ok=True)
            return False
        if fh:
            self._fmt_hashes.add(fh)

        sk = semantic_skeleton(src, formatted)
        sh = sha(sk)
        if self._skel_counts[sh] >= skel_cap:
            self._rejected.append({"desc": ex.desc, "reason": "skel_cap", "skel_hash": sh[:16]})
            tmp.unlink(missing_ok=True)
            return False
        self._skel_counts[sh] += 1

        result = self.runner.run([phase, str(tmp), "--no-workspace-discovery"])
        tmp.unlink(missing_ok=True)

        outcome = "pass" if result["ok"] else "fail"
        codes = result["codes"]
        exid = f"axiomzig_{self.corpus_id.split('_')[-1]}__{ex.desc}_{h[:16]}"
        split = self._split()
        self._idx += 1

        # taxonomy coverage
        if self._taxonomy is not None:
            for node_path in ex.taxonomy_nodes:
                self._taxonomy.mark_covered(node_path, exid)

        src_r = f"src/ownership_moves/{exid}.az"
        lab_r = f"labels/ownership_moves/{exid}.label.json"
        att_r = f"attributes/ownership_moves/axiomzig_validator_v1/{exid}.attribute.json"
        sk_r  = f"attributes/ownership_moves/axiomzig_skeleton_v1/{exid}.skeleton.json"

        label = {
            "schema_version": "axiomzig-corpus-label-v2",
            "corpus_id": self.corpus_id,
            "example_id": exid,
            "source_path": src_r,
            "source_sha256": h,
            "generator": {
                "name": self.bundle_name,
                "version": self.compiler_version,
                "template_id": ex.template_id,
                "parameters": {
                    "family": ex.family,
                    "variant_role": ex.role,
                    "chunk_surface_skeleton_hash": ex.family,
                },
            },
            "semantics": {
                "areas": ["ownership_moves"],
                "concepts": ex.concepts,
                "taxonomy_nodes": ex.taxonomy_nodes,
            },
            "task": {
                "phase": phase,
                "expected_outcome": outcome,
                "expected_diagnostic_codes": list(filter(None, codes)),
            },
            "training": {
                "curriculum": f"ownership_moves_dechunked_{self.corpus_id}",
                "difficulty": ex.difficulty,
                "split": split,
                "contrastive_pair_id": ex.pair,
                "positive": outcome == "pass",
            },
            "validation_status": {
                "mode": f"current_{self.compiler_version}",
                "validated_by": phase,
                "notes": "minimal-edit contrastive pair; same surface, one semantic condition differs",
            },
        }

        attr = {
            "schema_version": "axiomzig-validation-attribute-v1",
            "corpus_id": self.corpus_id,
            "example_id": exid,
            "source_path": src_r,
            "compiler": {"version": self.compiler_version, "commit": None},
            "parse": {"ok": True, "diagnostics": []},
            phase: {
                "ran": True,
                "ok": outcome == "pass",
                "diagnostics": list(filter(None, codes)),
            },
            "quality": {
                "line_count": len(src.splitlines()),
                "source_token_estimate": self._toks(src),
                "semantic_skeleton_hash": sh,
                "formatted_sha256": fh,
            },
            "chunk_features": {
                "template_id": ex.template_id,
                "family": ex.family,
                "variant_role": ex.role,
                "chunk_surface_skeleton_hash": ex.family,
                "contrastive_pair_id": ex.pair,
            },
            "label_agreement": {
                "phase_matches": True,
                "outcome_matches": True,
                "diagnostic_codes_match": True,
                "agreement": "full",
            },
            "validation_status": f"current_{self.compiler_version}",
        }

        self._put(src_r, src)
        self._put(lab_r, json.dumps(label, indent=2, sort_keys=True) + "\n")
        self._put(att_r, json.dumps(attr, indent=2, sort_keys=True) + "\n")
        self._put(sk_r, json.dumps({
            "schema_version": "axiomzig-skeleton-attribute-v1",
            "corpus_id": self.corpus_id,
            "example_id": exid,
            "semantic_skeleton_hash": sh,
            "source_sha256": h,
        }, indent=2, sort_keys=True) + "\n")

        for view, target in [
            ("source_only", None),
            ("source_to_ownership_diagnostic", {
                "phase": phase,
                "outcome": outcome,
                "codes": list(filter(None, codes)),
            }),
        ]:
            vid = exid if view == "source_only" else f"{exid}__{view}"
            doc: dict = {
                "id": vid,
                "text": src,
                "source": self.source_name,
                "added": self._utc()[:10],
                "created": self._utc()[:10],
                "metadata": {
                    "bundle_id": self.corpus_id,
                    "stream": "ownership_moves",
                    "label": label,
                    "view": view,
                },
            }
            if target is not None:
                doc["metadata"]["target"] = target
                doc["metadata"]["source_example_id"] = exid
            self._docs.append(doc)

        self._skel_index[sh].append(exid)

        self._ledger.append({
            "corpus_id": self.corpus_id,
            "example_id": exid,
            "category": "ownership_moves",
            "stream": "ownership_moves",
            "source_path": src_r,
            "source_sha256": h,
            "formatted_sha256": fh,
            "semantic_skeleton_hash": sh,
            "chunk_surface_skeleton_hash": ex.family,
            "template_id": ex.template_id,
            "phase": phase,
            "expected_outcome": outcome,
            "expected_diagnostic_codes": "|".join(filter(None, codes)),
            "difficulty": ex.difficulty,
            "split": split,
            "contrastive_pair_id": ex.pair or "",
            "family": ex.family,
            "variant_role": ex.role,
            "validation_mode": f"current_{self.compiler_version}",
            "validated_by": phase,
            "taxonomy_nodes": "|".join(ex.taxonomy_nodes),
        })
        return True

    # ── ledger/file synchronization helpers ───────────────────────────────────

    @staticmethod
    def _example_id_from_artifact_name(name: str) -> str | None:
        """Recover example_id from per-example source/label/attribute filenames."""
        for suffix in (".label.json", ".attribute.json", ".skeleton.json", ".az"):
            if name.endswith(suffix):
                return name[: -len(suffix)]
        return None

    def _doc_example_id(self, doc: dict) -> str:
        meta = doc.get("metadata", {})
        if "source_example_id" in meta:
            return meta["source_example_id"]
        return doc.get("id", "").split("__source_to", 1)[0]

    def _rebuild_skeleton_index(self) -> None:
        idx: dict[str, list[str]] = defaultdict(list)
        for r in self._ledger:
            h = r.get("semantic_skeleton_hash", "")
            if h:
                idx[h].append(r["example_id"])
        self._skel_index = idx

    def _rebuild_taxonomy_coverage(self) -> None:
        if self._taxonomy is None:
            return
        nodes = [TaxonomyNode(n.path, n.description) for n in self._taxonomy.nodes]
        self._taxonomy = TaxonomyCoverage(nodes=nodes)
        for r in self._ledger:
            for node_path in filter(None, r.get("taxonomy_nodes", "").split("|")):
                self._taxonomy.mark_covered(node_path, r["example_id"])

    def _sync_files_to_ledger(self) -> None:
        """Remove orphaned per-example artifacts and refresh skeleton cluster sizes."""
        keep_ids = {r["example_id"] for r in self._ledger}

        # Remove stale source/label/attribute files that no longer have ledger rows.
        for rel in list(self._files):
            if "/src/ownership_moves/" not in rel and "/labels/ownership_moves/" not in rel and "/attributes/ownership_moves/" not in rel:
                continue
            eid = self._example_id_from_artifact_name(Path(rel).name)
            if eid is not None and eid not in keep_ids:
                del self._files[rel]

        # Remove stale documents and rebuild skeleton index/taxonomy coverage.
        self._docs = [d for d in self._docs if self._doc_example_id(d) in keep_ids]
        self._rebuild_skeleton_index()
        self._rebuild_taxonomy_coverage()

        # Rewrite skeleton attributes with final cluster_size; this makes the mix
        # recipe's skeleton cap filter executable instead of merely documentary.
        cluster_size = {h: len(ids) for h, ids in self._skel_index.items()}
        for r in self._ledger:
            eid = r["example_id"]
            sh = r["semantic_skeleton_hash"]
            sk_r = f"{self.root}/attributes/ownership_moves/axiomzig_skeleton_v1/{eid}.skeleton.json"
            self._files[sk_r] = json.dumps({
                "schema_version": "axiomzig-skeleton-attribute-v1",
                "corpus_id": self.corpus_id,
                "example_id": eid,
                "semantic_skeleton_hash": sh,
                "cluster_size": cluster_size.get(sh, 1),
                "source_sha256": r["source_sha256"],
            }, indent=2, sort_keys=True).encode("utf-8") + b"\n"

    def _drop_example_ids(self, ids: set[str], reason: str, **meta: object) -> int:
        """Drop ledger/docs/files for examples in ids; return number of ledger rows removed."""
        if not ids:
            return 0
        dropped = [r for r in self._ledger if r["example_id"] in ids]
        if not dropped:
            return 0
        self._ledger = [r for r in self._ledger if r["example_id"] not in ids]
        self._docs = [d for d in self._docs if self._doc_example_id(d) not in ids]
        for r in dropped:
            rec = {"desc": r["example_id"], "reason": reason}
            rec.update(meta)
            if "template_id" not in rec:
                rec["template_id"] = r.get("template_id", "")
            if "contrastive_pair_id" not in rec:
                rec["contrastive_pair_id"] = r.get("contrastive_pair_id", "")
            self._rejected.append(rec)
        self._sync_files_to_ledger()
        return len(dropped)

    def _ids_for_pair_or_single(self, record: dict) -> set[str]:
        pair = record.get("contrastive_pair_id", "")
        if not pair:
            return {record["example_id"]}
        return {
            r["example_id"]
            for r in self._ledger
            if r.get("contrastive_pair_id", "") == pair
        }

    def _template_mixing_preserved_after_drop(self, drop_ids: set[str]) -> bool:
        remaining = [r for r in self._ledger if r["example_id"] not in drop_ids]
        by_template: dict[str, set[str]] = defaultdict(set)
        for r in remaining:
            by_template[r["template_id"]].add(r["expected_outcome"])
        # Any remaining template must stay mixed. Templates may be exhausted only
        # if taxonomy coverage is still preserved; the taxonomy check handles that.
        return all(len(outcomes) > 1 for outcomes in by_template.values())

    def _taxonomy_coverage_preserved_after_drop(self, drop_ids: set[str]) -> bool:
        before: Counter = Counter()
        after: Counter = Counter()
        for r in self._ledger:
            for node_path in filter(None, r.get("taxonomy_nodes", "").split("|")):
                before[node_path] += 1
                if r["example_id"] not in drop_ids:
                    after[node_path] += 1
        return all(after[path] > 0 for path, count in before.items() if count > 0)

    def _consistency_report(self) -> dict:
        """Hard final consistency check for release-grade corpus artifacts."""
        ledger_ids = {r["example_id"] for r in self._ledger}

        def count_artifact(kind: str) -> int:
            prefix = f"{self.root}/{kind}/ownership_moves/"
            return sum(1 for rel in self._files if rel.startswith(prefix))

        source_count = count_artifact("src")
        label_count = count_artifact("labels")
        validator_count = sum(
            1 for rel in self._files
            if rel.startswith(f"{self.root}/attributes/ownership_moves/axiomzig_validator_v1/")
        )
        skeleton_count = sum(
            1 for rel in self._files
            if rel.startswith(f"{self.root}/attributes/ownership_moves/axiomzig_skeleton_v1/")
        )

        orphan_files: list[str] = []
        for rel in self._files:
            if "/src/ownership_moves/" not in rel and "/labels/ownership_moves/" not in rel and "/attributes/ownership_moves/" not in rel:
                continue
            eid = self._example_id_from_artifact_name(Path(rel).name)
            if eid is not None and eid not in ledger_ids:
                orphan_files.append(rel)

        doc_ids = [self._doc_example_id(d) for d in self._docs]
        missing_doc_ids = sorted(ledger_ids - set(doc_ids))
        extra_doc_ids = sorted(set(doc_ids) - ledger_ids)

        pair_issues: list[dict] = []
        by_pair: dict[str, Counter] = defaultdict(Counter)
        for r in self._ledger:
            pair = r.get("contrastive_pair_id", "")
            if pair:
                by_pair[pair][r["expected_outcome"]] += 1
        for pair, counts in sorted(by_pair.items()):
            if counts.get("pass", 0) == 0 or counts.get("fail", 0) == 0:
                pair_issues.append({"pair": pair, "counts": dict(counts)})

        template_issues: list[dict] = []
        by_template: dict[str, Counter] = defaultdict(Counter)
        for r in self._ledger:
            by_template[r["template_id"]][r["expected_outcome"]] += 1
        for template_id, counts in sorted(by_template.items()):
            if counts.get("pass", 0) == 0 or counts.get("fail", 0) == 0:
                template_issues.append({"template_id": template_id, "counts": dict(counts)})

        expected_docs = 2 * len(self._ledger)
        failures: list[str] = []
        if source_count != len(self._ledger):
            failures.append("source_count_mismatch")
        if label_count != len(self._ledger):
            failures.append("label_count_mismatch")
        if validator_count != len(self._ledger):
            failures.append("validator_attribute_count_mismatch")
        if skeleton_count != len(self._ledger):
            failures.append("skeleton_attribute_count_mismatch")
        if len(self._docs) != expected_docs:
            failures.append("document_count_mismatch")
        if orphan_files:
            failures.append("orphan_per_example_files")
        if missing_doc_ids or extra_doc_ids:
            failures.append("document_id_mismatch")
        if pair_issues:
            failures.append("broken_contrastive_pairs")
        if template_issues:
            failures.append("uniform_remaining_templates")

        return {
            "status": "fail" if failures else "pass",
            "hard_fail_reasons": failures,
            "ledger_records": len(self._ledger),
            "source_files": source_count,
            "label_files": label_count,
            "validator_attribute_files": validator_count,
            "skeleton_attribute_files": skeleton_count,
            "document_records": len(self._docs),
            "expected_document_records": expected_docs,
            "orphan_files": sorted(orphan_files),
            "missing_document_example_ids": missing_doc_ids,
            "extra_document_example_ids": extra_doc_ids,
            "broken_contrastive_pairs": pair_issues,
            "uniform_remaining_templates": template_issues,
        }


    # ── post-processing filters ───────────────────────────────────────────────

    def filter_uniform_templates(self) -> int:
        """Remove templates that have only one outcome (making them perfect predictors)."""
        by_t: dict[str, set] = defaultdict(set)
        for r in self._ledger:
            by_t[r["template_id"]].add(r["expected_outcome"])
        uniform = {t for t, outcomes in by_t.items() if len(outcomes) == 1}
        if not uniform:
            return 0
        drop_ids = {r["example_id"] for r in self._ledger if r["template_id"] in uniform}
        return self._drop_example_ids(drop_ids, "uniform_template")

    def reduce_diagnostic_skew(
        self,
        max_pct: float = 0.40,
        max_iters: int = 400,
        preserve_pairs: bool = True,
        preserve_taxonomy: bool = True,
    ) -> int:
        """Greedy removal of over-represented diagnostic codes.

        By default this drops whole contrastive pairs, not isolated failures.
        That keeps the corpus useful for minimal-edit contrastive training and
        prevents the skew reducer from silently breaking pair completeness.
        """

        def diag_counts() -> Counter:
            dc: Counter = Counter()
            for r in self._ledger:
                if r["expected_outcome"] == "fail":
                    for c in filter(None, r["expected_diagnostic_codes"].split("|")):
                        dc[c] += 1
            return dc

        dropped = 0
        for _ in range(max_iters):
            dc = diag_counts()
            total = sum(dc.values())
            if not total:
                break
            code, cnt = dc.most_common(1)[0]
            if cnt / total <= max_pct:
                break

            removed = False
            for r in list(self._ledger):
                if r["expected_outcome"] != "fail":
                    continue
                if code not in r["expected_diagnostic_codes"].split("|"):
                    continue

                drop_ids = self._ids_for_pair_or_single(r) if preserve_pairs else {r["example_id"]}
                if not self._template_mixing_preserved_after_drop(drop_ids):
                    continue
                if preserve_taxonomy and not self._taxonomy_coverage_preserved_after_drop(drop_ids):
                    continue

                n = self._drop_example_ids(drop_ids, "skew_reduction", code=code)
                if n:
                    dropped += n
                    removed = True
                    break

            if not removed:
                break
        return dropped

    # ── finalize ──────────────────────────────────────────────────────────────

    def finalize(
        self,
        out_zip: Path,
        script_text: str = "",
        extra_schemas: dict[str, str] | None = None,
        mix_yaml: str = "",
    ) -> dict:
        """Write all artifacts and produce the output zip."""

        # Finalize post-filter state before writing aggregate ledgers/reports.
        self._sync_files_to_ledger()
        consistency = self._consistency_report()

        # JSONL document streams
        payload = "\n".join(json.dumps(d, sort_keys=True) for d in self._docs) + "\n"
        self._put("documents/all_documents.jsonl", payload)
        self._put(
            f"documents/ownership_moves/{self.corpus_id}.jsonl.gz",
            gzip.compress(payload.encode("utf-8")),
        )

        # Ledger
        self._put("ledgers/ledger.jsonl",
                  "\n".join(json.dumps(r, sort_keys=True) for r in self._ledger) + "\n")
        if self._ledger:
            csv_buf = io.StringIO()
            writer = csv.DictWriter(csv_buf, fieldnames=list(self._ledger[0].keys()))
            writer.writeheader()
            writer.writerows(self._ledger)
            self._put("ledgers/ledger.csv", csv_buf.getvalue())
        self._put("ledgers/source_hashes.sha256.txt",
                  "\n".join(f"{r['source_sha256']}  {self.root}/{r['source_path']}"
                             for r in self._ledger) + "\n")
        self._put("ledgers/semantic_skeleton_index.jsonl",
                  "\n".join(json.dumps({"h": h, "n": len(v), "ids": v}, sort_keys=True)
                             for h, v in self._skel_index.items()) + "\n")

        # Chunk risk
        risk = compute_chunk_risk(self._ledger)
        self._put("validation/chunk_risk_report.json",
                  json.dumps(risk, indent=2, sort_keys=True) + "\n")
        self._put("validation/consistency_report.json",
                  json.dumps(consistency, indent=2, sort_keys=True) + "\n")

        # Taxonomy coverage
        tax_dict = self._taxonomy.to_dict() if self._taxonomy else {}
        self._put("validation/taxonomy_coverage.json",
                  json.dumps(tax_dict, indent=2, sort_keys=True) + "\n")

        # Complexity profile
        cplx = complexity_profile(self._ledger)

        # Validation report
        out_counts = Counter(r["expected_outcome"] for r in self._ledger)
        validation = {
            "corpus_id": self.corpus_id,
            "bundle_name": self.bundle_name,
            "compiler_version": self.compiler_version,
            "source_records": len(self._ledger),
            "document_records": len(self._docs),
            "attribute_records": len(self._ledger),
            "outcome_counts": dict(out_counts),
            "negative_ratio": round(
                out_counts.get("fail", 0) / max(1, len(self._ledger)), 6
            ),
            "distinct_diagnostic_codes": risk["distinct_diagnostic_codes"],
            "max_single_diagnostic_pct": risk["max_single_diagnostic_pct"],
            "chunk_risk_status": risk["status"],
            "chunk_risk": risk,
            "consistency_status": consistency["status"],
            "consistency": consistency,
            "taxonomy_coverage": tax_dict,
            "complexity_profile": cplx,
            "label_agreement": {"full": len(self._ledger), "mismatch": 0},
            "rejected_candidates": len(self._rejected),
            "accepted": risk["status"] == "pass" and consistency["status"] == "pass",
        }
        self._put("validation/validation_report.json",
                  json.dumps(validation, indent=2, sort_keys=True) + "\n")
        self._put("validation/rejected_candidates.json",
                  json.dumps(self._rejected, indent=2) + "\n")

        # Quality report (Markdown)
        neg = out_counts.get("fail", 0)
        qr_lines = [
            f"# Quality Report — {self.bundle_name}\n",
            f"**Records:** {len(self._ledger)}  |  **Documents:** {len(self._docs)}",
            f"**Negative ratio:** {neg}/{len(self._ledger)} "
            f"({100*neg//max(1,len(self._ledger))}%)",
            f"**Chunk risk:** {risk['status']}",
            f"  Template MI: {risk['template_outcome_mi_bits']:.4f} bits",
            f"  Surface MI:  {risk['chunk_surface_mi_bits']:.4f} bits",
            f"  Max diag %:  {risk['max_single_diagnostic_pct']:.1%}",
            f"**Distinct codes:** {risk['distinct_diagnostic_codes']}",
            "",
            "### Diagnostic distribution",
        ]
        for code, cnt in sorted(risk["diagnostic_code_counts"].items()):
            qr_lines.append(f"- `{code}`: {cnt}")
        qr_lines += [
            "",
            "### Taxonomy coverage",
            f"- Covered: {tax_dict.get('covered_nodes', '?')}/{tax_dict.get('total_nodes', '?')} "
            f"nodes ({tax_dict.get('coverage_ratio', 0):.1%})",
        ]
        for p in tax_dict.get("covered_paths", []):
            qr_lines.append(f"  ✓ {p}")
        for p in tax_dict.get("missing_paths", []):
            qr_lines.append(f"  ✗ {p}")
        qr_lines += ["", "### Complexity profile"]
        for tier in ("easy", "medium", "hard"):
            qr_lines.append(f"- {tier}: {cplx.get(tier, 0):.1%}")
        self._put("reports/quality_report.md", "\n".join(qr_lines))
        self._put("reports/expansion_strategy.md",
                  "# Expansion Strategy\n\nNext: continue building S3 taxonomy coverage. "
                  "Uncovered nodes (see taxonomy_coverage.json) should be targeted first.\n")

        # Schemas
        default_schemas = {
            "label_v2.schema.json": json.dumps({"title": "axiomzig-corpus-label-v2"}, indent=2),
            "validation_attribute_v1.schema.json": json.dumps({"title": "axiomzig-validation-attribute-v1"}, indent=2),
            "document.schema.json": json.dumps({"title": "Dolma document schema"}, indent=2),
            "chunk_risk_v1.schema.json": json.dumps({"title": "Chunk Risk Audit v1"}, indent=2),
        }
        if extra_schemas:
            default_schemas.update(extra_schemas)
        for name, content in default_schemas.items():
            self._put(f"schemas/{name}", content + "\n")

        # Mix recipe
        if mix_yaml:
            self._put("mixes/phase1_semantic.yaml", mix_yaml)

        # Tools
        if script_text:
            self._put("tools/generator.py", script_text)
        framework_src = Path(__file__).read_text(encoding="utf-8")
        self._put("tools/az_corpus_framework.py", framework_src)

        # MANIFEST + README
        self._put("MANIFEST.json", json.dumps({
            "corpus_id": self.corpus_id,
            "bundle_name": self.bundle_name,
            "compiler_version": self.compiler_version,
            "source_records": len(self._ledger),
            "document_records": len(self._docs),
            "chunk_risk_status": risk["status"],
            "consistency_status": consistency["status"],
            "taxonomy_coverage_ratio": tax_dict.get("coverage_ratio", 0),
            "accepted": risk["status"] == "pass" and consistency["status"] == "pass",
        }, indent=2, sort_keys=True) + "\n")
        self._put("README.md",
                  f"# {self.bundle_name}\n\n"
                  f"Records: {len(self._ledger)}  |  "
                  f"Chunk risk: **{risk['status']}**  |  "
                  f"Taxonomy coverage: {tax_dict.get('covered_nodes','?')}/{tax_dict.get('total_nodes','?')}\n")

        # Write zip
        if out_zip.exists():
            out_zip.unlink()
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
            for rel, data in sorted(self._files.items()):
                z.writestr(rel, data)
        bad     = zipfile.ZipFile(out_zip).testzip()
        entries = len(zipfile.ZipFile(out_zip).namelist())

        return {
            "zip_file": str(out_zip),
            "zip_size_bytes": out_zip.stat().st_size,
            "zip_size_kb": out_zip.stat().st_size // 1024,
            "zip_entries": entries,
            "zip_integrity_error": bad,
            "zip_sha256": sha_bytes(out_zip.read_bytes()),
            "source_records": len(self._ledger),
            "document_records": len(self._docs),
            "rejected_candidates": len(self._rejected),
            "chunk_risk_status": risk["status"],
            "chunk_risk_hard_fail_reasons": risk["hard_fail_reasons"],
            "consistency_status": consistency["status"],
            "consistency_hard_fail_reasons": consistency["hard_fail_reasons"],
            "diagnostic_code_counts": risk["diagnostic_code_counts"],
            "taxonomy_coverage": tax_dict,
            "complexity_profile": cplx,
            "accepted": risk["status"] == "pass" and consistency["status"] == "pass",
        }
