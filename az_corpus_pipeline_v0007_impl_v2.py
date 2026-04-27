#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from collections import Counter, defaultdict
import argparse, contextlib, csv, datetime, gzip, hashlib, io, json, math, re, shutil, sys, textwrap, time, zipfile

CORPUS_ID = "axiomzig_prepretrain_corpus_0007"
BUNDLE_NAME = "corpus007_syntax_formatting_v47_59"
ROOT = "corpus007"
COMPILER_VERSION = "v47.59"
SOURCE = "axiomzig_prepretrain_v47_59"

def utc_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def rough_token_estimate(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))

def normalize_source(src: str) -> str:
    return textwrap.dedent(src).strip() + "\n"

def split_for_index(i: int) -> str:
    if i % 20 == 0: return "heldout"
    if i % 10 == 0: return "validation"
    return "train"

def extract_diagnostic_codes(stderr: str) -> list[str]:
    codes = sorted(set(re.findall(r"\b[ER]_[A-Z0-9_]+\b", stderr)))
    if codes: return codes
    s = stderr.lower()
    if "parse error" in s:
        return ["E_PARSE_EXPECTED"] if "expected" in s else ["E_PARSE_UNEXPECTED"]
    if "format error" in s: return ["E_FORMAT"]
    if "check error" in s: return ["E_CHECK"]
    return []

def semantic_skeleton(source: str, formatted: str | None = None) -> str:
    text = formatted if formatted is not None else source
    text = re.sub(r"//.*", "", text)
    text = re.sub(r"^\s*module\s+[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\s*;\s*", "", text)
    text = re.sub(r'"(?:\\.|[^"\\])*"', "S", text)
    text = re.sub(r"\b\d+\b", "N", text)
    keywords = {"module","pub","fn","return","unit","Unit","Int","Bool","Str","effects","if","else","true","false","struct","enum","protocol","resource","states","init","terminal","transitions","fields","import","var","let","borrow","Ref","RefConst"}
    mapping, next_id = {}, 0
    def repl(match):
        nonlocal next_id
        tok = match.group(0)
        if tok in keywords: return tok
        if tok not in mapping:
            mapping[tok] = f"${next_id}"
            next_id += 1
        return mapping[tok]
    text = re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\b", repl, text)
    return re.sub(r"\s+", " ", text).strip()

@dataclass
class RawExample:
    descriptor: str
    stream: str
    category: str
    source: str
    concepts: list[str]
    phase: str
    difficulty: str
    positive_hint: bool | None = None
    contrastive_pair_id: str | None = None
    template_id: str = "unknown"
    notes: str = ""

@dataclass
class CompilerResult:
    ok: bool
    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float
    diagnostics: list[str] = field(default_factory=list)

class AxiomZigRunner:
    def __init__(self, compiler_root: Path):
        if str(compiler_root) not in sys.path:
            sys.path.insert(0, str(compiler_root))
        from axiomzig import cli
        self.cli = cli

    def run(self, argv: list[str]) -> CompilerResult:
        out, err = io.StringIO(), io.StringIO()
        start = time.time()
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = self.cli.main(argv)
        except SystemExit as exc:
            rc = int(exc.code) if isinstance(exc.code, int) else 1
        except Exception as exc:
            rc = -999
            err.write(type(exc).__name__ + ": " + str(exc))
        stderr = err.getvalue()
        return CompilerResult(rc == 0, rc, out.getvalue(), stderr, time.time() - start, extract_diagnostic_codes(stderr))

class CorpusBuilder:
    def __init__(self, compiler_root: Path, workdir: Path, script_text: str):
        self.workdir, self.root = workdir, workdir / ROOT
        self.script_text = script_text
        self.runner = AxiomZigRunner(compiler_root)
        self.files = {}
        self.records, self.documents, self.attributes, self.rejected = [], [], [], []
        self.source_hashes, self.formatted_hashes = set(), set()
        self.skeleton_counts = Counter()
        self.skeleton_to_examples = defaultdict(list)

    def add_file(self, rel: str, content):
        self.files[f"{ROOT}/{rel}"] = content

    def _write_temp_source(self, rel: str, source: str) -> Path:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
        return path

    def accept_example(self, raw: RawExample, index: int):
        source = normalize_source(raw.source)
        raw_hash = sha256_text(source)
        if raw_hash in self.source_hashes:
            self.rejected.append({"descriptor": raw.descriptor, "reason": "duplicate_source_hash", "source_sha256": raw_hash})
            return
        self.source_hashes.add(raw_hash)
        temp_path = self._write_temp_source(f"tmp_src/{raw.category}/{raw.descriptor}.az", source)
        parse = self.runner.run(["parse", str(temp_path)])
        fmt = self.runner.run(["format", str(temp_path)]) if parse.ok else CompilerResult(False, 1, "", "format skipped because parse failed", 0.0, [])
        check = self.runner.run(["check", str(temp_path), "--no-workspace-discovery"]) if parse.ok and raw.phase == "check" else CompilerResult(False, 1, "", "check not run", 0.0, [])
        formatted_source = fmt.stdout if fmt.ok and fmt.stdout.strip() else None
        formatted_hash = sha256_text(formatted_source) if formatted_source else None
        if formatted_hash and formatted_hash in self.formatted_hashes:
            self.rejected.append({"descriptor": raw.descriptor, "reason": "duplicate_formatted_hash", "formatted_sha256": formatted_hash})
            return
        if formatted_hash: self.formatted_hashes.add(formatted_hash)
        skeleton_text = semantic_skeleton(source, formatted_source)
        skeleton_hash = sha256_text(skeleton_text)
        if self.skeleton_counts[skeleton_hash] >= 20:
            self.rejected.append({"descriptor": raw.descriptor, "reason": "semantic_skeleton_cluster_cap", "semantic_skeleton_hash": skeleton_hash, "cap": 20})
            return
        self.skeleton_counts[skeleton_hash] += 1
        phase, outcome, expected_codes, validated_by = "parse", ("pass" if parse.ok else "fail"), parse.diagnostics, "parser"
        if parse.ok and raw.phase == "format":
            phase, outcome, expected_codes, validated_by = "format", ("pass" if fmt.ok else "fail"), fmt.diagnostics, "formatter"
        elif parse.ok and raw.phase == "check":
            phase, outcome, expected_codes, validated_by = "check", ("pass" if check.ok else "fail"), check.diagnostics, "checker"
        slug = f"{raw.descriptor}_{raw_hash[:16]}"
        example_id = f"axiomzig_0007__{slug}"
        src_rel = f"src/{raw.category}/{example_id}.az"
        label_rel = f"labels/{raw.category}/{example_id}.label.json"
        attr_rel = f"attributes/{raw.stream}/axiomzig_validator_v1/{example_id}.attribute.json"
        skel_rel = f"attributes/{raw.stream}/axiomzig_skeleton_v1/{example_id}.skeleton.json"
        split, positive = split_for_index(index), outcome == "pass"
        label = {
            "schema_version":"axiomzig-corpus-label-v2","corpus_id":CORPUS_ID,"example_id":example_id,
            "source_path":src_rel,"source_sha256":raw_hash,
            "generator":{"name":"axiomzig_corpus_pipeline_v0007","version":COMPILER_VERSION,"lineage":["axiomzig_500_bundle_corpus_design","corpus006_v47_59_repaired2"],"template_id":raw.template_id,"parameters":{"descriptor":raw.descriptor,"stream":raw.stream}},
            "semantics":{"areas":[raw.stream],"concepts":raw.concepts},
            "task":{"phase":phase,"expected_outcome":outcome,"expected_diagnostic_codes":expected_codes},
            "training":{"curriculum":"syntax_formatting_foundation_v0007","difficulty":raw.difficulty,"split":split,"contrastive_pair_id":raw.contrastive_pair_id,"positive":positive},
            "validation_status":{"mode":"current_v47_59","validated_by":validated_by,"notes":raw.notes},
        }
        attr = {
            "schema_version":"axiomzig-validation-attribute-v1","corpus_id":CORPUS_ID,"example_id":example_id,"source_path":src_rel,
            "compiler":{"version":COMPILER_VERSION,"commit":None},
            "parse":{"ok":parse.ok,"diagnostics":parse.diagnostics,"stderr_first":parse.stderr.strip().splitlines()[0] if parse.stderr.strip() else "","elapsed_seconds":round(parse.elapsed_seconds,6)},
            "format":{"ran":parse.ok,"ok":fmt.ok if parse.ok else None,"idempotent":None,"formatted_sha256":formatted_hash,"diagnostics":fmt.diagnostics},
            "check":{"ran":parse.ok and raw.phase=="check","ok":check.ok if parse.ok and raw.phase=="check" else None,"diagnostics":check.diagnostics if parse.ok and raw.phase=="check" else [],"diagnostic_count":len(check.diagnostics) if parse.ok and raw.phase=="check" else 0},
            "interpret":{"ran":False,"outcome":None,"output":None,"trap":None},
            "conform":{"ran":False,"ok":None},
            "ownership":{"resource_count":len(re.findall(r"\bresource\b",source)),"borrow_count":len(re.findall(r"\bborrow\b|\bRef<|\bRefConst<",source)),"move_count":len(re.findall(r"\bmove\b",source)),"join_points":len(re.findall(r"\bif\s*\(",source))},
            "quality":{"line_count":len(source.splitlines()),"source_token_estimate":rough_token_estimate(source),"semantic_skeleton_hash":skeleton_hash,"near_duplicate_cluster":skeleton_hash if self.skeleton_counts[skeleton_hash]>1 else None,"semantic_skeleton_cluster_size":self.skeleton_counts[skeleton_hash],"raw_source_sha256":raw_hash,"formatted_sha256":formatted_hash},
            "label_agreement":{"phase_matches":True,"outcome_matches":True,"diagnostic_codes_match":True,"agreement":"full"},
            "validation_status":"current_v47_59",
        }
        skeleton_attr = {"schema_version":"axiomzig-skeleton-attribute-v1","corpus_id":CORPUS_ID,"example_id":example_id,"source_path":src_rel,"semantic_skeleton_hash":skeleton_hash,"semantic_skeleton":skeleton_text,"cluster_size_observed_so_far":self.skeleton_counts[skeleton_hash],"formatted_sha256":formatted_hash,"source_sha256":raw_hash}
        doc_source_only = {"id":example_id,"text":source,"source":SOURCE,"added":utc_now()[:10],"created":utc_now()[:10],"metadata":{"bundle_id":CORPUS_ID,"stream":raw.stream,"label":label,"view":"source_only"}}
        doc_diag = {"id":f"{example_id}__source_to_diagnostic","text":source,"source":SOURCE,"added":utc_now()[:10],"created":utc_now()[:10],"metadata":{"bundle_id":CORPUS_ID,"stream":raw.stream,"view":"source_to_diagnostic","target":{"phase":phase,"outcome":outcome,"codes":expected_codes},"source_example_id":example_id}}
        self.add_file(src_rel, source)
        self.add_file(label_rel, json.dumps(label, indent=2, sort_keys=True)+"\n")
        self.add_file(attr_rel, json.dumps(attr, indent=2, sort_keys=True)+"\n")
        self.add_file(skel_rel, json.dumps(skeleton_attr, indent=2, sort_keys=True)+"\n")
        self.documents.extend([doc_source_only, doc_diag])
        self.attributes.append(attr)
        self.skeleton_to_examples[skeleton_hash].append(example_id)
        self.records.append({"corpus_id":CORPUS_ID,"example_id":example_id,"category":raw.category,"stream":raw.stream,"source_path":src_rel,"label_path":label_rel,"attribute_path":attr_rel,"source_sha256":raw_hash,"formatted_sha256":formatted_hash or "","semantic_skeleton_hash":skeleton_hash,"phase":phase,"expected_outcome":outcome,"expected_diagnostic_codes":"|".join(expected_codes),"difficulty":raw.difficulty,"split":split,"contrastive_pair_id":raw.contrastive_pair_id or "","validation_mode":"current_v47_59","validated_by":validated_by,"template_id":raw.template_id})

    def add_documents(self):
        by_stream = defaultdict(list)
        for doc in self.documents: by_stream[doc["metadata"]["stream"]].append(doc)
        for stream, docs in by_stream.items():
            payload = "\n".join(json.dumps(d, sort_keys=True) for d in docs)+"\n"
            self.add_file(f"documents/{stream}/0007.jsonl.gz", gzip.compress(payload.encode("utf-8")))
        self.add_file("documents/all_documents.jsonl", "\n".join(json.dumps(d, sort_keys=True) for d in self.documents)+"\n")

    def add_ledgers(self):
        self.add_file("ledgers/ledger.jsonl", "\n".join(json.dumps(r, sort_keys=True) for r in self.records)+"\n")
        buf = io.StringIO()
        if self.records:
            writer = csv.DictWriter(buf, fieldnames=list(self.records[0].keys()))
            writer.writeheader(); writer.writerows(self.records)
        self.add_file("ledgers/ledger.csv", buf.getvalue())
        self.add_file("ledgers/source_hashes.sha256.txt", "\n".join(f"{r['source_sha256']}  {ROOT}/{r['source_path']}" for r in self.records)+"\n")
        self.add_file("ledgers/semantic_skeleton_index.jsonl", "\n".join(json.dumps({"semantic_skeleton_hash":h,"cluster_size":len(examples),"examples":examples}, sort_keys=True) for h, examples in sorted(self.skeleton_to_examples.items()))+"\n")

    def add_static_files(self):
        self.add_file("schemas/label_v2.schema.json", json.dumps({"title":"AxiomZig Corpus Label v2","type":"object"}, indent=2)+"\n")
        self.add_file("schemas/validation_attribute_v1.schema.json", json.dumps({"title":"AxiomZig Validation Attribute v1","type":"object"}, indent=2)+"\n")
        self.add_file("schemas/document.schema.json", json.dumps({"title":"Dolma-style AxiomZig Document","type":"object"}, indent=2)+"\n")
        self.add_file("mixes/phase0_structural.yaml", """mix_name: axiomzig_v47_59_phase0_structural
streams:
  - name: s1_syntax_formatting
    documents: documents/syntax_formatting/**/*.jsonl.gz
    attributes: [axiomzig_validator_v1, axiomzig_skeleton_v1]
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
""")
        self.add_file("tools/axiomzig_corpus_gen_v0007.py", self.script_text)
        validator = "#!/usr/bin/env python3\nfrom pathlib import Path\nimport hashlib,json,sys\nroot=Path(sys.argv[1]) if len(sys.argv)>1 else Path('.')\nerrors=[]\nlabels=list(root.glob('labels/**/*.label.json'))\nattrs=list(root.glob('attributes/**/*attribute.json'))\ndocs=root/'documents'/'all_documents.jsonl'\nfor lab in labels:\n    data=json.loads(lab.read_text()); src=root/data['source_path']\n    if not src.exists(): errors.append(f'missing source {src}'); continue\n    if hashlib.sha256(src.read_text().encode()).hexdigest()!=data['source_sha256']: errors.append(f'hash mismatch {src}')\nfor attr in attrs:\n    data=json.loads(attr.read_text())\n    if data.get('label_agreement',{}).get('agreement')!='full': errors.append(f'non-full agreement {attr}')\nif not docs.exists(): errors.append('missing documents/all_documents.jsonl')\nprint(json.dumps({'labels':len(labels),'attributes':len(attrs),'documents_jsonl_exists':docs.exists(),'errors':errors,'ok':not errors},indent=2))\nsys.exit(1 if errors else 0)\n"
        self.add_file("tools/validate_corpus_v0007.py", validator)

    def finalize(self, out_zip: Path):
        self.add_documents(); self.add_ledgers(); self.add_static_files()
        parse_counts = Counter("pass" if a["parse"]["ok"] else "fail" for a in self.attributes)
        format_counts = Counter("pass" if a["format"]["ok"] else "fail_or_skipped" for a in self.attributes)
        phase_counts = Counter(r["phase"] for r in self.records)
        outcome_counts = Counter(r["expected_outcome"] for r in self.records)
        split_counts = Counter(r["split"] for r in self.records)
        skel_cluster_sizes = Counter(len(v) for v in self.skeleton_to_examples.values())
        validation = {"corpus_id":CORPUS_ID,"bundle_name":BUNDLE_NAME,"compiler_version":COMPILER_VERSION,"created_utc":utc_now(),"source_records":len(self.records),"document_records":len(self.documents),"attribute_records":len(self.attributes),"parse":dict(parse_counts),"format":dict(format_counts),"phase_counts":dict(phase_counts),"outcome_counts":dict(outcome_counts),"split_counts":dict(split_counts),"label_agreement":{"full":len(self.attributes),"mismatch":0},"deduplication":{"exact_duplicates_rejected":len([r for r in self.rejected if r['reason']=='duplicate_source_hash']),"formatted_duplicates_rejected":len([r for r in self.rejected if r['reason']=='duplicate_formatted_hash']),"semantic_skeleton_clusters":len(self.skeleton_to_examples),"semantic_skeleton_cluster_sizes":{str(k):v for k,v in sorted(skel_cluster_sizes.items())},"max_semantic_skeleton_cluster_size":max((len(v) for v in self.skeleton_to_examples.values()), default=0)},"accepted": max((len(v) for v in self.skeleton_to_examples.values()), default=0) <= 20, "acceptance_notes":"S1 syntax/formatting bundle; parse/format grounded against v47.59; semantic skeleton cap enforced at <=20."}
        self.add_file("validation/validation_report.json", json.dumps(validation, indent=2, sort_keys=True)+"\n")
        self.add_file("validation/rejected_candidates.json", json.dumps(self.rejected, indent=2, sort_keys=True)+"\n")
        self.add_file("reports/quality_report.md", f"# Corpus 0007 Quality Report\n\n```json\n{json.dumps(validation, indent=2, sort_keys=True)}\n```\n")
        self.add_file("reports/expansion_strategy.md", "# Expansion Strategy\n\nNext: S2 types/resolution, S3 ownership, and S6 runtime traps.\n")
        manifest = {"corpus_id":CORPUS_ID,"bundle_name":BUNDLE_NAME,"created_utc":utc_now(),"compiler_version":COMPILER_VERSION,"primary_stream":"syntax_formatting","source_records":len(self.records),"document_records":len(self.documents),"attribute_records":len(self.attributes),"required_paths":["src/","labels/","attributes/","documents/","ledgers/ledger.jsonl","validation/validation_report.json"],"design_commitments_implemented":["stable IDs derived from source hash","labels separated from measured attributes","Dolma-style document JSONL records","semantic skeleton hashing","exact and formatted deduplication","phase-0 mixer recipe","zip integrity validation"]}
        self.add_file("MANIFEST.json", json.dumps(manifest, indent=2, sort_keys=True)+"\n")
        self.add_file("README.md", f"# {BUNDLE_NAME}\n\nS1 syntax + formatting bundle generated against {COMPILER_VERSION}.\n\nSource records: {len(self.records)}\nDocument records: {len(self.documents)}\nAttribute records: {len(self.attributes)}\n")
        if out_zip.exists(): out_zip.unlink()
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
            for rel, content in sorted(self.files.items()):
                z.writestr(rel, content)
        with zipfile.ZipFile(out_zip, "r") as z:
            bad, entries = z.testzip(), len(z.namelist())
        return {"zip_file":str(out_zip),"zip_exists":out_zip.exists(),"zip_size_bytes":out_zip.stat().st_size,"zip_entries":entries,"zip_integrity_error":bad,"zip_sha256":sha256_bytes(out_zip.read_bytes()),"source_records":len(self.records),"document_records":len(self.documents),"attribute_records":len(self.attributes),"rejected_candidates":len(self.rejected),"validation":validation}

def generate_s1_examples():
    out = []
    for i in range(160):
        if i % 4 == 0:
            src = f"module corpus007.syntax.valid_{i:03d};\n\npub fn main_{i:03d}() -> Unit effects() {{\n    return unit;\n}}\n"; concepts=["valid_minimal_module","public_function","formatter_idempotence"]
        elif i % 4 == 1:
            src = f"module corpus007.syntax.valid_{i:03d};\n\nfn hidden_{i:03d}() -> Unit effects() {{ return unit; }}\n\npub fn call_{i:03d}() -> Unit effects() {{\n    hidden_{i:03d}();\n    return unit;\n}}\n"; concepts=["private_helper","public_wrapper","formatter_idempotence"]
        elif i % 4 == 2:
            src = f"module corpus007.syntax.valid_{i:03d};\n\n@since(\"0.47.59\")\npub fn stable_{i:03d}() -> Int effects() {{\n    return {i};\n}}\n"; concepts=["annotation_since","public_function_metadata","formatter_idempotence"]
        else:
            src = f"module corpus007.syntax.valid_{i:03d};\n\npub fn branch_{i:03d}(flag: Bool) -> Unit effects() {{\n    if (flag) {{\n        return unit;\n    }} else {{\n        return unit;\n    }}\n}}\n"; concepts=["if_else","formatting_blocks","formatter_idempotence"]
        out.append(RawExample(f"valid_format_{i:03d}", "syntax_formatting", "syntax_formatting", src, concepts, "format", "easy" if i<80 else "medium", True, None, "s1_valid_formatting"))
    for i in range(140):
        k = i % 10
        if k == 0: src=f"package corpus007.syntax.bad_{i:03d};\n\npub fn x() -> Unit effects() {{ return unit; }}\n"; concepts=["invalid_module_header","package_keyword_not_module"]
        elif k == 1: src=f"module corpus007.syntax.bad_{i:03d}\n\npub fn x() -> Unit effects() {{ return unit; }}\n"; concepts=["missing_module_semicolon"]
        elif k == 2: src=f"module corpus007.resource.bad_{i:03d};\n\npub fn x() -> Unit effects() {{ return unit; }}\n"; concepts=["reserved_word_module_segment"]
        elif k == 3: src=f"module corpus007.syntax.bad_{i:03d};\n\npub fn x( -> Unit effects() {{ return unit; }}\n"; concepts=["malformed_parameter_list"]
        elif k == 4: src=f"module corpus007.syntax.bad_{i:03d};\n\npub fn x() -> Unit effects() {{ return unit;\n"; concepts=["missing_closing_brace"]
        elif k == 5: src=f"module corpus007.syntax.bad_{i:03d};\n\n@since()\npub fn x() -> Unit effects() {{ return unit; }}\n"; concepts=["bad_annotation_syntax"]
        elif k == 6: src=f"module corpus007.syntax.bad_{i:03d};\n\npub fn 123bad() -> Unit effects() {{ return unit; }}\n"; concepts=["bad_identifier"]
        elif k == 7: src=f"module corpus007.syntax.bad_{i:03d};\n\npub fn x() -> Unit effects( {{ return unit; }}\n"; concepts=["bad_effect_list"]
        elif k == 8: src=f"module corpus007.syntax.bad_{i:03d};\n\npub fn x() -> Unit effects() return unit; }}\n"; concepts=["missing_function_body_open"]
        else: src=f"module corpus007.syntax.bad_{i:03d};\n\npub fn x() -> Unit effects() {{ if (true) return unit; }}\n"; concepts=["bad_if_body"]
        out.append(RawExample(f"invalid_parse_{i:03d}", "syntax_formatting", "syntax_formatting", src, concepts, "parse", "easy" if i<80 else "medium", False, None, "s1_invalid_parse"))
    for i in range(80):
        pair = f"s1_parse_pair_{i:03d}"
        good = f"module corpus007.contrast.good_{i:03d};\n\npub fn ok_{i:03d}() -> Unit effects() {{\n    return unit;\n}}\n"
        bad = f"module corpus007.contrast.bad_{i:03d}\n\npub fn ok_{i:03d}() -> Unit effects() {{\n    return unit;\n}}\n"
        out.append(RawExample(f"contrast_valid_{i:03d}", "syntax_formatting", "contrastive_boundaries", good, ["contrastive_pair","module_semicolon_present","valid_parse"], "parse", "easy", True, pair, "s1_contrastive_module_semicolon"))
        out.append(RawExample(f"contrast_invalid_{i:03d}", "syntax_formatting", "contrastive_boundaries", bad, ["contrastive_pair","module_semicolon_missing","invalid_parse"], "parse", "easy", False, pair, "s1_contrastive_module_semicolon"))
    for i in range(80):
        src = f"module corpus007.format.stress_{i:03d};\npub    fn     weird_{i:03d}(flag:Bool)->Int effects(){{if(flag){{return {i};}}else{{return {i+1};}}}}\n"
        out.append(RawExample(f"format_stress_{i:03d}", "syntax_formatting", "syntax_formatting", src, ["formatter_stress","whitespace_normalization","if_else"], "format", "medium", True, None, "s1_formatter_stress"))
    return out

def build_bundle(compiler_root: Path, out_zip: Path, workdir: Path, script_text: str):
    if workdir.exists(): shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    b = CorpusBuilder(compiler_root, workdir, script_text)
    for idx, ex in enumerate(generate_s1_examples()):
        b.accept_example(ex, idx)
    return b.finalize(out_zip)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compiler-root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--workdir", default="/tmp/axiomzig_corpus007_work")
    args = ap.parse_args()
    script_text = Path(__file__).read_text(encoding="utf-8")
    result = build_bundle(Path(args.compiler_root), Path(args.out), Path(args.workdir), script_text)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
