"""
Spike: run every report against PostgreSQL (baseline) and against a
read-only parquet:// bundle, and emit a compatibility matrix.

Key detail: report.example_input() returns a FULL kwargs dict for most
reports (not just input_data), so it must be splatted, not wrapped.

Set SPIKE_JSON_FIX=1 to apply the candidate json_deserializer fix to the
DuckDB engine and measure the post-fix state.

Outcome per report / per backend:
  OK        - ran, returned rows
  EMPTY     - ran, returned 0 rows (inconclusive if source data is empty)
  PARAM     - needs params/files the spike can't supply
  ERROR     - raised; message captured
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from pathlib import Path

import pandas as pd

SP = Path(__file__).resolve().parent
BUNDLE = Path(sys.argv[1] if len(sys.argv) > 1 else SP / "spike_bundle")
PARQUET_URI = f"parquet://{BUNDLE.resolve()}/tables"
PG_URI = os.environ.get(
    "SPIKE_PG_URI", "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
)

# ---------------------------------------------------------------------------
# Candidate fix under test (applied to the DuckDB engine only).
# psycopg2 returns JSON pre-parsed and the PG dialect disables the
# deserializer; duckdb_engine does not, so SQLAlchemy calls json.loads()
# on an already-decoded dict.
# ---------------------------------------------------------------------------
if os.environ.get("SPIKE_JSON_FIX") == "1":
    import biofilter.modules.db.database as dbmod
    from sqlalchemy.engine.url import make_url

    _orig_kwargs = dbmod.Database._engine_kwargs

    def _patched(self, url):
        kw = _orig_kwargs(self, url)
        if make_url(str(url)).drivername.startswith("duckdb"):
            kw["json_deserializer"] = lambda v: (
                v if isinstance(v, (dict, list)) else json.loads(v)
            )
        return kw

    dbmod.Database._engine_kwargs = _patched

from biofilter import Biofilter  # noqa: E402

# ---------------------------------------------------------------------------
# Input overrides: the shipped example_input() targets identifiers that don't
# exist in our chr1:1-6Mb window. Substituting real in-window identifiers is
# what makes the variant reports actually exercise data on both backends.
# ---------------------------------------------------------------------------
IN_WINDOW_VARIANTS = [
    "rs754902779",
    "rs146254088",
    "rs1276372832",
    "chr1:1000006",
    "chr1:1000018",
    "chr1:1000025",
]
IN_WINDOW_GENES = ["SKI", "CCDC27", "SMIM1", "A1BG", "A2M"]

INPUT_OVERRIDES: dict[str, dict] = {
    # annotation_master_* ship input_data="__ALL__", which sweeps the entire
    # domain (and, for genes, the full effects table). Small real inputs keep
    # the sweep bounded while still exercising the same code paths.
    "report_annotation_master_gene": {"input_data": IN_WINDOW_GENES},
    "report_annotation_master_disease": {
        "input_data": ["adrenocortical insufficiency"]
    },
    "report_annotation_master_go": {
        "input_data": ["GO:0000001", "GO:0000006", "GO:0000007"]
    },
    "report_annotation_master_pathway": {
        "input_data": ["R-HSA-164843", "R-HSA-9909438"]
    },
    "report_annotation_master_protein": {
        "input_data": ["A0A087X1C5", "A0A096LP01"]
    },
    "report_annotation_master_chemical": {"input_data": ["CHEBI:15377"]},
    "report_entity_neighborhood_summary": {
        "items": ["gene:A1BG", "gene:A2M", "A1BG"],
        "match_mode": "exact",
    },
    "report_snp_snp_model": {
        "input_data": IN_WINDOW_VARIANTS[3:5],
        "build": 38,
    },
    "report_variant_modeling": {"input_data": IN_WINDOW_VARIANTS, "build": 38},
    "report_variant_gene_location_model": {
        "input_data": IN_WINDOW_GENES,
        "build": 38,
    },
    "report_annotation_master_variant": {"input_data": IN_WINDOW_VARIANTS},
    "report_gene_to_variant_filtering": {"input_data": IN_WINDOW_GENES},
    "report_variant_single_gene_annotation": {"input_data": ["SKI"]},
    "report_entity_relationship_model": {
        "input_data": ["TP53", "BRCA1", "A2M"],
        "input_entity_groups": ["Gene"],
        "output_entity_groups": ["Pathway", "Protein"],
        "relationship_scope": "input_to_any",
    },
}

# Reports that cannot be exercised by the spike (need external files the
# repo doesn't ship). Recorded as SKIPPED rather than silently failing.
NEEDS_EXTERNAL_FILE = {
    "report_snp_snp_pair_generator",
    "report_variant_list_intersect",
    "report_variant_annotation_expanded",
    "report_variant_binning",
    "report_template",
}


def classify(result) -> tuple[str, int | None]:
    if result is None:
        return "EMPTY", 0
    if isinstance(result, pd.DataFrame):
        return ("OK" if len(result) else "EMPTY"), len(result)
    if isinstance(result, (list, tuple, dict)):
        return ("OK" if len(result) else "EMPTY"), len(result)
    return "OK", None


def build_kwargs(bf, module_name: str) -> dict:
    if module_name in INPUT_OVERRIDES:
        return dict(INPUT_OVERRIDES[module_name])
    cls = bf.report.get_report_class(module_name)
    try:
        example = cls.example_input()
    except Exception:
        example = None
    if example is None:
        return {}
    # example_input() returns a full kwargs dict for most reports.
    if isinstance(example, dict):
        return dict(example)
    return {"input_data": example}


def run_one(bf, module_name: str) -> dict:
    t0 = time.perf_counter()
    if module_name in NEEDS_EXTERNAL_FILE:
        return {
            "status": "SKIPPED",
            "rows": None,
            "seconds": 0.0,
            "error": "needs external input file",
        }
    try:
        result = bf.report.run(module_name, **build_kwargs(bf, module_name))
        status, rows = classify(result)
        return {
            "status": status,
            "rows": rows,
            "seconds": round(time.perf_counter() - t0, 2),
            "error": None,
        }
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        status = (
            "PARAM" if "missing required parameter" in msg.lower() else "ERROR"
        )
        return {
            "status": status,
            "rows": None,
            "seconds": round(time.perf_counter() - t0, 2),
            "error": msg[:400],
            "trace": traceback.format_exc()[-1200:],
        }


def main() -> None:
    fix = os.environ.get("SPIKE_JSON_FIX") == "1"
    print(f"PG      : {PG_URI}")
    print(f"PARQUET : {PARQUET_URI}")
    print(f"JSON FIX: {'APPLIED' if fix else 'not applied (baseline)'}\n")

    bf_pg = Biofilter(db_uri=PG_URI)
    bf_pg.db.connect()
    modules = sorted(r["module"] for r in bf_pg.report.list(verbose=False))

    bf_pq = Biofilter(db_uri=PARQUET_URI)
    bf_pq.db.connect()

    rows = []
    for i, mod in enumerate(modules, 1):
        pg = run_one(bf_pg, mod)
        pq = run_one(bf_pq, mod)
        if pg["status"] == "SKIPPED":
            verdict = "SKIPPED"
        elif pg["status"] in ("OK", "EMPTY") and pq["status"] == "ERROR":
            verdict = "BLOCKED"
        elif pg["status"] == pq["status"] and pg["rows"] == pq["rows"]:
            verdict = "PARITY"
        elif pg["status"] == pq["status"]:
            verdict = "PARITY_ROWDIFF"
        else:
            verdict = "DIVERGENT"
        rows.append(
            {
                "report": mod,
                "pg": pg["status"],
                "pg_rows": pg["rows"],
                "pg_s": pg["seconds"],
                "pq": pq["status"],
                "pq_rows": pq["rows"],
                "pq_s": pq["seconds"],
                "verdict": verdict,
                "pq_error": pq["error"],
                "pg_error": pg["error"],
                "pq_trace": pq.get("trace"),
            }
        )
        print(
            f"[{i:2d}/{len(modules)}] {mod:46s} "
            f"pg={pg['status']}({pg['rows']}) pq={pq['status']}({pq['rows']}) -> {verdict}",
            flush=True,
        )

    df = pd.DataFrame(rows)
    suffix = "postfix" if fix else "baseline"
    out = BUNDLE.parent / f"spike_matrix_{suffix}.json"
    out.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")

    print("\n" + "=" * 92)
    print(f"COMPATIBILITY MATRIX ({suffix})")
    print("=" * 92)
    print(
        df[["report", "pg", "pg_rows", "pq", "pq_rows", "verdict"]].to_string(
            index=False
        )
    )
    print("\nVerdict counts:")
    print(df["verdict"].value_counts().to_string())
    print(f"\nFull results: {out}")


if __name__ == "__main__":
    main()
