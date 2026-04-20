from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

import pandas as pd
from sqlalchemy import MetaData, and_, func, inspect as sa_inspect, select

from biofilter.modules.db.models import (
    VariantConsequence,
    VariantConsequenceCategory,
    VariantConsequenceGroup,
)
from biofilter.modules.db.models.model_variants import (
    map_variant_effect_predictions,
    map_variant_masters,
    map_variant_molecular_effects,
)
from biofilter.modules.report.reports.base_report import ReportBase

_RSID_RE = re.compile(r"^rs\d+$", re.IGNORECASE)
_CHR_POS_RE = re.compile(r"^(?:chr)?([0-9xyXYmMtT]+)\s*[:;,\s]\s*(\d+)$")


def _norm(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _parse_int(v: Any) -> int | None:
    try:
        return int(str(v).strip()) if v is not None else None
    except Exception:
        return None


def _parse_chr(v: Any) -> int | None:
    s = (
        _norm(v)
        .lower()
        .replace("chromosome", "")
        .replace("chrom", "")
        .replace("chr", "")
        .strip()
    )
    if s == "x":
        return 23
    if s == "y":
        return 24
    if s in {"m", "mt", "mito", "mitochondria"}:
        return 25
    try:
        vi = int(s)
        return vi if 1 <= vi <= 25 else None
    except Exception:
        return None


def _fmt_chr(c: int | None) -> str | None:
    if c is None:
        return None
    return {23: "chrX", 24: "chrY", 25: "chrMT"}.get(c, f"chr{c}")


def _parse_input_item(item: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "raw": str(item),
        "kind": "invalid",
        "rsid": None,
        "chromosome": None,
        "position": None,
        "note": None,
    }
    if isinstance(item, dict):
        c = _parse_chr(item.get("chromosome") or item.get("chr"))
        p = _parse_int(item.get("position") or item.get("pos"))
        if c and p and p > 0:
            out.update(kind="chr_pos", chromosome=c, position=p, raw=f"{_fmt_chr(c)}:{p}")
        else:
            out["note"] = "Invalid dict — expected chromosome and position keys."
        return out
    s = _norm(item)
    out["raw"] = s
    if not s:
        out["note"] = "Empty input."
        return out
    if _RSID_RE.match(s):
        out.update(kind="rsid", rsid=s.lower())
        return out
    m = _CHR_POS_RE.match(s)
    if m:
        c = _parse_chr(m.group(1))
        p = _parse_int(m.group(2))
        if c and p and p > 0:
            out.update(kind="chr_pos", chromosome=c, position=p)
            return out
    out["note"] = "Expected rsID (rs12345) or chr:pos (chr1:100000)."
    return out


_SAFE_VME_COLS = [
    "variant_id", "chromosome", "gene_id", "gene_symbol", "transcript_id",
    "feature_type", "consequence_raw", "consequence_id", "impact_id", "biotype_id",
    "impact_rank", "consequence_rank", "is_most_severe_for_annotation",
    "is_most_severe_for_variant", "canonical", "mane_select", "mane_plus_clinical",
    "hgvsc", "hgvsp", "cdna_position", "cds_position", "protein_position",
    "amino_acids", "codons", "lof_confidence", "lof_flag", "lof_filter", "variant_class",
]


class AnnotationMasterVariantReport(ReportBase):
    name = "annotation_master_variant"
    description = (
        "Full annotation expansion for an input list of variants (rsID or chr:pos). "
        "Returns one row per variant×transcript annotation, joining variant_masters, "
        "variant_molecular_effects (VEP), and variant_effect_predictions (AlphaMissense). "
        "Includes population frequencies, pathogenicity scores, consequence hierarchy, "
        "and LoF classification."
    )

    columns = [
        # Input tracking
        "input_value",
        "status",
        "note",
        # Variant identity
        "variant_id",
        "rsid",
        "chromosome",
        "position_start",
        "position_end",
        "reference_allele",
        "alternate_allele",
        "variant_type",
        "allele_type",
        # Population frequencies
        "ac",
        "an",
        "af",
        "grpmax",
        "grpmax_af",
        # Pathogenicity scores (variant_masters)
        "cadd_phred",
        "cadd_raw_score",
        "revel_max",
        "spliceai_ds_max",
        "pangolin_largest_ds",
        "sift_max",
        "polyphen_max",
        # Molecular effect (variant_molecular_effects — one row per transcript)
        "gene_symbol",
        "gene_id",
        "transcript_id",
        "feature_type",
        "consequence_raw",
        "consequence_name",
        "consequence_group",
        "consequence_category",
        "consequence_rank",
        "impact_name",
        "impact_rank",
        "biotype_name",
        "is_most_severe_for_variant",
        "is_most_severe_for_annotation",
        "canonical",
        "mane_select",
        "mane_plus_clinical",
        "hgvsc",
        "hgvsp",
        "cdna_position",
        "cds_position",
        "protein_position",
        "amino_acids",
        "codons",
        "variant_class",
        "lof_confidence",
        "lof_filter",
        # AlphaMissense
        "alphamissense_score",
        "alphamissense_classification",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def example_input(cls) -> dict:
        return {
            "input_data": ["rs429358", "rs7412", "chr19:44908684"],
            "most_severe_only": False,
            "canonical_only": False,
        }

    # ------------------------------------------------------------------
    # Dimension loaders (into memory — small lookup tables)
    # ------------------------------------------------------------------

    def _load_consequence_map(self) -> dict[int, dict[str, Any]]:
        rows = (
            self.session.query(
                VariantConsequence.id,
                VariantConsequence.name.label("name"),
                VariantConsequence.severity_rank.label("rank"),
                VariantConsequenceGroup.name.label("group"),
                VariantConsequenceCategory.name.label("category"),
            )
            .join(
                VariantConsequenceGroup,
                VariantConsequenceGroup.id == VariantConsequence.consequence_group_id,
                isouter=True,
            )
            .join(
                VariantConsequenceCategory,
                VariantConsequenceCategory.id == VariantConsequence.consequence_category_id,
                isouter=True,
            )
            .all()
        )
        return {
            row.id: {
                "consequence_name": row.name,
                "consequence_rank": row.rank,
                "consequence_group": row.group,
                "consequence_category": row.category,
            }
            for row in rows
        }

    def _load_impact_map(self) -> dict[int, str]:
        from biofilter.modules.db.models import VariantImpact
        rows = self.session.query(VariantImpact.id, VariantImpact.name).all()
        return {int(r.id): _norm(r.name) for r in rows}

    def _load_biotype_map(self) -> dict[int, str]:
        from biofilter.modules.db.models import VariantBiotype
        rows = self.session.query(VariantBiotype.id, VariantBiotype.name).all()
        return {int(r.id): _norm(r.name) for r in rows}

    # ------------------------------------------------------------------
    # Variant lookup
    # ------------------------------------------------------------------

    def _lookup_variants(
        self,
        parsed_inputs: list[dict[str, Any]],
        vm,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Returns {input_raw: [variant_master_row_dicts, ...]}.
        Groups by chromosome for efficiency.
        """
        conn = self.session.connection()
        result: dict[str, list[dict[str, Any]]] = {}

        rsid_inputs = [p for p in parsed_inputs if p["kind"] == "rsid"]
        pos_inputs = [p for p in parsed_inputs if p["kind"] == "chr_pos"]

        # rsID lookup — single query for all rsIDs
        if rsid_inputs:
            rsids = [p["rsid"] for p in rsid_inputs]
            stmt = vm.select().where(func.lower(vm.c.rsid).in_(rsids))
            rows = conn.execute(stmt).mappings().fetchall()
            rsid_to_rows: dict[str, list[dict]] = defaultdict(list)
            seen: set[int] = set()
            for row in rows:
                vid = int(row["variant_id"])
                rsid_key = _norm(row["rsid"]).lower()
                if vid not in seen:
                    seen.add(vid)
                    rsid_to_rows[rsid_key].append(dict(row))
            for p in rsid_inputs:
                result[p["raw"]] = rsid_to_rows.get(p["rsid"], [])

        # chr:pos lookup — one query per input
        for p in pos_inputs:
            chrom, pos = p["chromosome"], p["position"]
            stmt = vm.select().where(
                and_(
                    vm.c.chromosome == chrom,
                    vm.c.position_start <= pos,
                    vm.c.position_end >= pos,
                )
            )
            if "allele_type" in vm.c:
                stmt = stmt.where(func.lower(vm.c.allele_type) == "snv")
            rows = conn.execute(stmt).mappings().fetchall()
            seen: set[int] = set()
            deduped = []
            for row in rows:
                vid = int(row["variant_id"])
                if vid not in seen:
                    seen.add(vid)
                    deduped.append(dict(row))
            result[p["raw"]] = deduped

        return result

    # ------------------------------------------------------------------
    # Molecular effects & predictions — batch by chromosome
    # ------------------------------------------------------------------

    def _db_columns(self, table_name: str) -> set[str]:
        """Return the column names that actually exist in the DB table."""
        try:
            return {col["name"] for col in sa_inspect(self.db.engine).get_columns(table_name)}
        except Exception:
            return set()

    def _fetch_mol_effects(
        self,
        vme,
        chromosome: int,
        variant_ids: list[int],
        batch_size: int = 500,
    ) -> list[dict[str, Any]]:
        conn = self.session.connection()
        db_cols = self._db_columns("variant_molecular_effects")
        safe_cols = [vme.c[col] for col in _SAFE_VME_COLS if col in db_cols]

        rows: list[dict[str, Any]] = []
        for i in range(0, len(variant_ids), batch_size):
            batch = variant_ids[i : i + batch_size]
            stmt = select(*safe_cols).where(
                and_(vme.c.chromosome == chromosome, vme.c.variant_id.in_(batch))
            )
            for row in conn.execute(stmt).mappings().fetchall():
                rows.append(dict(row))
        return rows

    def _fetch_predictions(
        self,
        vep,
        chromosome: int,
        variant_ids: list[int],
        batch_size: int = 500,
    ) -> dict[tuple[int, str | None], dict[str, Any]]:
        """Returns {(variant_id, norm_transcript): {score, classification}}."""
        conn = self.session.connection()
        out: dict[tuple[int, str | None], dict[str, Any]] = {}
        for i in range(0, len(variant_ids), batch_size):
            batch = variant_ids[i : i + batch_size]
            stmt = vep.select().where(
                and_(
                    vep.c.chromosome == chromosome,
                    vep.c.variant_id.in_(batch),
                    vep.c.predictor_name.ilike("alphamissense"),
                )
            )
            for row in conn.execute(stmt).mappings().fetchall():
                vid = int(row["variant_id"])
                tx = _norm(row.get("transcript_id")).split(".")[0] or None
                key = (vid, tx)
                if key not in out:
                    out[key] = {
                        "alphamissense_score": row.get("score"),
                        "alphamissense_classification": row.get("classification"),
                    }
        return out

    # ------------------------------------------------------------------
    # Row builder
    # ------------------------------------------------------------------

    def _build_rows(
        self,
        input_value: str,
        vm_row: dict[str, Any],
        mol_effects: list[dict[str, Any]],
        am_map: dict[tuple[int, str | None], dict[str, Any]],
        consequence_map: dict[int, dict[str, Any]],
        impact_map: dict[int, str],
        biotype_map: dict[int, str],
        most_severe_only: bool,
        canonical_only: bool,
    ) -> list[dict[str, Any]]:
        variant_id = int(vm_row["variant_id"])
        chromosome = int(vm_row["chromosome"])

        vm_base: dict[str, Any] = {
            "input_value": input_value,
            "status": "found",
            "note": None,
            "variant_id": variant_id,
            "rsid": vm_row.get("rsid"),
            "chromosome": chromosome,
            "position_start": vm_row.get("position_start"),
            "position_end": vm_row.get("position_end"),
            "reference_allele": vm_row.get("reference_allele"),
            "alternate_allele": vm_row.get("alternate_allele"),
            "variant_type": vm_row.get("variant_type"),
            "allele_type": vm_row.get("allele_type"),
            "ac": vm_row.get("ac"),
            "an": vm_row.get("an"),
            "af": vm_row.get("af"),
            "grpmax": vm_row.get("grpmax"),
            "grpmax_af": vm_row.get("grpmax_af"),
            "cadd_phred": vm_row.get("cadd_phred"),
            "cadd_raw_score": vm_row.get("cadd_raw_score"),
            "revel_max": vm_row.get("revel_max"),
            "spliceai_ds_max": vm_row.get("spliceai_ds_max"),
            "pangolin_largest_ds": vm_row.get("pangolin_largest_ds"),
            "sift_max": vm_row.get("sift_max"),
            "polyphen_max": vm_row.get("polyphen_max"),
        }

        if not mol_effects:
            return [{**vm_base, **{c: None for c in self.columns if c not in vm_base}}]

        # Optional filters
        effects = mol_effects
        if most_severe_only:
            effects = [e for e in effects if e.get("is_most_severe_for_variant")]
            if not effects:
                effects = mol_effects
        if canonical_only:
            canon = [e for e in effects if e.get("canonical")]
            effects = canon if canon else effects

        rows: list[dict[str, Any]] = []
        for eff in effects:
            tx_raw = _norm(eff.get("transcript_id"))
            tx_norm = tx_raw.split(".")[0] or None

            csq_id = _parse_int(eff.get("consequence_id"))
            csq_meta = consequence_map.get(csq_id, {}) if csq_id is not None else {}

            impact_id = _parse_int(eff.get("impact_id"))
            biotype_id = _parse_int(eff.get("biotype_id"))

            am = am_map.get((variant_id, tx_norm)) or am_map.get((variant_id, None), {})

            row = {
                **vm_base,
                "gene_symbol": eff.get("gene_symbol"),
                "gene_id": eff.get("gene_id"),
                "transcript_id": tx_raw or None,
                "feature_type": eff.get("feature_type"),
                "consequence_raw": eff.get("consequence_raw"),
                "consequence_name": csq_meta.get("consequence_name"),
                "consequence_group": csq_meta.get("consequence_group"),
                "consequence_category": csq_meta.get("consequence_category"),
                "consequence_rank": csq_meta.get("consequence_rank") or eff.get("consequence_rank"),
                "impact_name": impact_map.get(impact_id) if impact_id is not None else None,
                "impact_rank": eff.get("impact_rank"),
                "biotype_name": biotype_map.get(biotype_id) if biotype_id is not None else None,
                "is_most_severe_for_variant": eff.get("is_most_severe_for_variant"),
                "is_most_severe_for_annotation": eff.get("is_most_severe_for_annotation"),
                "canonical": eff.get("canonical"),
                "mane_select": eff.get("mane_select"),
                "mane_plus_clinical": eff.get("mane_plus_clinical"),
                "hgvsc": eff.get("hgvsc"),
                "hgvsp": eff.get("hgvsp"),
                "cdna_position": eff.get("cdna_position"),
                "cds_position": eff.get("cds_position"),
                "protein_position": eff.get("protein_position"),
                "amino_acids": eff.get("amino_acids"),
                "codons": eff.get("codons"),
                "variant_class": eff.get("variant_class"),
                "lof_confidence": eff.get("lof_confidence"),
                "lof_filter": eff.get("lof_filter"),
                "alphamissense_score": am.get("alphamissense_score"),
                "alphamissense_classification": am.get("alphamissense_classification"),
            }
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        input_raw = self.param("input_data", required=True)
        input_list = self.resolve_input_list(input_raw, param_name="input_data")
        most_severe_only: bool = bool(self.param("most_severe_only", False))
        canonical_only: bool = bool(self.param("canonical_only", False))

        parsed = [_parse_input_item(item) for item in input_list]
        valid = [p for p in parsed if p["kind"] != "invalid"]
        invalid = [p for p in parsed if p["kind"] == "invalid"]

        meta = MetaData()
        vm = map_variant_masters(self.db.engine, meta)
        vme = map_variant_molecular_effects(self.db.engine, meta)
        vep = map_variant_effect_predictions(self.db.engine, meta)

        consequence_map = self._load_consequence_map()
        impact_map = self._load_impact_map()
        biotype_map = self._load_biotype_map()

        # Lookup all valid inputs
        input_to_vm_rows = self._lookup_variants(valid, vm)

        # Group found variants by chromosome for batch mol_effects/predictions fetch
        chrom_to_variant_ids: dict[int, list[int]] = defaultdict(list)
        variant_id_to_vm: dict[int, dict[str, Any]] = {}
        for vm_rows in input_to_vm_rows.values():
            for vrow in vm_rows:
                vid = int(vrow["variant_id"])
                chrom = int(vrow["chromosome"])
                if vid not in variant_id_to_vm:
                    variant_id_to_vm[vid] = vrow
                    chrom_to_variant_ids[chrom].append(vid)

        # Fetch molecular effects + predictions per chromosome
        mol_by_vid: dict[int, list[dict[str, Any]]] = defaultdict(list)
        am_map: dict[tuple[int, str | None], dict[str, Any]] = {}

        for chrom, vids in chrom_to_variant_ids.items():
            for eff in self._fetch_mol_effects(vme, chrom, vids):
                mol_by_vid[int(eff["variant_id"])].append(eff)
            am_map.update(self._fetch_predictions(vep, chrom, vids))

        # Build output rows
        all_rows: list[dict[str, Any]] = []

        for p in invalid:
            all_rows.append(
                {c: None for c in self.columns}
                | {"input_value": p["raw"], "status": "invalid_input", "note": p["note"]}
            )

        for p in valid:
            vm_rows = input_to_vm_rows.get(p["raw"], [])
            if not vm_rows:
                all_rows.append(
                    {c: None for c in self.columns}
                    | {"input_value": p["raw"], "status": "not_found", "note": "No variant found in DB."}
                )
                continue
            for vrow in vm_rows:
                vid = int(vrow["variant_id"])
                rows = self._build_rows(
                    input_value=p["raw"],
                    vm_row=vrow,
                    mol_effects=mol_by_vid.get(vid, []),
                    am_map=am_map,
                    consequence_map=consequence_map,
                    impact_map=impact_map,
                    biotype_map=biotype_map,
                    most_severe_only=most_severe_only,
                    canonical_only=canonical_only,
                )
                all_rows.extend(rows)

        df = pd.DataFrame(all_rows).reindex(columns=self.columns)

        # Sort: chromosome → position → most_severe first → consequence_rank
        sort_cols = [c for c in ["chromosome", "position_start", "is_most_severe_for_variant", "consequence_rank"] if c in df.columns]
        ascending = [True, True, False, True]
        if sort_cols:
            df = df.sort_values(
                by=sort_cols,
                ascending=ascending[: len(sort_cols)],
                na_position="last",
            ).reset_index(drop=True)

        self.results = df
        return df
