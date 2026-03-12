from __future__ import annotations

import re
from dataclasses import dataclass
# from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy import MetaData, Table, and_, select

from biofilter.modules.report.reports.base_report import ReportBase


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _norm_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _parse_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(str(x).strip())
    except Exception:
        return None


def _parse_chr_to_int(chr_value: Any) -> Optional[int]:
    """
    Normalize chromosome input to Biofilter integer encoding:
      1..22 autosomes, 23=X, 24=Y, 25=MT

    Accepts: "1", "chr1", "X", "chrX", "MT", "M", etc.
    """
    s = _norm_str(chr_value).lower()
    if not s:
        return None

    s = s.replace("chromosome", "").replace("chrom", "").replace("chr", "").strip()  # noqa E501

    if s == "x":
        return 23
    if s == "y":
        return 24
    if s in ("mt", "m", "mito", "mitochondria"):
        return 25

    try:
        v = int(s)
        if 1 <= v <= 25:
            return v
        return None
    except Exception:
        return None


def _format_chr(chromosome: Optional[int]) -> Optional[str]:
    if chromosome is None:
        return None
    if chromosome == 23:
        return "chrX"
    if chromosome == 24:
        return "chrY"
    if chromosome == 25:
        return "chrMT"
    return f"chr{chromosome}"


def _overlap_bp(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    """
    Inclusive overlap in bp between intervals
    [a_start, a_end] and [b_start, b_end].
    """
    return max(0, min(a_end, b_end) - max(a_start, b_start) + 1)


def _distance_bp(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    """
    Distance in bp between intervals [a_start, a_end] and [b_start, b_end].
    Returns 0 if overlapping.
    """
    if _overlap_bp(a_start, a_end, b_start, b_end) > 0:
        return 0
    if a_end < b_start:
        return b_start - a_end
    if a_start > b_end:
        return a_start - b_end
    return 0


@dataclass
class NormalizedRegionInput:
    raw: str
    chromosome: Optional[int]
    start: Optional[int]
    end: Optional[int]
    status: str
    note: Optional[str] = None


# -----------------------------------------------------------------------------
# Report
# -----------------------------------------------------------------------------
class VariantConsequencesReport(ReportBase):
    """
    Given genomic regions (chr:start:end), return matching variants from
    variant_masters and all molecular consequences from variant_molecular_effects,  # noqa E501
    enriched with consequence / impact / biotype dimension labels.
    """

    name = "variant_consequences"
    description = (
        "Given genomic regions, returns matching variants from variant_masters and "  # noqa E501
        "their molecular consequences from variant_molecular_effects, including "  # noqa E501
        "resolved consequence, group, category, impact, and biotype labels."
    )

    columns = [
        # Input / query
        "Input",
        "Input Chromosome (encoded)",
        "Input Chromosome",
        "Input Start",
        "Input End",
        "Range Up",
        "Range Down",
        "Query Start",
        "Query End",
        # Status
        "Status",
        "Note",
        "Variants Found",
        "Consequences Found",
        # Variant master
        "Variant ID",
        "Variant Key",
        "Chromosome (encoded)",
        "Chromosome",
        "Position Start",
        "Position End",
        "Overlap (bp)",
        "Distance to Query (bp)",
        "Reference Allele",
        "Alternate Allele",
        "rsID",
        "Variant Type",
        "Allele Type",
        "AC",
        "AN",
        "AF",
        "grpmax",
        "grpmax_af",
        "cadd_raw_score",
        "cadd_phred",
        "revel_max",
        "spliceai_ds_max",
        "pangolin_largest_ds",
        "sift_max",
        "polyphen_max",
        # Molecular effect raw
        "Gene ID",
        "Gene Symbol",
        "Transcript ID",
        "Feature Type",
        "Consequence Raw",
        # Resolved dimensions
        "Consequence ID",
        "Consequence",
        "Consequence Severity Rank",
        "Consequence Group ID",
        "Consequence Group",
        "Consequence Category ID",
        "Consequence Category",
        "Impact ID",
        "Impact",
        "Biotype ID",
        "Biotype",
        # LoF / annotation extras
        "Canonical",
        "MANE Select",
        "MANE Plus Clinical",
        "LoF",
        "LoF Flags",
        "LoF Filter",
        "LoF Confidence",
        "LoF Info",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def explain(cls) -> str:
        return """\
🧬 VARIANT CONSEQUENCES Report (v1)
==================================

Purpose:
- Given one or more genomic regions, return all overlapping variants from
  variant_masters and their molecular consequences from variant_molecular_effects.  # noqa E501

Input:
- items: list[str] or list[dict]
  Supported region format:
    - "chr1:12345:12345"
    - "1:12345:12400"
    - "chrX:2781644:2781644"
  Dict format:
    - {"chromosome": "1", "start": 12345, "end": 12400}
    - {"chr": "X", "pos_start": 2781644, "pos_end": 2781644}

- input_path: path to a text file with one region per line

Key params:
- range_up (default 0): extend query interval upstream
- range_down (default 0): extend query interval downstream
- emit_not_found_rows (default True): keep one row per input even if no variants found  # noqa E501
- include_variant_only_rows (default True): keep variants even if they have no consequences  # noqa E501
- limit_variants_per_input (default 1000): safety bound applied in memory after overlap matching  # noqa E501

Behavior:
- First queries variant_masters by chromosome and interval overlap
- Then fetches variant_molecular_effects using (chromosome, variant_id)
- Resolves labels from:
    * variant_consequences
    * variant_consequence_groups
    * variant_consequence_categories
    * variant_impacts
    * variant_biotypes

Output:
- One row per input × variant × consequence
- If no variant is found, emits one row with Status=not_found (if enabled)
- If a variant is found but has no consequences, emits one row with Status=variant_only  # noqa E501
  (if include_variant_only_rows=True)
"""

    @classmethod
    def example_input(cls) -> list[str]:
        return [
            "chrY:2781644:2781644",
            "chr1:55516888:55516888",
            "chr7:55019017:55019017",
        ]

    # -------------------------------------------------------------------------
    # Reflection helpers
    # -------------------------------------------------------------------------
    def _table(self, table_name: str) -> Table:
        metadata = MetaData()
        return Table(table_name, metadata, autoload_with=self.db.engine)

    def _get_tables(self) -> dict[str, Table]:
        return {
            "variant_masters": self._table("variant_masters"),
            "variant_molecular_effects": self._table("variant_molecular_effects"),  # noqa E501
            "variant_consequences": self._table("variant_consequences"),
            "variant_consequence_groups": self._table("variant_consequence_groups"),  # noqa E501
            "variant_consequence_categories": self._table(
                "variant_consequence_categories"
            ),
            "variant_impacts": self._table("variant_impacts"),
            "variant_biotypes": self._table("variant_biotypes"),
        }

    # -------------------------------------------------------------------------
    # Input parsing
    # -------------------------------------------------------------------------
    def _parse_region_string(self, value: str) -> NormalizedRegionInput:
        raw = _norm_str(value)
        if not raw:
            return NormalizedRegionInput(
                raw=value,
                chromosome=None,
                start=None,
                end=None,
                status="invalid_input",
                note="Empty input.",
            )

        parts = re.split(r"[:;,\|\t ]+", raw)
        if len(parts) != 3:
            return NormalizedRegionInput(
                raw=raw,
                chromosome=None,
                start=None,
                end=None,
                status="invalid_input",
                note="Expected format chr:start:end.",
            )

        chr_in = _parse_chr_to_int(parts[0])
        start = _parse_int(parts[1])
        end = _parse_int(parts[2])

        if chr_in is None:
            return NormalizedRegionInput(
                raw=raw,
                chromosome=None,
                start=None,
                end=None,
                status="invalid_input",
                note="Could not parse chromosome.",
            )

        if start is None or end is None:
            return NormalizedRegionInput(
                raw=raw,
                chromosome=chr_in,
                start=None,
                end=None,
                status="invalid_input",
                note="Could not parse start/end positions.",
            )

        if start <= 0 or end <= 0:
            return NormalizedRegionInput(
                raw=raw,
                chromosome=chr_in,
                start=start,
                end=end,
                status="invalid_input",
                note="Positions must be positive integers.",
            )

        if end < start:
            start, end = end, start

        return NormalizedRegionInput(
            raw=raw,
            chromosome=chr_in,
            start=start,
            end=end,
            status="ok",
            note=None,
        )

    def _parse_region_dict(self, item: dict[str, Any]) -> NormalizedRegionInput:  # noqa E501
        raw = str(item)

        chrom = item.get("chromosome") or item.get("chr") or item.get("chrom")
        start = item.get("start") or item.get("pos_start") or item.get("position_start")  # noqa E501
        end = item.get("end") or item.get("pos_end") or item.get("position_end")  # noqa E501

        chr_in = _parse_chr_to_int(chrom)
        start_i = _parse_int(start)
        end_i = _parse_int(end)

        if chr_in is None:
            return NormalizedRegionInput(
                raw=raw,
                chromosome=None,
                start=None,
                end=None,
                status="invalid_input",
                note="Could not parse chromosome from dict.",
            )

        if start_i is None or end_i is None:
            return NormalizedRegionInput(
                raw=raw,
                chromosome=chr_in,
                start=None,
                end=None,
                status="invalid_input",
                note="Could not parse start/end from dict.",
            )

        if end_i < start_i:
            start_i, end_i = end_i, start_i

        return NormalizedRegionInput(
            raw=raw,
            chromosome=chr_in,
            start=start_i,
            end=end_i,
            status="ok",
            note=None,
        )

    def _normalize_inputs(self, items: list[Any]) -> list[NormalizedRegionInput]:  # noqa E501
        norm: list[NormalizedRegionInput] = []

        for item in items:
            try:
                if isinstance(item, str):
                    norm.append(self._parse_region_string(item))
                elif isinstance(item, dict):
                    norm.append(self._parse_region_dict(item))
                else:
                    norm.append(
                        NormalizedRegionInput(
                            raw=str(item),
                            chromosome=None,
                            start=None,
                            end=None,
                            status="invalid_input",
                            note="Unsupported input type.",
                        )
                    )
            except Exception as e:
                norm.append(
                    NormalizedRegionInput(
                        raw=str(item),
                        chromosome=None,
                        start=None,
                        end=None,
                        status="invalid_input",
                        note=f"Failed to parse input: {e}",
                    )
                )

        return norm

    # -------------------------------------------------------------------------
    # Query helpers
    # -------------------------------------------------------------------------
    def _query_variants_for_chromosome(
        self,
        vm: Table,
        chromosome: int,
        min_start: int,
        max_end: int,
    ) -> list[dict[str, Any]]:
        stmt = select(vm).where(
            and_(
                vm.c.chromosome == chromosome,
                vm.c.position_start <= max_end,
                vm.c.position_end >= min_start,
            )
        )
        rows = self.session.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def _query_effects_for_chromosome(
        self,
        vme: Table,
        chromosome: int,
        variant_ids: list[int],
    ) -> list[dict[str, Any]]:
        if not variant_ids:
            return []

        stmt = select(vme).where(
            and_(
                vme.c.chromosome == chromosome,
                vme.c.variant_id.in_(variant_ids),
            )
        )
        rows = self.session.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def _load_dimension_maps(
        self,
        tables: dict[str, Table],
        consequence_ids: set[int],
        impact_ids: set[int],
        biotype_ids: set[int],
    ) -> tuple[
        dict[int, dict[str, Any]],
        dict[int, str],
        dict[int, str],
        dict[int, str],
        dict[int, str],
    ]:
        vc = tables["variant_consequences"]
        vcg = tables["variant_consequence_groups"]
        vcc = tables["variant_consequence_categories"]
        vi = tables["variant_impacts"]
        vb = tables["variant_biotypes"]

        consequence_map: dict[int, dict[str, Any]] = {}
        group_map: dict[int, str] = {}
        category_map: dict[int, str] = {}
        impact_map: dict[int, str] = {}
        biotype_map: dict[int, str] = {}

        if consequence_ids:
            stmt = select(vc).where(vc.c.id.in_(sorted(consequence_ids)))
            rows = self.session.execute(stmt).mappings().all()
            for row in rows:
                consequence_map[int(row["id"])] = dict(row)

            group_ids = {
                int(v["consequence_group_id"])
                for v in consequence_map.values()
                if v.get("consequence_group_id") is not None
            }
            category_ids = {
                int(v["consequence_category_id"])
                for v in consequence_map.values()
                if v.get("consequence_category_id") is not None
            }

            if group_ids:
                stmt = select(vcg).where(vcg.c.id.in_(sorted(group_ids)))
                rows = self.session.execute(stmt).mappings().all()
                for row in rows:
                    group_map[int(row["id"])] = row.get("name")

            if category_ids:
                stmt = select(vcc).where(vcc.c.id.in_(sorted(category_ids)))
                rows = self.session.execute(stmt).mappings().all()
                for row in rows:
                    category_map[int(row["id"])] = row.get("name")

        if impact_ids:
            stmt = select(vi).where(vi.c.id.in_(sorted(impact_ids)))
            rows = self.session.execute(stmt).mappings().all()
            for row in rows:
                impact_map[int(row["id"])] = row.get("name")

        if biotype_ids:
            stmt = select(vb).where(vb.c.id.in_(sorted(biotype_ids)))
            rows = self.session.execute(stmt).mappings().all()
            for row in rows:
                biotype_map[int(row["id"])] = row.get("name")

        return consequence_map, group_map, category_map, impact_map, biotype_map  # noqa E501

    # -------------------------------------------------------------------------
    # Output row builders
    # -------------------------------------------------------------------------
    def _base_row(
        self,
        raw_input: str,
        input_chr: Optional[int],
        input_start: Optional[int],
        input_end: Optional[int],
        range_up: int,
        range_down: int,
        query_start: Optional[int],
        query_end: Optional[int],
    ) -> dict[str, Any]:
        return {
            "Input": raw_input,
            "Input Chromosome (encoded)": input_chr,
            "Input Chromosome": _format_chr(input_chr),
            "Input Start": input_start,
            "Input End": input_end,
            "Range Up": range_up,
            "Range Down": range_down,
            "Query Start": query_start,
            "Query End": query_end,
            "Status": None,
            "Note": None,
            "Variants Found": 0,
            "Consequences Found": 0,
            "Variant ID": None,
            "Variant Key": None,
            "Chromosome (encoded)": None,
            "Chromosome": None,
            "Position Start": None,
            "Position End": None,
            "Overlap (bp)": None,
            "Distance to Query (bp)": None,
            "Reference Allele": None,
            "Alternate Allele": None,
            "rsID": None,
            "Variant Type": None,
            "Allele Type": None,
            "AC": None,
            "AN": None,
            "AF": None,
            "grpmax": None,
            "grpmax_af": None,
            "cadd_raw_score": None,
            "cadd_phred": None,
            "revel_max": None,
            "spliceai_ds_max": None,
            "pangolin_largest_ds": None,
            "sift_max": None,
            "polyphen_max": None,
            "Gene ID": None,
            "Gene Symbol": None,
            "Transcript ID": None,
            "Feature Type": None,
            "Consequence Raw": None,
            "Consequence ID": None,
            "Consequence": None,
            "Consequence Severity Rank": None,
            "Consequence Group ID": None,
            "Consequence Group": None,
            "Consequence Category ID": None,
            "Consequence Category": None,
            "Impact ID": None,
            "Impact": None,
            "Biotype ID": None,
            "Biotype": None,
            "Canonical": None,
            "MANE Select": None,
            "MANE Plus Clinical": None,
            "LoF": None,
            "LoF Flags": None,
            "LoF Filter": None,
            "LoF Confidence": None,
            "LoF Info": None,
        }

    def _fill_variant_fields(
        self, row: dict[str, Any], variant: dict[str, Any], overlap: int, distance: int  # noqa E501
    ) -> None:
        chromosome = variant.get("chromosome")
        row.update(
            {
                "Variant ID": variant.get("variant_id"),
                "Variant Key": variant.get("variant_key"),
                "Chromosome (encoded)": chromosome,
                "Chromosome": _format_chr(chromosome),
                "Position Start": variant.get("position_start"),
                "Position End": variant.get("position_end"),
                "Overlap (bp)": overlap,
                "Distance to Query (bp)": distance,
                "Reference Allele": variant.get("reference_allele"),
                "Alternate Allele": variant.get("alternate_allele"),
                "rsID": variant.get("rsid"),
                "Variant Type": variant.get("variant_type"),
                "Allele Type": variant.get("allele_type"),
                "AC": variant.get("ac"),
                "AN": variant.get("an"),
                "AF": variant.get("af"),
                "grpmax": variant.get("grpmax"),
                "grpmax_af": variant.get("grpmax_af"),
                "cadd_raw_score": variant.get("cadd_raw_score"),
                "cadd_phred": variant.get("cadd_phred"),
                "revel_max": variant.get("revel_max"),
                "spliceai_ds_max": variant.get("spliceai_ds_max"),
                "pangolin_largest_ds": variant.get("pangolin_largest_ds"),
                "sift_max": variant.get("sift_max"),
                "polyphen_max": variant.get("polyphen_max"),
            }
        )

    def _fill_effect_fields(
        self,
        row: dict[str, Any],
        effect: dict[str, Any],
        consequence_map: dict[int, dict[str, Any]],
        group_map: dict[int, str],
        category_map: dict[int, str],
        impact_map: dict[int, str],
        biotype_map: dict[int, str],
    ) -> None:
        consequence_id = effect.get("consequence_id")
        impact_id = effect.get("impact_id")
        biotype_id = effect.get("biotype_id")

        consequence = (
            consequence_map.get(int(consequence_id))
            if consequence_id is not None
            else None
        )

        group_id = consequence.get("consequence_group_id") if consequence else None  # noqa E501
        category_id = (
            consequence.get("consequence_category_id") if consequence else None
        )

        row.update(
            {
                "Gene ID": effect.get("gene_id"),
                "Gene Symbol": effect.get("gene_symbol"),
                "Transcript ID": effect.get("transcript_id"),
                "Feature Type": effect.get("feature_type"),
                "Consequence Raw": effect.get("consequence_raw"),
                "Consequence ID": consequence_id,
                "Consequence": consequence.get("name") if consequence else None,  # noqa E501
                "Consequence Severity Rank": (
                    consequence.get("severity_rank") if consequence else None
                ),
                "Consequence Group ID": group_id,
                "Consequence Group": (
                    group_map.get(int(group_id)) if group_id is not None else None  # noqa E501
                ),
                "Consequence Category ID": category_id,
                "Consequence Category": (
                    category_map.get(int(category_id))
                    if category_id is not None
                    else None
                ),
                "Impact ID": impact_id,
                "Impact": (
                    impact_map.get(int(impact_id)) if impact_id is not None else None  # noqa E501
                ),
                "Biotype ID": biotype_id,
                "Biotype": (
                    biotype_map.get(int(biotype_id)) if biotype_id is not None else None  # noqa E501
                ),
                "Canonical": effect.get("canonical"),
                "MANE Select": effect.get("mane_select"),
                "MANE Plus Clinical": effect.get("mane_plus_clinical"),
                "LoF": effect.get("lof"),
                "LoF Flags": effect.get("lof_flags"),
                "LoF Filter": effect.get("lof_filter"),
                "LoF Confidence": effect.get("lof_confidence"),
                "LoF Info": effect.get("lof_info"),
            }
        )

    # -------------------------------------------------------------------------
    # Main
    # -------------------------------------------------------------------------
    def run(self):
        input_data = self.param("items", None)
        if input_data is None:
            input_data = self.param("input_data", None)

        input_path = self.param("input_path", None)

        if input_data is None and input_path is None:
            raise ValueError("Provide either 'items'/'input_data' or 'input_path'.")  # noqa E501

        if input_data is None and input_path is not None:
            input_data = self.resolve_input_list(input_path, param_name="input_path")  # noqa E501

        range_up = max(0, int(self.param("range_up", 0) or 0))
        range_down = max(0, int(self.param("range_down", 0) or 0))
        emit_not_found_rows = bool(self.param("emit_not_found_rows", True))
        include_variant_only_rows = bool(self.param("include_variant_only_rows", True))  # noqa E501
        limit_variants_per_input = int(
            self.param("limit_variants_per_input", 1000) or 1000
        )

        if not isinstance(input_data, list):
            raise ValueError("'items'/'input_data' must resolve to a list of entries.")  # noqa E501

        tables = self._get_tables()
        vm = tables["variant_masters"]
        vme = tables["variant_molecular_effects"]

        norm_inputs = self._normalize_inputs(input_data)

        prepared_inputs: list[dict[str, Any]] = []
        rows_out: list[dict[str, Any]] = []

        for inp in norm_inputs:
            query_start = None
            query_end = None
            if inp.start is not None:
                query_start = max(1, int(inp.start) - range_up)
            if inp.end is not None:
                query_end = int(inp.end) + range_down

            base = self._base_row(
                raw_input=inp.raw,
                input_chr=inp.chromosome,
                input_start=inp.start,
                input_end=inp.end,
                range_up=range_up,
                range_down=range_down,
                query_start=query_start,
                query_end=query_end,
            )

            if inp.status != "ok":
                base["Status"] = inp.status
                base["Note"] = inp.note
                rows_out.append(base)
                continue

            prepared_inputs.append(
                {
                    "Input": inp.raw,
                    "Chromosome": inp.chromosome,
                    "Start": inp.start,
                    "End": inp.end,
                    "Query Start": query_start,
                    "Query End": query_end,
                    "Base": base,
                }
            )

        if not prepared_inputs:
            return pd.DataFrame(rows_out, columns=self.columns)

        # ---------------------------------------------------------------------
        # Query candidate variants grouped by chromosome
        # ---------------------------------------------------------------------
        candidate_variants_by_chr: dict[int, list[dict[str, Any]]] = {}
        inputs_by_chr: dict[int, list[dict[str, Any]]] = {}

        for item in prepared_inputs:
            inputs_by_chr.setdefault(item["Chromosome"], []).append(item)

        for chrom, items_chr in inputs_by_chr.items():
            min_start = min(int(x["Query Start"]) for x in items_chr)
            max_end = max(int(x["Query End"]) for x in items_chr)

            candidates = self._query_variants_for_chromosome(
                vm=vm,
                chromosome=int(chrom),
                min_start=min_start,
                max_end=max_end,
            )
            candidate_variants_by_chr[int(chrom)] = candidates

        # ---------------------------------------------------------------------
        # Match variants to each input interval in memory
        # ---------------------------------------------------------------------
        matched_variants_per_input: dict[
            tuple[str, int, int, int], list[dict[str, Any]]
        ] = {}
        variant_ids_by_chr: dict[int, set[int]] = {}

        for item in prepared_inputs:
            chrom = int(item["Chromosome"])
            qstart = int(item["Query Start"])
            qend = int(item["Query End"])
            key = (item["Input"], chrom, qstart, qend)

            matches: list[dict[str, Any]] = []

            for variant in candidate_variants_by_chr.get(chrom, []):
                vstart = int(variant["position_start"])
                vend = int(variant["position_end"])

                ov = _overlap_bp(qstart, qend, vstart, vend)
                if ov <= 0:
                    continue

                dist = _distance_bp(qstart, qend, vstart, vend)

                payload = dict(variant)
                payload["_overlap_bp"] = ov
                payload["_distance_bp"] = dist
                matches.append(payload)

            matches.sort(
                key=lambda x: (
                    x.get("_distance_bp", 0),
                    -(x.get("_overlap_bp", 0)),
                    x.get("position_start", 0),
                    x.get("variant_id", 0),
                )
            )

            if limit_variants_per_input > 0:
                matches = matches[:limit_variants_per_input]

            matched_variants_per_input[key] = matches

            for m in matches:
                vid = m.get("variant_id")
                if vid is not None:
                    variant_ids_by_chr.setdefault(chrom, set()).add(int(vid))

        # ---------------------------------------------------------------------
        # Query molecular effects in bulk per chromosome
        # ---------------------------------------------------------------------
        effects_by_chr_variant: dict[tuple[int, int], list[dict[str, Any]]] = {}  # noqa E501

        all_consequence_ids: set[int] = set()
        all_impact_ids: set[int] = set()
        all_biotype_ids: set[int] = set()

        for chrom, vids in variant_ids_by_chr.items():
            effect_rows = self._query_effects_for_chromosome(
                vme=vme,
                chromosome=chrom,
                variant_ids=sorted(vids),
            )

            for eff in effect_rows:
                key = (int(eff["chromosome"]), int(eff["variant_id"]))
                effects_by_chr_variant.setdefault(key, []).append(eff)

                if eff.get("consequence_id") is not None:
                    all_consequence_ids.add(int(eff["consequence_id"]))
                if eff.get("impact_id") is not None:
                    all_impact_ids.add(int(eff["impact_id"]))
                if eff.get("biotype_id") is not None:
                    all_biotype_ids.add(int(eff["biotype_id"]))

        consequence_map, group_map, category_map, impact_map, biotype_map = (
            self._load_dimension_maps(
                tables=tables,
                consequence_ids=all_consequence_ids,
                impact_ids=all_impact_ids,
                biotype_ids=all_biotype_ids,
            )
        )

        # ---------------------------------------------------------------------
        # Build final output
        # ---------------------------------------------------------------------
        for item in prepared_inputs:
            chrom = int(item["Chromosome"])
            qstart = int(item["Query Start"])
            qend = int(item["Query End"])
            key = (item["Input"], chrom, qstart, qend)
            base = dict(item["Base"])

            matched_variants = matched_variants_per_input.get(key, [])

            if not matched_variants:
                if emit_not_found_rows:
                    row = dict(base)
                    row["Status"] = "not_found"
                    row["Note"] = "No variants found overlapping query interval."  # noqa E501
                    row["Variants Found"] = 0
                    row["Consequences Found"] = 0
                    rows_out.append(row)
                continue

            total_consequences_this_input = 0

            for variant in matched_variants:
                vid = int(variant["variant_id"])
                effects = effects_by_chr_variant.get((chrom, vid), [])

                if not effects:
                    if include_variant_only_rows:
                        row = dict(base)
                        row["Status"] = "variant_only"
                        row["Note"] = (
                            "Variant found, but no molecular consequences found."  # noqa E501
                        )
                        row["Variants Found"] = len(matched_variants)
                        row["Consequences Found"] = 0
                        self._fill_variant_fields(
                            row=row,
                            variant=variant,
                            overlap=int(variant["_overlap_bp"]),
                            distance=int(variant["_distance_bp"]),
                        )
                        rows_out.append(row)
                    continue

                total_consequences_this_input += len(effects)

                for eff in effects:
                    row = dict(base)
                    row["Status"] = "resolved"
                    row["Note"] = None
                    row["Variants Found"] = len(matched_variants)
                    row["Consequences Found"] = len(effects)

                    self._fill_variant_fields(
                        row=row,
                        variant=variant,
                        overlap=int(variant["_overlap_bp"]),
                        distance=int(variant["_distance_bp"]),
                    )

                    self._fill_effect_fields(
                        row=row,
                        effect=eff,
                        consequence_map=consequence_map,
                        group_map=group_map,
                        category_map=category_map,
                        impact_map=impact_map,
                        biotype_map=biotype_map,
                    )

                    rows_out.append(row)

            # Optional: if only variant_only rows were emitted, keep counts aligned  # noqa E501
            if total_consequences_this_input == 0:
                pass

        if not rows_out:
            return pd.DataFrame(columns=self.columns)

        df = pd.DataFrame(rows_out)

        # Ensure stable output schema/order
        for col in self.columns:
            if col not in df.columns:
                df[col] = None

        df = df[self.columns]

        # Stable sort
        sort_cols = [
            "Input",
            "Input Chromosome (encoded)",
            "Query Start",
            "Variant ID",
            "Transcript ID",
            "Consequence ID",
        ]
        sort_cols = [c for c in sort_cols if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, kind="stable").reset_index(drop=True)  # noqa E501

        return df
