from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased

from biofilter.modules.report.reports.base_report import ReportBase

# -----------------------------------------------------------------------------
# Core ORM models (must exist)
# -----------------------------------------------------------------------------
from biofilter.modules.db.models.model_entities import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
)
from biofilter.modules.db.models.model_config import GenomeAssembly

# -----------------------------------------------------------------------------
# Variant SNPs is a partitioned/core table (NOT an ORM model)
# We will map it using SQLAlchemy Core (Table) via map_variant_snp().
# Adjust these imports if your paths differ.
# -----------------------------------------------------------------------------
try:
    from biofilter.modules.db.base import (
        Base,
    )  # Declarative Base (same metadata used by core tables)
except Exception:  # pragma: no cover
    Base = None  # type: ignore

try:
    # Your imperative table mapper (as you shared)
    from biofilter.modules.db.models.model_variants import map_variant_snp
except Exception:  # pragma: no cover
    map_variant_snp = None  # type: ignore

try:
    # Optional: if you have a registry helper that calls map_variant_snp(engine, Base.metadata)
    from biofilter.modules.db.core_tables import register_imperative_tables
except Exception:  # pragma: no cover
    register_imperative_tables = None  # type: ignore

# -----------------------------------------------------------------------------
# Optional gene-domain models (if present in your schema)
# -----------------------------------------------------------------------------
try:
    from biofilter.modules.db.models.model_genes import (
        GeneMaster,
        GeneGroupMembership,
        GeneGroup,
        GeneLocusGroup,
        GeneLocusType,
    )
except Exception:  # pragma: no cover
    GeneMaster = None  # type: ignore
    GeneGroupMembership = None  # type: ignore
    GeneGroup = None  # type: ignore
    GeneLocusGroup = None  # type: ignore
    GeneLocusType = None  # type: ignore


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

    Accepts: "1", "chr1", "X", "chrX", "MT", "M", "chrM", etc.
    """
    s = _norm_str(chr_value).lower()
    if not s:
        return None
    s = s.replace("chrom", "").replace("chr", "").strip()
    if s in ("x",):
        return 23
    if s in ("y",):
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


def _parse_rs_id(x: Any) -> Optional[int]:
    """
    Parse rs123 -> 123
    """
    s = _norm_str(x).lower()
    if not s:
        return None
    s = s.replace("rs", "").strip()
    try:
        return int(s)
    except Exception:
        return None


def _build_to_int(build: Any) -> Optional[int]:
    b = _parse_int(build)
    if b in (37, 38):
        return b
    return None


def _overlap_bp(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    """
    Return overlap length in base-pairs (inclusive coordinates).
    If no overlap, returns 0.
    """
    return max(0, min(a_end, b_end) - max(a_start, b_start) + 1)


def _distance_bp(
    window_start: int, window_end: int, gene_start: int, gene_end: int
) -> int:
    """
    Return distance in bp between [window_start, window_end] and [gene_start, gene_end].
    0 if overlapping.
    """
    if _overlap_bp(window_start, window_end, gene_start, gene_end) > 0:
        return 0
    if window_end < gene_start:
        return gene_start - window_end
    if window_start > gene_end:
        return window_start - gene_end
    # Should not happen if overlap == 0, but keep safe.
    return 0


@dataclass
class NormalizedInput:
    raw: str
    input_type: str  # "pos" | "rs" | "unknown"
    build_in: Optional[int]
    chr_in: Optional[int]
    pos_in: Optional[int]
    rs_id: Optional[int]


# -----------------------------------------------------------------------------
# Report
# -----------------------------------------------------------------------------
class PositionToGeneReport(ReportBase):
    """
    Position/Variant -> Gene report.

    - Input supports:
        * position: (build, chr, pos)
        * variant: rsID (resolved via Core table 'variant_snps' -> position_38)
    - Always searches genes in build 38 using EntityLocation (interval overlap).
    - Returns 1 row per (input x gene). If no gene is found, returns 1 row with
      status=not_found and gene fields as NULL (if emit_not_found_rows=True).
    - Adds Overlap and Distance metrics to support downstream ranking.
    """

    name = "position_to_gene"
    description = (
        "Given genomic positions (build 37/38) or rsIDs, returns genes overlapping a configurable "
        "window using EntityLocation (build 38). Includes overlap and distance metrics."
    )

    columns = [
        # Input / normalization
        "Input",
        "Input Type",
        "Build In",
        "Chr In (encoded)",
        "Position In",
        "rsID",
        "Range Up",
        "Range Down",
        "Search Start In",
        "Search End In",
        # Build 38 derived coordinates (used for gene search)
        "Chr 38 (encoded)",
        "Position 38",
        "Search Start 38",
        "Search End 38",
        # Status
        "Status",
        "Note",
        "Genes Found",
        # Gene fields
        "Gene Entity ID",
        "Gene Symbol (primary)",
        "Gene Start (Build 38)",
        "Gene End (Build 38)",
        "Overlap (bp)",
        "Distance to Gene (bp)",
        "Gene Strand",
        "Gene Region Label",
        # Gene identifiers (heuristic from code aliases)
        "Entrez ID",
        "Ensembl ID",
        "HGNC ID",
        # Optional gene metadata
        "Gene Locus Group",
        "Gene Locus Type",
        "Gene Groups",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def explain(cls) -> str:
        return """\
🧬 POSITION → GENE Report (v1)
=============================

Purpose:
- Map genomic inputs to genes using Biofilter's persistent gene locations.

Inputs:
- items: list[str] or list[list] (recommended)
  Supported tokens:
    - "pos:38:1:4000"
    - "pos:GRCh37:chr1:4000"
    - "rs:rs123"
    - "rs123" (auto-detected as rs)
- input_path: TSV/CSV file with columns:
    input_type, build, chromosome, position, variant_id

Key params:
- range_up (default 0): extend search window upward
- range_down (default 0): extend search window downward
- default_build (default 38): used when build missing for position inputs
- emit_not_found_rows (default True): keep input row even when no gene is found
- limit_genes (default 50): safety bound for dense regions

Build behavior:
- Gene search is always performed on build 38 (EntityLocation.build == 38).
- For rs inputs: uses Core table 'variant_snps' (source_type='rs', source_id=<rs_int>) to get position_38.
- For position inputs in build 37: liftover is not performed in v1; status=unmapped_build.

Output:
- One row per input-gene match.
- If no genes found, emits a single row with Status=not_found (if enabled).
- Includes:
    * Overlap (bp)
    * Distance to Gene (bp)
for downstream ranking without hard-coded gene prioritization.
"""

    @classmethod
    def example_input(cls) -> list[str]:
        return [
            "pos:38:1:4000",
            "pos:38:X:154000000",
            "rs:rs429358",  # APOE region
        ]

    # -------------------------------------------------------------------------
    # Parsing
    # -------------------------------------------------------------------------
    def _normalize_items(self, items: Any) -> list[NormalizedInput]:
        """
        Accepts list inputs and normalizes into (pos|rs).
        """
        out: list[NormalizedInput] = []

        if not items:
            return out

        if isinstance(items, (str, int)):
            items = [items]

        for obj in items:
            raw = str(obj)

            # list/tuple form: ["pos","38","1",4000] or ["rs","rs123"]
            if isinstance(obj, (list, tuple)) and obj:
                tag = _norm_str(obj[0]).lower()
                if tag == "pos" and len(obj) >= 4:
                    build_in = _build_to_int(obj[1])
                    chr_in = _parse_chr_to_int(obj[2])
                    pos_in = _parse_int(obj[3])
                    out.append(
                        NormalizedInput(
                            raw=raw,
                            input_type="pos",
                            build_in=build_in,
                            chr_in=chr_in,
                            pos_in=pos_in,
                            rs_id=None,
                        )
                    )
                    continue
                if tag in ("rs", "variant") and len(obj) >= 2:
                    rs_id = _parse_rs_id(obj[1])
                    out.append(
                        NormalizedInput(
                            raw=raw,
                            input_type="rs",
                            build_in=None,
                            chr_in=None,
                            pos_in=None,
                            rs_id=rs_id,
                        )
                    )
                    continue

            # string form
            s = _norm_str(obj)
            sl = s.lower()

            # explicit "pos:*"
            if sl.startswith("pos:"):
                parts = s.split(":")
                # pos:38:1:4000 or pos:GRCh37:chr1:4000
                build_in = None
                chr_in = None
                pos_in = None
                if len(parts) >= 4:
                    build_token = _norm_str(parts[1]).lower().replace("grch", "")
                    build_in = _build_to_int(build_token)
                    chr_in = _parse_chr_to_int(parts[2])
                    pos_in = _parse_int(parts[3])
                out.append(
                    NormalizedInput(
                        raw=s,
                        input_type="pos",
                        build_in=build_in,
                        chr_in=chr_in,
                        pos_in=pos_in,
                        rs_id=None,
                    )
                )
                continue

            # explicit "rs:*"
            if sl.startswith("rs:") or sl.startswith("variant:"):
                rs_token = s.split(":", 1)[1]
                rs_id = _parse_rs_id(rs_token)
                out.append(
                    NormalizedInput(
                        raw=s,
                        input_type="rs",
                        build_in=None,
                        chr_in=None,
                        pos_in=None,
                        rs_id=rs_id,
                    )
                )
                continue

            # auto-detect rs123
            if sl.startswith("rs") and _parse_rs_id(s) is not None:
                out.append(
                    NormalizedInput(
                        raw=s,
                        input_type="rs",
                        build_in=None,
                        chr_in=None,
                        pos_in=None,
                        rs_id=_parse_rs_id(s),
                    )
                )
                continue

            # fallback: treat as invalid
            out.append(
                NormalizedInput(
                    raw=s,
                    input_type="unknown",
                    build_in=None,
                    chr_in=None,
                    pos_in=None,
                    rs_id=None,
                )
            )

        return out

    def _read_input_file(self, input_path: str) -> list[NormalizedInput]:
        """
        Reads TSV/CSV with columns:
          input_type, build, chromosome, position, variant_id
        """
        path = _norm_str(input_path)
        if not path:
            return []

        # Try TSV first, fallback CSV
        try:
            df = pd.read_csv(path, sep="\t")
        except Exception:
            df = pd.read_csv(path)

        def _row_to_norm(r) -> NormalizedInput:
            input_type = _norm_str(r.get("input_type")).lower()
            build_in = _build_to_int(r.get("build"))
            chr_in = _parse_chr_to_int(r.get("chromosome"))
            pos_in = _parse_int(r.get("position"))
            rs_id = _parse_rs_id(r.get("variant_id"))
            raw = f"{input_type}|{build_in}|{chr_in}|{pos_in}|{rs_id}"
            if input_type in ("rs", "variant"):
                return NormalizedInput(
                    raw=raw,
                    input_type="rs",
                    build_in=None,
                    chr_in=None,
                    pos_in=None,
                    rs_id=rs_id,
                )
            if input_type == "pos":
                return NormalizedInput(
                    raw=raw,
                    input_type="pos",
                    build_in=build_in,
                    chr_in=chr_in,
                    pos_in=pos_in,
                    rs_id=None,
                )
            return NormalizedInput(
                raw=raw,
                input_type="unknown",
                build_in=build_in,
                chr_in=chr_in,
                pos_in=pos_in,
                rs_id=rs_id,
            )

        return [_row_to_norm(r) for r in df.to_dict(orient="records")]

    # -------------------------------------------------------------------------
    # Core
    # -------------------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        # -----------------------------
        # Params
        # -----------------------------
        items = self.params.get("items")
        input_path = self.params.get("input_path")

        range_up = _parse_int(self.params.get("range_up", 0)) or 0
        range_down = _parse_int(self.params.get("range_down", 0)) or 0
        if range_up < 0:
            range_up = 0
        if range_down < 0:
            range_down = 0

        default_build = _build_to_int(self.params.get("default_build", 38)) or 38
        emit_not_found_rows = bool(self.params.get("emit_not_found_rows", True))
        limit_genes = _parse_int(self.params.get("limit_genes", 50)) or 50
        if limit_genes < 1:
            limit_genes = 50

        output_columns = self.params.get("output_columns")
        if output_columns is not None:
            if isinstance(output_columns, str):
                output_columns = [output_columns]
            output_columns = [
                str(c).strip() for c in output_columns if c and str(c).strip()
            ]
            allowed = set(self.available_columns())
            unknown = [c for c in output_columns if c not in allowed]
            if unknown:
                self.logger.log(
                    f"Unknown output_columns (must match display names): {unknown}. "
                    f"Allowed: {sorted(allowed)}",
                    "ERROR",
                )
                return pd.DataFrame()

        # -----------------------------
        # Normalize inputs
        # -----------------------------
        norm_inputs: list[NormalizedInput] = []
        if input_path:
            norm_inputs.extend(self._read_input_file(input_path))
        if items:
            norm_inputs.extend(self._normalize_items(items))

        if not norm_inputs:
            self.logger.log(
                "No inputs provided. Use items=... or input_path=...", "ERROR"
            )
            return pd.DataFrame()

        # Resolve Gene group id
        gene_group_id = (
            self.session.query(EntityGroup.id)
            .filter(EntityGroup.name.ilike("Genes"))
            .scalar()
        )
        if not gene_group_id:
            self.logger.log("EntityGroup 'Genes' not found.", "ERROR")
            return pd.DataFrame()

        # Resolve GenomeAssembly id for build 38
        asm38 = (
            self.session.query(GenomeAssembly)
            .filter(or_(GenomeAssembly.build == 38, GenomeAssembly.name.ilike("%38%")))
            .first()
        )
        if not asm38:
            self.logger.log("GenomeAssembly for build 38 not found.", "ERROR")
            return pd.DataFrame()

        asm38_id = asm38.id

        # -----------------------------
        # Resolve rs -> position_38 (bulk) using SQLAlchemy Core table
        # -----------------------------
        rs_inputs = [
            x for x in norm_inputs if x.input_type == "rs" and x.rs_id is not None
        ]
        rs_map: dict[int, dict[str, Any]] = {}

        if rs_inputs:
            if map_variant_snp is None or Base is None:
                self.logger.log(
                    "variant_snps table mapping not available. "
                    "Ensure Base and map_variant_snp(engine, metadata) imports are correct.",
                    "ERROR",
                )
                return pd.DataFrame()

            rs_ids = sorted({x.rs_id for x in rs_inputs if x.rs_id is not None})

            engine = self.session.get_bind()

            # Ensure the core table exists in metadata (partition-aware)
            # Option A (recommended): if you already have a centralized registry:
            if register_imperative_tables is not None:
                try:
                    register_imperative_tables(engine)
                except Exception:
                    # Safe fallback: we can still map directly below
                    pass

            # Option B: map directly
            variant_snps = map_variant_snp(engine, Base.metadata)

            # We store rs as (source_type='rs', source_id=<int>)
            stmt = select(
                variant_snps.c.source_id.label("rs_id"),
                variant_snps.c.chromosome.label("chr"),
                variant_snps.c.position_38.label("pos38"),
                variant_snps.c.position_37.label("pos37"),
            ).where(
                and_(
                    variant_snps.c.source_type == "rs",
                    variant_snps.c.source_id.in_(rs_ids),
                )
            )

            rows = self.session.execute(stmt).all()
            for row in rows:
                rs_id = int(row.rs_id)
                rs_map[rs_id] = {
                    "chr": int(row.chr) if row.chr is not None else None,
                    "pos38": int(row.pos38) if row.pos38 is not None else None,
                    "pos37": int(row.pos37) if row.pos37 is not None else None,
                }

        # -----------------------------
        # Build search intervals (build 38)
        # -----------------------------
        prepared: list[dict[str, Any]] = []
        for inp in norm_inputs:
            base_row = {
                "Input": inp.raw,
                "Input Type": inp.input_type,
                "Build In": inp.build_in,
                "Chr In (encoded)": inp.chr_in,
                "Position In": inp.pos_in,
                "rsID": f"rs{inp.rs_id}" if inp.rs_id is not None else None,
                "Range Up": range_up,
                "Range Down": range_down,
                "Search Start In": None,
                "Search End In": None,
                "Chr 38 (encoded)": None,
                "Position 38": None,
                "Search Start 38": None,
                "Search End 38": None,
                "Status": None,
                "Note": None,
            }

            if inp.input_type == "pos":
                build_in = inp.build_in or default_build
                chr_in = inp.chr_in
                pos_in = inp.pos_in

                if chr_in is None or pos_in is None:
                    base_row["Status"] = "invalid_input"
                    base_row["Note"] = "Missing chromosome/position."
                    prepared.append(base_row)
                    continue

                start_in = max(1, pos_in - range_down)
                end_in = pos_in + range_up
                base_row["Search Start In"] = start_in
                base_row["Search End In"] = end_in

                if build_in != 38:
                    # v1: no liftover from 37 -> 38
                    base_row["Status"] = "unmapped_build"
                    base_row["Note"] = (
                        "Build 37 position inputs require liftover (not supported in v1)."
                    )
                    prepared.append(base_row)
                    continue

                base_row["Chr 38 (encoded)"] = chr_in
                base_row["Position 38"] = pos_in
                base_row["Search Start 38"] = start_in
                base_row["Search End 38"] = end_in
                base_row["Status"] = "ready"
                prepared.append(base_row)
                continue

            if inp.input_type == "rs":
                if inp.rs_id is None:
                    base_row["Status"] = "invalid_input"
                    base_row["Note"] = "Invalid rsID."
                    prepared.append(base_row)
                    continue

                hit = rs_map.get(inp.rs_id)
                if not hit:
                    base_row["Status"] = "not_found"
                    base_row["Note"] = "Variant not found in variant_snps."
                    prepared.append(base_row)
                    continue

                chr38 = hit.get("chr")
                pos38 = hit.get("pos38")
                if chr38 is None or pos38 is None:
                    base_row["Status"] = "unmapped_build"
                    base_row["Note"] = "Variant missing position_38."
                    prepared.append(base_row)
                    continue

                start38 = max(1, int(pos38) - range_down)
                end38 = int(pos38) + range_up

                base_row["Chr 38 (encoded)"] = int(chr38)
                base_row["Position 38"] = int(pos38)
                base_row["Search Start 38"] = start38
                base_row["Search End 38"] = end38
                base_row["Status"] = "ready"
                prepared.append(base_row)
                continue

            base_row["Status"] = "invalid_input"
            base_row["Note"] = "Unrecognized input format."
            prepared.append(base_row)

        # Only rows ready for gene search
        ready = [r for r in prepared if r["Status"] == "ready"]

        # -----------------------------
        # Query genes by overlap (bulk by chromosome)
        # -----------------------------
        gene_hits: dict[tuple[str, int, int, int], list[dict[str, Any]]] = {}

        if ready:
            # Group inputs by chr38 to reduce queries
            by_chr: dict[int, list[dict[str, Any]]] = {}
            for r in ready:
                c = int(r["Chr 38 (encoded)"])
                by_chr.setdefault(c, []).append(r)

            # Primary alias join (symbol)
            PrimaryAlias = aliased(EntityAlias)

            for chr38, rows in by_chr.items():
                # Envelope window to reduce location rows fetched
                min_start = min(int(r["Search Start 38"]) for r in rows)
                max_end = max(int(r["Search End 38"]) for r in rows)

                q = (
                    self.session.query(
                        EntityLocation.entity_id.label("gene_entity_id"),
                        EntityLocation.start_pos.label("gene_start"),
                        EntityLocation.end_pos.label("gene_end"),
                        EntityLocation.strand.label("gene_strand"),
                        EntityLocation.region_label.label("region_label"),
                        PrimaryAlias.alias_value.label("gene_symbol"),
                    )
                    .join(Entity, Entity.id == EntityLocation.entity_id)
                    .outerjoin(
                        PrimaryAlias,
                        and_(
                            PrimaryAlias.entity_id == Entity.id,
                            PrimaryAlias.is_primary == True,  # noqa: E712
                        ),
                    )
                    .filter(
                        EntityLocation.assembly_id == asm38_id,
                        EntityLocation.build == 38,
                        EntityLocation.chromosome == chr38,
                        EntityLocation.entity_group_id == gene_group_id,
                        EntityLocation.start_pos <= max_end,
                        EntityLocation.end_pos >= min_start,
                    )
                )

                loc_rows = q.all()

                # Match each input interval against returned locations
                for r in rows:
                    s = int(r["Search Start 38"])
                    e = int(r["Search End 38"])
                    key = (r["Input"], int(chr38), s, e)
                    gene_hits[key] = []

                    for g in loc_rows:
                        gs = int(g.gene_start)
                        ge = int(g.gene_end)

                        ov = _overlap_bp(s, e, gs, ge)
                        # v1 match policy: overlap only
                        if ov <= 0:
                            continue

                        dist = _distance_bp(s, e, gs, ge)

                        gene_hits[key].append(
                            {
                                "Gene Entity ID": int(g.gene_entity_id),
                                "Gene Symbol (primary)": g.gene_symbol,
                                "Gene Start (Build 38)": gs,
                                "Gene End (Build 38)": ge,
                                "Overlap (bp)": int(ov),
                                "Distance to Gene (bp)": int(dist),
                                "Gene Strand": g.gene_strand,
                                "Gene Region Label": g.region_label,
                            }
                        )

        # -----------------------------
        # Gene identifiers via EntityAlias (codes) + gene domain metadata (optional)
        # -----------------------------
        all_gene_entity_ids = sorted(
            {g["Gene Entity ID"] for genes in gene_hits.values() for g in genes}
        )

        # Map entity_id -> codes dict
        code_map: dict[int, dict[str, Optional[str]]] = {}
        if all_gene_entity_ids:
            # Heuristics for id sources (adjust based on your ETL conventions)
            hgnc_sources = {"HGNC"}
            ensembl_sources = {"ENSEMBL"}
            entrez_sources = {"ENTREZ", "NCBI"}

            codes = (
                self.session.query(
                    EntityAlias.entity_id,
                    EntityAlias.xref_source,
                    EntityAlias.alias_value,
                    EntityAlias.alias_type,
                )
                .filter(
                    EntityAlias.entity_id.in_(all_gene_entity_ids),
                    EntityAlias.alias_type.ilike("code"),
                )
                .all()
            )

            for row in codes:
                eid = int(row.entity_id)
                src = (_norm_str(row.xref_source) or "").upper()
                val = _norm_str(row.alias_value) or None

                if eid not in code_map:
                    code_map[eid] = {
                        "Entrez ID": None,
                        "Ensembl ID": None,
                        "HGNC ID": None,
                    }

                if val is None:
                    continue

                # HGNC ID often looks like "HGNC:11998"
                if (
                    src in hgnc_sources or val.upper().startswith("HGNC:")
                ) and code_map[eid]["HGNC ID"] is None:
                    code_map[eid]["HGNC ID"] = val

                # Ensembl ID often starts with ENSG
                if (
                    src in ensembl_sources or val.upper().startswith("ENSG")
                ) and code_map[eid]["Ensembl ID"] is None:
                    code_map[eid]["Ensembl ID"] = val

                # Entrez is usually digits
                if (src in entrez_sources or val.isdigit()) and code_map[eid][
                    "Entrez ID"
                ] is None:
                    code_map[eid]["Entrez ID"] = val

        # Optional joins to GeneMaster / locus / groups
        locus_map: dict[int, dict[str, Any]] = {}
        groups_map: dict[int, list[str]] = {}

        if all_gene_entity_ids and GeneMaster is not None:
            # GeneMaster by entity_id
            gm_rows = (
                self.session.query(GeneMaster)
                .filter(GeneMaster.entity_id.in_(all_gene_entity_ids))
                .all()
            )

            # Capture locus group/type if relationships exist
            for gm in gm_rows:
                eid = int(gm.entity_id)
                locus_map.setdefault(eid, {})
                locus_map[eid]["Gene Locus Group"] = getattr(
                    getattr(gm, "gene_locus_group", None), "name", None
                )
                locus_map[eid]["Gene Locus Type"] = getattr(
                    getattr(gm, "gene_locus_type", None), "name", None
                )

            # Gene functional groups (many-to-many)
            if GeneGroupMembership is not None and GeneGroup is not None:
                gm_id_by_entity = {int(gm.entity_id): int(gm.id) for gm in gm_rows}
                if gm_id_by_entity:
                    mem_rows = (
                        self.session.query(
                            GeneGroupMembership.gene_id,
                            GeneGroup.name.label("group_name"),
                        )
                        .join(GeneGroup, GeneGroup.id == GeneGroupMembership.group_id)
                        .filter(
                            GeneGroupMembership.gene_id.in_(
                                list(gm_id_by_entity.values())
                            )
                        )
                        .all()
                    )

                    # invert map gene_id -> entity_id
                    ent_by_gene_id = {gid: eid for eid, gid in gm_id_by_entity.items()}

                    for m in mem_rows:
                        eid = ent_by_gene_id.get(int(m.gene_id))
                        if eid is None:
                            continue
                        groups_map.setdefault(eid, []).append(m.group_name)

        # -----------------------------
        # Assemble output rows
        # -----------------------------
        out_rows: list[dict[str, Any]] = []

        for r in prepared:
            if r["Status"] != "ready":
                if emit_not_found_rows:
                    row = {c: None for c in self.columns}
                    row.update(r)
                    row["Genes Found"] = 0
                    out_rows.append(row)
                continue

            chr38 = int(r["Chr 38 (encoded)"])
            s38 = int(r["Search Start 38"])
            e38 = int(r["Search End 38"])
            key = (r["Input"], chr38, s38, e38)

            genes = gene_hits.get(key, [])

            if not genes:
                if emit_not_found_rows:
                    row = {c: None for c in self.columns}
                    row.update(r)
                    row["Status"] = "not_found"
                    row["Note"] = r.get("Note") or "No genes overlap this interval."
                    row["Genes Found"] = 0
                    out_rows.append(row)
                continue

            # Deterministic + useful ordering:
            # 1) larger overlap first, 2) smaller distance first, 3) symbol, 4) entity_id
            genes = sorted(
                genes,
                key=lambda x: (
                    -(x.get("Overlap (bp)") or 0),
                    (x.get("Distance to Gene (bp)") or 0),
                    x.get("Gene Symbol (primary)") or "",
                    x.get("Gene Entity ID") or 0,
                ),
            )

            for g in genes[:limit_genes]:
                row = {c: None for c in self.columns}
                row.update(r)
                row.update(g)

                eid = int(g["Gene Entity ID"])
                row["Genes Found"] = len(genes)

                # codes
                c = code_map.get(eid, {})
                row["Entrez ID"] = c.get("Entrez ID")
                row["Ensembl ID"] = c.get("Ensembl ID")
                row["HGNC ID"] = c.get("HGNC ID")

                # locus
                l = locus_map.get(eid, {})
                row["Gene Locus Group"] = l.get("Gene Locus Group")
                row["Gene Locus Type"] = l.get("Gene Locus Type")

                # groups
                gg = groups_map.get(eid, [])
                row["Gene Groups"] = gg if gg else []

                # final status
                row["Status"] = "resolved"
                row["Note"] = r.get("Note")

                out_rows.append(row)

        out_df = pd.DataFrame(out_rows, columns=self.columns)

        # Column filtering
        if output_columns is not None:
            out_df = out_df[output_columns].copy()

        return out_df
