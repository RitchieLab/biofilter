from __future__ import annotations

import re
from typing import Any

import pandas as pd
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    EntityRelationship,
    GeneMaster,
    GeneGroup,
    GeneGroupMembership,
    GeneLocusGroup,
)
from biofilter.modules.report.reports.base_report import ReportBase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm_str(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_chr_to_int(chr_value: Any) -> int | None:
    s = _norm_str(chr_value).lower()
    s = s.replace("chromosome", "").replace("chrom", "").replace("chr", "").strip()
    if s == "x":
        return 23
    if s == "y":
        return 24
    if s in {"m", "mt", "mito", "mitochondria"}:
        return 25
    try:
        v = int(s)
        return v if 1 <= v <= 25 else None
    except Exception:
        return None


def _parse_input_variant(raw: Any) -> dict[str, Any]:
    """
    Parse chr:pos or rsID input.

    Returns dict with keys: type ('position'|'rsid'), chromosome (int|None),
    position (int|None), rsid (str|None), raw (str).
    """
    s = _norm_str(raw)
    result: dict[str, Any] = {
        "raw": s, "type": None, "chromosome": None, "position": None, "rsid": None,
    }
    if not s:
        return result

    # rsID pattern
    if re.match(r"^rs\d+$", s, re.IGNORECASE):
        result["type"] = "rsid"
        result["rsid"] = s.lower()
        return result

    # chr:pos pattern — separators: : ; , - (space)
    clean = s.lower().replace("chr", "")
    parts = re.split(r"[:;,\-\s]+", clean, maxsplit=1)
    if len(parts) == 2:
        chrom = _parse_chr_to_int(parts[0])
        try:
            pos = int(parts[1].strip())
        except Exception:
            pos = None
        if chrom is not None and pos is not None:
            result["type"] = "position"
            result["chromosome"] = chrom
            result["position"] = pos
            return result

    return result


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

class OneVariantAnnotationReport(ReportBase):
    name = "one_variant_annotation"
    description = (
        "Given a single variant (chr:pos or rsID), identifies the overlapping "
        "gene via entity_locations, then expands through all pathways that gene "
        "participates in to build a partner-gene list annotated with shared "
        "pathway information, genomic locations, and gene group classifications."
    )

    @classmethod
    def example_input(cls):
        return {
            "input_variant": "chr19:44904604",
            "build": 38,
            "pathway_sources": None,   # None = all sources
        }

    @classmethod
    def explain(cls) -> str:
        return (
            "Parameters:\n"
            "  input_variant   (required) chr:pos (e.g. chr19:44904604) or rsID\n"
            "  build           (optional) genome build, 37 or 38 (default 38)\n"
            "  pathway_sources (optional) list of source names to restrict pathways "
            "(e.g. ['Reactome','KEGG'])\n"
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        input_raw = self.param("input_variant", required=True)
        build_raw = self.param("build", default=38)
        pathway_sources_raw = self.param("pathway_sources", default=None)

        # -- 1. Parse input ------------------------------------------------
        parsed = _parse_input_variant(input_raw)
        if parsed["type"] is None:
            self.logger.log(f"Could not parse input variant: {input_raw!r}", "ERROR")
            return pd.DataFrame()

        build = int(str(build_raw).strip()) if build_raw else 38
        pathway_source_filter: set[str] = (
            {s.strip().lower() for s in pathway_sources_raw}
            if isinstance(pathway_sources_raw, (list, tuple, set))
            else set()
        )

        # -- 2. Look up variant_masters ------------------------------------
        seed_info = self._resolve_seed_variant(parsed)
        if seed_info is None:
            self.logger.log(
                f"Variant not found in variant_masters: {input_raw!r}", "WARNING"
            )
            return pd.DataFrame()

        chrom_int: int = seed_info["chromosome"]
        position: int = seed_info["position"]

        self.logger.log(
            f"Seed variant resolved → chr{chrom_int}:{position} "
            f"({seed_info['allele_count']} allele row(s), rsid={seed_info['rsid']})"
        )

        # -- 3. Find seed gene via entity_locations ------------------------
        assembly_map = self.resolve_assembly_map(str(build))
        assembly_ids = list(assembly_map.values())
        if not assembly_ids:
            self.logger.log(f"No assembly found for build {build}", "WARNING")

        gene_group = (
            self.session.query(EntityGroup)
            .filter(EntityGroup.name == "Genes")
            .first()
        )
        if gene_group is None:
            self.logger.log("EntityGroup 'Genes' not found", "ERROR")
            return pd.DataFrame()

        pathway_group = (
            self.session.query(EntityGroup)
            .filter(EntityGroup.name == "Pathways")
            .first()
        )
        if pathway_group is None:
            self.logger.log("EntityGroup 'Pathways' not found", "ERROR")
            return pd.DataFrame()

        seed_gene = self._find_gene_at_position(
            chrom_int, position, gene_group, assembly_ids
        )
        if seed_gene is None:
            self.logger.log(
                f"No gene found at chr{chrom_int}:{position} (build {build})",
                "WARNING",
            )
            return pd.DataFrame()

        self.logger.log(
            f"Seed gene → {seed_gene['gene_symbol']} (entity_id={seed_gene['entity_id']})"
        )

        # -- 4. Gene groups for seed gene ----------------------------------
        seed_gene_meta = self._get_gene_metadata_batch(
            [seed_gene["entity_id"]]
        )
        seed_gene.update(seed_gene_meta.get(seed_gene["entity_id"], {}))

        # -- 5. Find all pathways for the seed gene ------------------------
        pathways = self._get_gene_pathways(
            seed_gene["entity_id"], pathway_group, pathway_source_filter
        )
        if not pathways:
            self.logger.log(
                f"No pathways found for gene {seed_gene['gene_symbol']}", "WARNING"
            )
            return pd.DataFrame()

        pathway_entity_ids = {p["pathway_entity_id"] for p in pathways}
        self.logger.log(
            f"Pathways found for {seed_gene['gene_symbol']}: {len(pathway_entity_ids)}"
        )

        # -- 6. Find all partner genes in those pathways -------------------
        partner_genes = self._get_genes_in_pathways(
            pathway_entity_ids, seed_gene["entity_id"], gene_group
        )
        self.logger.log(f"Partner genes found: {len(partner_genes)}")

        # -- 7. Enrich partner genes with locations and gene groups --------
        partner_entity_ids = list(partner_genes.keys())

        partner_locations = self._get_gene_locations_batch(
            partner_entity_ids, gene_group, assembly_ids
        )
        partner_meta = self._get_gene_metadata_batch(partner_entity_ids)

        for gene_id, partner in partner_genes.items():
            partner.update(partner_locations.get(gene_id, {}))
            partner.update(partner_meta.get(gene_id, {}))

        # -- 8. Count unique variants per gene (seed + partners) -----------
        # Build unified gene list with location data for the count query
        seed_gene_loc = {
            "entity_id": seed_gene["entity_id"],
            "chromosome": seed_gene.get("chromosome"),
            "start_pos": seed_gene.get("start_pos"),
            "end_pos": seed_gene.get("end_pos"),
        }
        partner_gene_locs = [
            {
                "entity_id": g["entity_id"],
                "chromosome": g.get("partner_chromosome"),
                "start_pos": g.get("partner_start_pos"),
                "end_pos": g.get("partner_end_pos"),
            }
            for g in partner_genes.values()
        ]
        variant_counts = self._count_variants_per_gene(
            [seed_gene_loc] + partner_gene_locs
        )
        self.logger.log(
            f"Variant counts resolved for {len(variant_counts)} gene(s)"
        )

        # -- 9. Build output DataFrame -------------------------------------
        return self._build_output(
            seed_info=seed_info,
            seed_gene=seed_gene,
            pathways=pathways,
            partner_genes=partner_genes,
            pathway_entity_ids=pathway_entity_ids,
            variant_counts=variant_counts,
        )

    # ------------------------------------------------------------------
    # Step 2 — Resolve seed variant in variant_masters
    # ------------------------------------------------------------------

    def _resolve_seed_variant(self, parsed: dict[str, Any]) -> dict[str, Any] | None:
        from sqlalchemy import MetaData, Table

        metadata = MetaData()
        try:
            vm = Table("variant_masters", metadata, autoload_with=self.db.engine)
        except Exception:
            self.logger.log("variant_masters table not found", "ERROR")
            return None

        if parsed["type"] == "position":
            stmt = (
                select(
                    vm.c.chromosome,
                    vm.c.position_start,
                    func.count().label("allele_count"),
                    func.max(vm.c.rsid).label("rsid"),
                )
                .where(
                    and_(
                        vm.c.chromosome == parsed["chromosome"],
                        vm.c.position_start == parsed["position"],
                    )
                )
                .group_by(vm.c.chromosome, vm.c.position_start)
            )
        else:
            stmt = (
                select(
                    vm.c.chromosome,
                    vm.c.position_start,
                    func.count().label("allele_count"),
                    func.max(vm.c.rsid).label("rsid"),
                )
                .where(vm.c.rsid == parsed["rsid"])
                .group_by(vm.c.chromosome, vm.c.position_start)
            )

        rows = self.session.execute(stmt).mappings().all()
        if not rows:
            return None

        row = rows[0]
        return {
            "chromosome": int(row["chromosome"]),
            "position": int(row["position_start"]),
            "allele_count": int(row["allele_count"]),
            "rsid": _norm_str(row["rsid"]) or None,
            "input_raw": parsed["raw"],
        }

    # ------------------------------------------------------------------
    # Step 3 — Find overlapping gene in entity_locations
    # ------------------------------------------------------------------

    def _find_gene_at_position(
        self,
        chromosome: int,
        position: int,
        gene_group: EntityGroup,
        assembly_ids: list[int],
    ) -> dict[str, Any] | None:
        primary_alias = aliased(EntityAlias)

        q = (
            self.session.query(
                EntityLocation.entity_id.label("entity_id"),
                EntityLocation.chromosome.label("chromosome"),
                EntityLocation.start_pos.label("start_pos"),
                EntityLocation.end_pos.label("end_pos"),
                primary_alias.alias_value.label("gene_symbol"),
            )
            .join(Entity, Entity.id == EntityLocation.entity_id)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(
                EntityLocation.entity_group_id == gene_group.id,
                EntityLocation.chromosome == chromosome,
                EntityLocation.start_pos <= position,
                EntityLocation.end_pos >= position,
            )
        )
        if assembly_ids:
            q = q.filter(EntityLocation.assembly_id.in_(assembly_ids))

        rows = q.all()
        if not rows:
            return None

        # Pick the gene whose locus most tightly contains the position
        best = min(rows, key=lambda r: (r.end_pos - r.start_pos))
        return {
            "entity_id": int(best.entity_id),
            "gene_symbol": _norm_str(best.gene_symbol) or f"entity_{best.entity_id}",
            "chromosome": int(best.chromosome),
            "start_pos": int(best.start_pos),
            "end_pos": int(best.end_pos),
        }

    # ------------------------------------------------------------------
    # Step 4 / 7a — Gene groups and locus group for a batch of entity_ids
    # ------------------------------------------------------------------

    def _get_gene_metadata_batch(
        self, entity_ids: list[int]
    ) -> dict[int, dict[str, Any]]:
        """
        Returns dict: entity_id → {locus_group, gene_groups}

        - locus_group: str (e.g. "protein-coding gene")
        - gene_groups: pipe-separated string of functional group names
          (e.g. "HOX Cluster|Transcription Factors")
        """
        if not entity_ids:
            return {}

        BATCH = 500
        result: dict[int, dict[str, Any]] = {}

        for i in range(0, len(entity_ids), BATCH):
            batch = entity_ids[i : i + BATCH]

            # Locus group: gene_masters → gene_locus_groups
            locus_rows = (
                self.session.query(
                    GeneMaster.entity_id.label("entity_id"),
                    GeneLocusGroup.name.label("locus_group"),
                )
                .join(
                    GeneLocusGroup,
                    GeneLocusGroup.id == GeneMaster.locus_group_id,
                    isouter=True,
                )
                .filter(GeneMaster.entity_id.in_(batch))
                .all()
            )
            for row in locus_rows:
                eid = int(row.entity_id)
                result.setdefault(eid, {"locus_group": None, "gene_groups": None})
                result[eid]["locus_group"] = _norm_str(row.locus_group) or None

            # Functional groups: gene_masters → gene_group_memberships → gene_groups
            group_rows = (
                self.session.query(
                    GeneMaster.entity_id.label("entity_id"),
                    GeneGroup.name.label("group_name"),
                )
                .join(GeneGroupMembership, GeneGroupMembership.gene_id == GeneMaster.id)
                .join(GeneGroup, GeneGroup.id == GeneGroupMembership.group_id)
                .filter(GeneMaster.entity_id.in_(batch))
                .all()
            )
            # Collect all group names per entity
            entity_groups: dict[int, list[str]] = {}
            for row in group_rows:
                eid = int(row.entity_id)
                result.setdefault(eid, {"locus_group": None, "gene_groups": None})
                entity_groups.setdefault(eid, []).append(_norm_str(row.group_name))

            for eid, names in entity_groups.items():
                result[eid]["gene_groups"] = "|".join(sorted(set(names))) or None

        return result

    # ------------------------------------------------------------------
    # Step 7b — Genomic locations for a batch of partner entity_ids
    # ------------------------------------------------------------------

    def _get_gene_locations_batch(
        self,
        entity_ids: list[int],
        gene_group: EntityGroup,
        assembly_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        if not entity_ids:
            return {}

        BATCH = 500
        result: dict[int, dict[str, Any]] = {}

        for i in range(0, len(entity_ids), BATCH):
            batch = entity_ids[i : i + BATCH]

            q = (
                self.session.query(
                    EntityLocation.entity_id.label("entity_id"),
                    EntityLocation.chromosome.label("chromosome"),
                    EntityLocation.start_pos.label("start_pos"),
                    EntityLocation.end_pos.label("end_pos"),
                )
                .filter(
                    EntityLocation.entity_id.in_(batch),
                    EntityLocation.entity_group_id == gene_group.id,
                )
            )
            if assembly_ids:
                q = q.filter(EntityLocation.assembly_id.in_(assembly_ids))

            for row in q.all():
                eid = int(row.entity_id)
                # If multiple assemblies return duplicates, keep first
                if eid not in result:
                    result[eid] = {
                        "partner_chromosome": int(row.chromosome),
                        "partner_start_pos": int(row.start_pos),
                        "partner_end_pos": int(row.end_pos),
                    }

        return result

    # ------------------------------------------------------------------
    # Step 5 — Get pathways for seed gene
    # ------------------------------------------------------------------

    def _get_gene_pathways(
        self,
        gene_entity_id: int,
        pathway_group: EntityGroup,
        source_filter: set[str],
    ) -> list[dict[str, Any]]:
        from biofilter.modules.db.models import ETLDataSource

        q = (
            self.session.query(
                EntityRelationship.entity_1_id.label("entity_1_id"),
                EntityRelationship.entity_2_id.label("entity_2_id"),
                EntityRelationship.data_source_id.label("data_source_id"),
            )
            .filter(
                or_(
                    and_(
                        EntityRelationship.entity_1_id == gene_entity_id,
                        EntityRelationship.entity_2_group_id == pathway_group.id,
                    ),
                    and_(
                        EntityRelationship.entity_2_id == gene_entity_id,
                        EntityRelationship.entity_1_group_id == pathway_group.id,
                    ),
                )
            )
        )
        rows = q.all()

        # Preload data source names
        ds_ids = {int(r.data_source_id) for r in rows if r.data_source_id}
        ds_names: dict[int, str] = {}
        if ds_ids:
            ds_rows = (
                self.session.query(ETLDataSource.id, ETLDataSource.name)
                .filter(ETLDataSource.id.in_(ds_ids))
                .all()
            )
            ds_names = {int(r.id): _norm_str(r.name) for r in ds_rows}

        # Collect unique pathway entity_ids
        seen: set[int] = set()
        pathway_candidates: list[dict[str, Any]] = []
        for row in rows:
            pathway_entity_id = (
                int(row.entity_2_id)
                if int(row.entity_1_id) == gene_entity_id
                else int(row.entity_1_id)
            )
            if pathway_entity_id in seen:
                continue
            seen.add(pathway_entity_id)
            ds_name = ds_names.get(int(row.data_source_id or 0), "")
            if source_filter and ds_name.lower() not in source_filter:
                continue
            pathway_candidates.append({
                "pathway_entity_id": pathway_entity_id,
                "data_source_id": row.data_source_id,
                "data_source_name": ds_name,
            })

        if not pathway_candidates:
            return []

        # Batch-resolve pathway names
        pathway_ids = [p["pathway_entity_id"] for p in pathway_candidates]
        name_map: dict[int, str] = {}
        BATCH = 500
        for i in range(0, len(pathway_ids), BATCH):
            batch = pathway_ids[i : i + BATCH]
            rows_a = (
                self.session.query(EntityAlias.entity_id, EntityAlias.alias_value)
                .filter(
                    EntityAlias.entity_id.in_(batch),
                    EntityAlias.is_primary.is_(True),
                )
                .all()
            )
            for r in rows_a:
                name_map[int(r.entity_id)] = _norm_str(r.alias_value)

        pathways: list[dict[str, Any]] = []
        for p in pathway_candidates:
            pid = p["pathway_entity_id"]
            p["pathway_name"] = name_map.get(pid) or f"pathway_{pid}"
            pathways.append(p)

        return pathways

    # ------------------------------------------------------------------
    # Step 6 — Get all partner genes in those pathways
    # ------------------------------------------------------------------

    def _get_genes_in_pathways(
        self,
        pathway_entity_ids: set[int],
        seed_gene_entity_id: int,
        gene_group: EntityGroup,
    ) -> dict[int, dict[str, Any]]:
        """
        Returns dict: partner_entity_id → {entity_id, gene_symbol, pathway_entity_ids}
        """
        if not pathway_entity_ids:
            return {}

        BATCH = 500
        pathway_ids_list = list(pathway_entity_ids)
        gene_to_pathways: dict[int, set[int]] = {}

        for i in range(0, len(pathway_ids_list), BATCH):
            batch = pathway_ids_list[i : i + BATCH]

            q = (
                self.session.query(
                    EntityRelationship.entity_1_id.label("entity_1_id"),
                    EntityRelationship.entity_1_group_id.label("entity_1_group_id"),
                    EntityRelationship.entity_2_id.label("entity_2_id"),
                    EntityRelationship.entity_2_group_id.label("entity_2_group_id"),
                )
                .filter(
                    or_(
                        and_(
                            EntityRelationship.entity_1_id.in_(batch),
                            EntityRelationship.entity_2_group_id == gene_group.id,
                        ),
                        and_(
                            EntityRelationship.entity_2_id.in_(batch),
                            EntityRelationship.entity_1_group_id == gene_group.id,
                        ),
                    )
                )
            )

            for row in q.all():
                e1, e2 = int(row.entity_1_id), int(row.entity_2_id)
                if row.entity_2_group_id == gene_group.id:
                    gene_id, pathway_id = e2, e1
                else:
                    gene_id, pathway_id = e1, e2

                if gene_id == seed_gene_entity_id:
                    continue

                gene_to_pathways.setdefault(gene_id, set()).add(pathway_id)

        if not gene_to_pathways:
            return {}

        # Batch-resolve gene symbols
        gene_ids = list(gene_to_pathways.keys())
        gene_symbols: dict[int, str] = {}

        for i in range(0, len(gene_ids), BATCH):
            batch = gene_ids[i : i + BATCH]
            rows = (
                self.session.query(EntityAlias.entity_id, EntityAlias.alias_value)
                .filter(
                    EntityAlias.entity_id.in_(batch),
                    EntityAlias.is_primary.is_(True),
                )
                .all()
            )
            for row in rows:
                gene_symbols[int(row.entity_id)] = _norm_str(row.alias_value)

        return {
            gene_id: {
                "entity_id": gene_id,
                "gene_symbol": gene_symbols.get(gene_id, f"entity_{gene_id}"),
                "pathway_entity_ids": path_ids,
            }
            for gene_id, path_ids in gene_to_pathways.items()
        }

    # ------------------------------------------------------------------
    # Step 7c — Count unique variants per gene from variant_masters
    # ------------------------------------------------------------------

    def _count_variants_per_gene(
        self,
        genes: list[dict[str, Any]],
    ) -> dict[int, int]:
        """
        Count unique variants (distinct position_start + reference_allele,
        regardless of alternate allele) that fall within each gene's
        chromosomal range [start_pos, end_pos].

        Strategy: group genes by chromosome → one query per chromosome over
        the combined [min_start, max_end] range → Python-side count per gene.
        This avoids N individual queries for N genes.

        genes: list of dicts with keys entity_id, chromosome, start_pos, end_pos.
        Returns dict: entity_id → variant_count.
        """
        from sqlalchemy import MetaData, Table

        # Load variant_masters table reference once
        metadata = MetaData()
        try:
            vm = Table("variant_masters", metadata, autoload_with=self.db.engine)
        except Exception:
            self.logger.log("variant_masters not found — skipping variant counts", "WARNING")
            return {}

        # Filter out genes without location data
        located = [
            g for g in genes
            if g.get("chromosome") is not None
            and g.get("start_pos") is not None
            and g.get("end_pos") is not None
        ]
        if not located:
            return {}

        # Group genes by chromosome
        by_chrom: dict[int, list[dict[str, Any]]] = {}
        for g in located:
            by_chrom.setdefault(int(g["chromosome"]), []).append(g)

        counts: dict[int, int] = {}

        for chrom, chrom_genes in by_chrom.items():
            # Widen to the full range covering all genes on this chromosome
            min_start = min(g["start_pos"] for g in chrom_genes)
            max_end = max(g["end_pos"] for g in chrom_genes)

            # One query: distinct (position_start, reference_allele) within range
            # Partition filter on chromosome is applied first — fast on both
            # partitioned Postgres tables and indexed SQLite tables.
            stmt = (
                select(
                    vm.c.position_start,
                    vm.c.reference_allele,
                )
                .where(
                    and_(
                        vm.c.chromosome == chrom,
                        vm.c.position_start >= min_start,
                        vm.c.position_start <= max_end,
                    )
                )
                .distinct()
            )

            rows = self.session.execute(stmt).fetchall()

            # Build a sorted list of unique positions for fast range counting
            # Each entry: (position_start,) — reference_allele already deduped
            # by the DISTINCT. We only need position_start for the gene range check.
            unique_positions = sorted({int(r[0]) for r in rows})

            # For each gene on this chromosome, count positions in [start, end]
            import bisect
            for g in chrom_genes:
                g_start = int(g["start_pos"])
                g_end = int(g["end_pos"])
                lo = bisect.bisect_left(unique_positions, g_start)
                hi = bisect.bisect_right(unique_positions, g_end)
                counts[int(g["entity_id"])] = hi - lo

        return counts

    # ------------------------------------------------------------------
    # Step 8 — Build output DataFrame
    # ------------------------------------------------------------------

    def _build_output(
        self,
        seed_info: dict[str, Any],
        seed_gene: dict[str, Any],
        pathways: list[dict[str, Any]],
        partner_genes: dict[int, dict[str, Any]],
        pathway_entity_ids: set[int],
        variant_counts: dict[int, int],
    ) -> pd.DataFrame:
        pathway_meta: dict[int, dict[str, Any]] = {
            p["pathway_entity_id"]: p for p in pathways
        }

        rows = []
        for gene_id, partner in partner_genes.items():
            shared_ids = partner["pathway_entity_ids"] & pathway_entity_ids
            shared_names = sorted(
                pathway_meta[pid]["pathway_name"]
                for pid in shared_ids
                if pid in pathway_meta
            )
            shared_sources = sorted({
                pathway_meta[pid]["data_source_name"]
                for pid in shared_ids
                if pid in pathway_meta and pathway_meta[pid]["data_source_name"]
            })

            rows.append({
                # --- Seed variant ---
                "seed_input": seed_info["input_raw"],
                "seed_rsid": seed_info.get("rsid"),
                "seed_chromosome": seed_info["chromosome"],
                "seed_position": seed_info["position"],
                "seed_allele_count": seed_info["allele_count"],
                # --- Seed gene ---
                "seed_gene_entity_id": seed_gene["entity_id"],
                "seed_gene_symbol": seed_gene["gene_symbol"],
                "seed_gene_chromosome": seed_gene.get("chromosome"),
                "seed_gene_start": seed_gene.get("start_pos"),
                "seed_gene_end": seed_gene.get("end_pos"),
                "seed_gene_locus_group": seed_gene.get("locus_group"),
                "seed_gene_groups": seed_gene.get("gene_groups"),
                "seed_gene_total_pathways": len(pathway_entity_ids),
                # --- Partner gene ---
                "partner_gene_entity_id": gene_id,
                "partner_gene_symbol": partner["gene_symbol"],
                "partner_gene_chromosome": partner.get("partner_chromosome"),
                "partner_gene_start": partner.get("partner_start_pos"),
                "partner_gene_end": partner.get("partner_end_pos"),
                "partner_gene_locus_group": partner.get("locus_group"),
                "partner_gene_groups": partner.get("gene_groups"),
                # --- Variant counts ---
                "seed_gene_variant_count": variant_counts.get(seed_gene["entity_id"]),
                "partner_gene_variant_count": variant_counts.get(gene_id),
                # --- Shared pathways ---
                "shared_pathway_count": len(shared_ids),
                "shared_pathway_ids": "|".join(str(pid) for pid in sorted(shared_ids)),
                "shared_pathway_names": "|".join(shared_names),
                "shared_pathway_sources": "|".join(shared_sources),
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df.sort_values(
            ["shared_pathway_count", "partner_gene_symbol"],
            ascending=[False, True],
            inplace=True,
        )
        df.reset_index(drop=True, inplace=True)
        return df
