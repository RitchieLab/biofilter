from __future__ import annotations

import bisect
import re
from typing import Any

import pandas as pd
from sqlalchemy import MetaData, Table, and_, func, or_, select
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    ETLDataSource,
    ETLSourceSystem,
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
# Module-level helpers
# ---------------------------------------------------------------------------


def _norm_str(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_chr_to_int(raw: Any) -> int | None:
    """Normalise a chromosome token to an integer (23=X, 24=Y, 25=MT)."""
    s = _norm_str(raw).lower()
    for prefix in ("chromosome", "chrom", "chr"):
        s = s.replace(prefix, "")
    s = s.strip()
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
    Parse a user-supplied variant string.

    Accepted formats:
      - rsID  : rs429358
      - chr:pos variants: chr19:44904604 / 19:44904604 / chr19-44904604 etc.

    Returns a dict with keys: type ('position'|'rsid'|None), chromosome,
    position, rsid, raw.
    """
    s = _norm_str(raw)
    result: dict[str, Any] = {
        "raw": s,
        "type": None,
        "chromosome": None,
        "position": None,
        "rsid": None,
    }
    if not s:
        return result

    # rsID
    if re.match(r"^rs\d+$", s, re.IGNORECASE):
        result["type"] = "rsid"
        result["rsid"] = s.lower()
        return result

    # chr:pos  (separators: : ; , - space)
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


def _empty_output_row(seed_input: str, status: str) -> pd.DataFrame:
    """
    Return a single-row DataFrame that signals a resolution failure.
    All gene/partner columns are None so downstream code can still
    read the file without key errors.
    """
    return pd.DataFrame(
        [
            {
                "resolution_status": status,
                "seed_input": seed_input,
                "seed_rsid": None,
                "seed_chromosome": None,
                "seed_position": None,
                "seed_allele_count": None,
                "group_entity_type": None,
                "seed_gene_entity_id": None,
                "seed_gene_symbol": None,
                "seed_gene_chromosome": None,
                "seed_gene_start": None,
                "seed_gene_end": None,
                "seed_gene_locus_group": None,
                "seed_gene_groups": None,
                "seed_gene_total_groups": None,
                "partner_gene_entity_id": None,
                "partner_gene_symbol": None,
                "partner_gene_chromosome": None,
                "partner_gene_start": None,
                "partner_gene_end": None,
                "partner_gene_locus_group": None,
                "partner_gene_groups": None,
                "seed_gene_variant_count": None,
                "partner_gene_variant_count": None,
                "shared_group_count": None,
                "shared_group_ids": None,
                "shared_group_names": None,
                "shared_group_sources": None,
            }
        ]
    )


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


class VariantSingleGeneAnnotationReport(ReportBase):
    """
    Phase 1 of the single-variant SNP×SNP interaction pipeline.

    Given one input variant (chr:pos or rsID), the report:
      1. Resolves the variant position (via variant_masters when rsID is given).
      2. Finds the closest seed gene at that position using entity_locations
         (with an optional base-pair window).
      3. Expands through the requested biological group type (e.g. Pathways,
         Diseases, GO, or direct Gene links) to collect partner genes.
      4. Enriches every partner gene with genomic coordinates, locus group,
         functional gene groups, and a rough variant count.

    Output: one row per (seed gene × partner gene) pair, analogous to
    one_variant_annotation but with generic 'group' terminology so any
    group_entity_type can be used without changing the output schema.

    Resolution failures (rsID not found, no gene at position, etc.) return a
    single-row DataFrame with a non-None `resolution_status` field so the
    caller always receives a usable object.
    """

    name = "variant_single_gene_annotation"
    description = (
        "Phase 1 of the single-variant interaction pipeline. "
        "Resolves a variant (chr:pos or rsID) to its seed gene, then expands "
        "through a configurable biological group type to build a partner-gene "
        "list annotated with shared group information and variant counts."
    )

    # batch size for IN-list queries
    _BATCH = 500

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @classmethod
    def example_input(cls) -> dict:
        return {
            "input_variant": "chr19:44904604",
            "build": 38,
            "window_bp": 0,
            "group_entity_type": "Pathways",
            "source_system_filter": None,
        }

    @classmethod
    def explain(cls) -> str:
        return (
            "Parameters\n"
            "----------\n"
            "  input_variant       (required) chr:pos (e.g. chr19:44904604) or rsID "
            "(e.g. rs429358)\n"
            "  build               (optional, default 38) genome build used to look "
            "up entity_locations\n"
            "  window_bp           (optional, default 0) base-pair window around the "
            "position for gene lookup; applies only to chr:pos input. "
            "When multiple genes fall inside the window the closest one is used.\n"
            "  group_entity_type   (optional, default 'Pathways') EntityGroup name "
            "used for the expansion step. Examples: 'Pathways', 'Diseases', 'GO', "
            "'Genes'. Use 'Genes' for direct gene-gene links (1-hop).\n"
            "  source_system_filter (optional) list of ETLSourceSystem names to "
            "restrict which relationships are considered. "
            "Example: ['Reactome', 'KEGG']\n"
        )

    def run(self) -> pd.DataFrame:
        # ── 1. Read parameters ─────────────────────────────────────────────
        # run_example() passes example_input() as input_data=<dict>; unpack it
        # so the individual keys become available via self.param().
        _id = self.param("input_data", default=None)
        if isinstance(_id, dict):
            for k, v in _id.items():
                self.params.setdefault(k, v)

        input_raw = self.param("input_variant", default=None)
        if not input_raw:
            raise ValueError("Missing required parameter: 'input_variant'")
        build = int(self.param("build", default=38) or 38)
        window_bp = int(self.param("window_bp", default=0) or 0)
        group_entity_type = (
            _norm_str(self.param("group_entity_type", default="Pathways")) or "Pathways"
        )
        source_system_raw = self.param("source_system_filter", default=None)

        source_system_filter: list[str] = []
        if isinstance(source_system_raw, (list, tuple, set)):
            source_system_filter = [_norm_str(s) for s in source_system_raw if s]
        elif isinstance(source_system_raw, str) and source_system_raw.strip():
            source_system_filter = [source_system_raw.strip()]

        self.logger.log(
            f"input_variant={input_raw!r}  build={build}  window_bp={window_bp}  "
            f"group_entity_type={group_entity_type!r}  "
            f"source_system_filter={source_system_filter or 'all'}"
        )

        # ── 2. Parse input ─────────────────────────────────────────────────
        parsed = _parse_input_variant(input_raw)
        if parsed["type"] is None:
            self.logger.log(f"Cannot parse input variant: {input_raw!r}", "ERROR")
            return _empty_output_row(input_raw, "invalid_input_format")

        # ── 3. Resolve position (rsID path needs variant_masters lookup) ───
        if parsed["type"] == "rsid":
            seed_info = self._resolve_rsid(parsed["rsid"])
            if seed_info is None:
                self.logger.log(
                    f"rsID not found in variant_masters: {input_raw!r}", "WARNING"
                )
                return _empty_output_row(input_raw, "rsid_not_found")
        else:
            seed_info = {
                "chromosome": parsed["chromosome"],
                "position": parsed["position"],
                "allele_count": None,
                "rsid": None,
                "input_raw": parsed["raw"],
            }

        chrom: int = seed_info["chromosome"]
        pos: int = seed_info["position"]
        self.logger.log(
            f"Resolved position → chr{chrom}:{pos}  rsid={seed_info['rsid']}"
        )

        # ── 4. Load assembly map ───────────────────────────────────────────
        assembly_map = self.resolve_assembly_map(str(build))
        assembly_ids = list(assembly_map.values())

        # ── 5. Resolve EntityGroup references ─────────────────────────────
        gene_group = self._get_entity_group("Genes")
        if gene_group is None:
            self.logger.log("EntityGroup 'Genes' not found in database", "ERROR")
            return _empty_output_row(input_raw, "configuration_error")

        is_direct = group_entity_type.strip().lower() == "genes"

        if not is_direct:
            target_group = self._get_entity_group(group_entity_type)
            if target_group is None:
                available = self._list_entity_groups()
                self.logger.log(
                    f"EntityGroup {group_entity_type!r} not found. "
                    f"Available groups: {available}",
                    "ERROR",
                )
                return _empty_output_row(
                    input_raw, f"group_not_found:{group_entity_type}"
                )
        else:
            target_group = gene_group  # unused in direct mode but keeps typing clean

        # ── 6. Resolve source system filter → data_source_ids ─────────────
        data_source_ids: set[int] = set()
        if source_system_filter:
            data_source_ids = self._resolve_data_source_ids(source_system_filter)
            if not data_source_ids:
                self.logger.log(
                    f"No data sources found for source systems: {source_system_filter}. "
                    "Proceeding without source filter.",
                    "WARNING",
                )

        # ── 7. Find seed gene at position ──────────────────────────────────
        seed_gene = self._find_gene_at_position(
            chrom, pos, window_bp, gene_group, assembly_ids
        )
        if seed_gene is None:
            self.logger.log(
                f"No gene found at chr{chrom}:{pos} (window_bp={window_bp}, build={build})",
                "WARNING",
            )
            return _empty_output_row(input_raw, "gene_not_found")

        self.logger.log(
            f"Seed gene → {seed_gene['gene_symbol']} "
            f"(entity_id={seed_gene['entity_id']}, "
            f"chr{seed_gene['chromosome']}:{seed_gene['start_pos']}-{seed_gene['end_pos']})"
        )

        # ── 8. Enrich seed gene with metadata ──────────────────────────────
        seed_meta = self._get_gene_metadata_batch([seed_gene["entity_id"]])
        seed_gene.update(seed_meta.get(seed_gene["entity_id"], {}))

        # ── 9. Expand to partner genes ─────────────────────────────────────
        if is_direct:
            partner_genes, group_meta = self._expand_direct_genes(
                seed_gene["entity_id"], gene_group, data_source_ids
            )
        else:
            partner_genes, group_meta = self._expand_via_intermediary(
                seed_gene["entity_id"], target_group, gene_group, data_source_ids
            )

        if not partner_genes:
            self.logger.log(
                f"No partner genes found for {seed_gene['gene_symbol']} "
                f"via {group_entity_type!r}",
                "WARNING",
            )
            return _empty_output_row(input_raw, "no_partners_found")

        self.logger.log(f"Partner genes found: {len(partner_genes)}")

        # ── 10. Enrich partner genes with locations and metadata ───────────
        partner_ids = list(partner_genes.keys())

        locations = self._get_gene_locations_batch(
            partner_ids, gene_group, assembly_ids
        )
        meta = self._get_gene_metadata_batch(partner_ids)

        for eid, partner in partner_genes.items():
            partner.update(locations.get(eid, {}))
            partner.update(meta.get(eid, {}))

        # ── 11. Count variants per gene ────────────────────────────────────
        seed_loc = {
            "entity_id": seed_gene["entity_id"],
            "chromosome": seed_gene.get("chromosome"),
            "start_pos": seed_gene.get("start_pos"),
            "end_pos": seed_gene.get("end_pos"),
        }
        partner_locs = [
            {
                "entity_id": eid,
                "chromosome": p.get("partner_chromosome"),
                "start_pos": p.get("partner_start_pos"),
                "end_pos": p.get("partner_end_pos"),
            }
            for eid, p in partner_genes.items()
        ]
        variant_counts = self._count_variants_per_gene([seed_loc] + partner_locs)
        self.logger.log(f"Variant counts resolved for {len(variant_counts)} gene(s)")

        # ── 12. Build output ───────────────────────────────────────────────
        return self._build_output(
            seed_info=seed_info,
            seed_gene=seed_gene,
            partner_genes=partner_genes,
            group_meta=group_meta,
            group_entity_type=group_entity_type,
            variant_counts=variant_counts,
        )

    # ------------------------------------------------------------------
    # Step 3 — rsID resolution
    # ------------------------------------------------------------------

    def _resolve_rsid(self, rsid: str) -> dict[str, Any] | None:
        """
        Look up an rsID in variant_masters and return the canonical position.
        If the rsID maps to multiple chromosomes (edge case), we pick the first.
        """
        metadata = MetaData()
        try:
            vm = Table("variant_masters", metadata, autoload_with=self.db.engine)
        except Exception:
            self.logger.log("variant_masters table not available", "ERROR")
            return None

        stmt = (
            select(
                vm.c.chromosome,
                vm.c.position_start,
                func.count().label("allele_count"),
            )
            .where(func.lower(vm.c.rsid) == rsid.lower())
            .group_by(vm.c.chromosome, vm.c.position_start)
            .order_by(vm.c.chromosome, vm.c.position_start)
        )
        rows = self.session.execute(stmt).mappings().all()
        if not rows:
            return None

        row = rows[0]
        return {
            "chromosome": int(row["chromosome"]),
            "position": int(row["position_start"]),
            "allele_count": int(row["allele_count"]),
            "rsid": rsid,
            "input_raw": rsid,
        }

    # ------------------------------------------------------------------
    # Step 5 — EntityGroup helpers
    # ------------------------------------------------------------------

    def _get_entity_group(self, name: str) -> EntityGroup | None:
        return (
            self.session.query(EntityGroup)
            .filter(func.lower(EntityGroup.name) == name.strip().lower())
            .first()
        )

    def _list_entity_groups(self) -> list[str]:
        rows = self.session.query(EntityGroup.name).all()
        return sorted(r.name for r in rows)

    # ------------------------------------------------------------------
    # Step 6 — Source system → data source IDs
    # ------------------------------------------------------------------

    def _resolve_data_source_ids(self, system_names: list[str]) -> set[int]:
        """
        Resolve a list of ETLSourceSystem names to the set of ETLDataSource.id
        values so we can filter entity_relationships.data_source_id.
        """
        names_lower = [s.strip().lower() for s in system_names]

        sys_rows = (
            self.session.query(ETLSourceSystem.id)
            .filter(func.lower(ETLSourceSystem.name).in_(names_lower))
            .all()
        )
        sys_ids = {int(r.id) for r in sys_rows}
        if not sys_ids:
            return set()

        ds_rows = (
            self.session.query(ETLDataSource.id)
            .filter(ETLDataSource.source_system_id.in_(sys_ids))
            .all()
        )
        return {int(r.id) for r in ds_rows}

    # ------------------------------------------------------------------
    # Step 7 — Find seed gene (with window and closest-gene logic)
    # ------------------------------------------------------------------

    def _find_gene_at_position(
        self,
        chromosome: int,
        position: int,
        window_bp: int,
        gene_group: EntityGroup,
        assembly_ids: list[int],
    ) -> dict[str, Any] | None:
        """
        Return the gene that best covers (or is closest to) the given position.

        With window_bp = 0: only genes whose locus contains the position
        (start_pos <= position <= end_pos).

        With window_bp > 0: extend the search to genes within window_bp
        of the position. Among all candidates:
          - genes that contain the position have distance 0 (preferred)
          - genes upstream have distance = gene_start - position
          - genes downstream have distance = position - gene_end
        The gene with the smallest distance is returned; ties broken by
        smallest locus span (most specific gene).
        """
        primary_alias = aliased(EntityAlias)

        search_start = position - window_bp
        search_end = position + window_bp

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
                EntityLocation.start_pos <= search_end,
                EntityLocation.end_pos >= search_start,
            )
        )
        if assembly_ids:
            q = q.filter(EntityLocation.assembly_id.in_(assembly_ids))

        rows = q.all()
        if not rows:
            return None

        def _distance(row) -> int:
            """Distance from position to gene; 0 if position is inside the gene."""
            if row.start_pos <= position <= row.end_pos:
                return 0
            if position < row.start_pos:
                return row.start_pos - position
            return position - row.end_pos

        # Primary sort: distance ascending; secondary: locus span ascending
        best = min(rows, key=lambda r: (_distance(r), r.end_pos - r.start_pos))
        return {
            "entity_id": int(best.entity_id),
            "gene_symbol": _norm_str(best.gene_symbol) or f"entity_{best.entity_id}",
            "chromosome": int(best.chromosome),
            "start_pos": int(best.start_pos),
            "end_pos": int(best.end_pos),
        }

    # ------------------------------------------------------------------
    # Step 9a — 2-hop expansion via intermediary entities
    # ------------------------------------------------------------------

    def _expand_via_intermediary(
        self,
        seed_entity_id: int,
        target_group: EntityGroup,
        gene_group: EntityGroup,
        data_source_ids: set[int],
    ) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
        """
        Two-hop expansion:
          seed gene → [intermediary entities of target_group] → partner genes

        Returns:
          partner_genes : entity_id → {entity_id, gene_symbol,
                                       intermediary_entity_ids (set)}
          group_meta    : intermediary_entity_id → {name, data_source_name}
        """
        # ── hop 1: seed gene → intermediary entities ──────────────────────
        q1 = self.session.query(
            EntityRelationship.entity_1_id,
            EntityRelationship.entity_2_id,
            EntityRelationship.data_source_id,
        ).filter(
            or_(
                and_(
                    EntityRelationship.entity_1_id == seed_entity_id,
                    EntityRelationship.entity_2_group_id == target_group.id,
                ),
                and_(
                    EntityRelationship.entity_2_id == seed_entity_id,
                    EntityRelationship.entity_1_group_id == target_group.id,
                ),
            )
        )
        if data_source_ids:
            q1 = q1.filter(EntityRelationship.data_source_id.in_(data_source_ids))

        hop1_rows = q1.all()
        if not hop1_rows:
            return {}, {}

        # Collect unique intermediary entity IDs + data_source_ids
        intermediary_ids: set[int] = set()
        ds_ids_seen: set[int] = set()
        interm_to_ds: dict[int, int] = {}

        for row in hop1_rows:
            e1, e2 = int(row.entity_1_id), int(row.entity_2_id)
            interm_id = e2 if e1 == seed_entity_id else e1
            intermediary_ids.add(interm_id)
            if row.data_source_id:
                ds_ids_seen.add(int(row.data_source_id))
                interm_to_ds[interm_id] = int(row.data_source_id)

        # ── resolve intermediary names ──────────────────────────────────────
        interm_name_map = self._resolve_primary_names(list(intermediary_ids))

        # ── resolve data source names ───────────────────────────────────────
        ds_name_map = self._resolve_data_source_names(ds_ids_seen)

        group_meta: dict[int, dict[str, Any]] = {
            iid: {
                "name": interm_name_map.get(iid, f"entity_{iid}"),
                "data_source_name": ds_name_map.get(interm_to_ds.get(iid, 0), ""),
            }
            for iid in intermediary_ids
        }

        # ── hop 2: intermediary entities → partner genes ───────────────────
        interm_list = list(intermediary_ids)
        gene_to_intermediaries: dict[int, set[int]] = {}

        for i in range(0, len(interm_list), self._BATCH):
            batch = interm_list[i : i + self._BATCH]
            q2 = self.session.query(
                EntityRelationship.entity_1_id,
                EntityRelationship.entity_1_group_id,
                EntityRelationship.entity_2_id,
                EntityRelationship.entity_2_group_id,
            ).filter(
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
            if data_source_ids:
                q2 = q2.filter(EntityRelationship.data_source_id.in_(data_source_ids))

            for row in q2.all():
                e1, e2 = int(row.entity_1_id), int(row.entity_2_id)
                if row.entity_2_group_id == gene_group.id:
                    gene_id, interm_id = e2, e1
                else:
                    gene_id, interm_id = e1, e2

                if gene_id == seed_entity_id:
                    continue
                gene_to_intermediaries.setdefault(gene_id, set()).add(interm_id)

        if not gene_to_intermediaries:
            return {}, group_meta

        # ── resolve partner gene symbols ───────────────────────────────────
        gene_symbols = self._resolve_primary_names(list(gene_to_intermediaries.keys()))

        partner_genes: dict[int, dict[str, Any]] = {
            gene_id: {
                "entity_id": gene_id,
                "gene_symbol": gene_symbols.get(gene_id, f"entity_{gene_id}"),
                "intermediary_entity_ids": interm_ids,
            }
            for gene_id, interm_ids in gene_to_intermediaries.items()
        }
        return partner_genes, group_meta

    # ------------------------------------------------------------------
    # Step 9b — 1-hop direct gene expansion
    # ------------------------------------------------------------------

    def _expand_direct_genes(
        self,
        seed_entity_id: int,
        gene_group: EntityGroup,
        data_source_ids: set[int],
    ) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
        """
        One-hop expansion:
          seed gene → [entity_relationship where both sides are Genes] → partner genes

        The 'group_meta' dict maps a synthetic key (data_source_id) to its name,
        since there is no intermediary entity — the relationship itself is the link.

        Returns:
          partner_genes : entity_id → {entity_id, gene_symbol,
                                       intermediary_entity_ids (empty set),
                                       direct_link_sources (list[str])}
          group_meta    : {} (no intermediary entities in direct mode)
        """
        q = self.session.query(
            EntityRelationship.entity_1_id,
            EntityRelationship.entity_2_id,
            EntityRelationship.data_source_id,
        ).filter(
            or_(
                and_(
                    EntityRelationship.entity_1_id == seed_entity_id,
                    EntityRelationship.entity_2_group_id == gene_group.id,
                ),
                and_(
                    EntityRelationship.entity_2_id == seed_entity_id,
                    EntityRelationship.entity_1_group_id == gene_group.id,
                ),
            )
        )
        if data_source_ids:
            q = q.filter(EntityRelationship.data_source_id.in_(data_source_ids))

        rows = q.all()
        if not rows:
            return {}, {}

        # Aggregate partner_id → set of data_source_ids
        partner_to_ds: dict[int, set[int]] = {}
        ds_ids_seen: set[int] = set()

        for row in rows:
            e1, e2 = int(row.entity_1_id), int(row.entity_2_id)
            partner_id = e2 if e1 == seed_entity_id else e1
            if partner_id == seed_entity_id:
                continue
            ds_id = int(row.data_source_id) if row.data_source_id else 0
            partner_to_ds.setdefault(partner_id, set()).add(ds_id)
            if ds_id:
                ds_ids_seen.add(ds_id)

        if not partner_to_ds:
            return {}, {}

        ds_name_map = self._resolve_data_source_names(ds_ids_seen)
        gene_symbols = self._resolve_primary_names(list(partner_to_ds.keys()))

        partner_genes: dict[int, dict[str, Any]] = {
            partner_id: {
                "entity_id": partner_id,
                "gene_symbol": gene_symbols.get(partner_id, f"entity_{partner_id}"),
                "intermediary_entity_ids": set(),  # no intermediary in direct mode
                "direct_link_sources": sorted(
                    ds_name_map.get(ds_id, "") for ds_id in ds_ids if ds_id
                ),
            }
            for partner_id, ds_ids in partner_to_ds.items()
        }
        return partner_genes, {}

    # ------------------------------------------------------------------
    # Step 10a — Gene metadata (locus_group, gene_groups)
    # ------------------------------------------------------------------

    def _get_gene_metadata_batch(
        self, entity_ids: list[int]
    ) -> dict[int, dict[str, Any]]:
        """Return {entity_id: {locus_group, gene_groups}} for a batch of genes."""
        if not entity_ids:
            return {}

        result: dict[int, dict[str, Any]] = {}

        for i in range(0, len(entity_ids), self._BATCH):
            batch = entity_ids[i : i + self._BATCH]

            locus_rows = (
                self.session.query(
                    GeneMaster.entity_id,
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

            group_rows = (
                self.session.query(
                    GeneMaster.entity_id,
                    GeneGroup.name.label("group_name"),
                )
                .join(GeneGroupMembership, GeneGroupMembership.gene_id == GeneMaster.id)
                .join(GeneGroup, GeneGroup.id == GeneGroupMembership.group_id)
                .filter(GeneMaster.entity_id.in_(batch))
                .all()
            )
            entity_groups: dict[int, list[str]] = {}
            for row in group_rows:
                eid = int(row.entity_id)
                result.setdefault(eid, {"locus_group": None, "gene_groups": None})
                entity_groups.setdefault(eid, []).append(_norm_str(row.group_name))

            for eid, names in entity_groups.items():
                result[eid]["gene_groups"] = "|".join(sorted(set(names))) or None

        return result

    # ------------------------------------------------------------------
    # Step 10b — Gene genomic locations batch
    # ------------------------------------------------------------------

    def _get_gene_locations_batch(
        self,
        entity_ids: list[int],
        gene_group: EntityGroup,
        assembly_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        if not entity_ids:
            return {}

        result: dict[int, dict[str, Any]] = {}

        for i in range(0, len(entity_ids), self._BATCH):
            batch = entity_ids[i : i + self._BATCH]
            q = self.session.query(
                EntityLocation.entity_id,
                EntityLocation.chromosome,
                EntityLocation.start_pos,
                EntityLocation.end_pos,
            ).filter(
                EntityLocation.entity_id.in_(batch),
                EntityLocation.entity_group_id == gene_group.id,
            )
            if assembly_ids:
                q = q.filter(EntityLocation.assembly_id.in_(assembly_ids))

            for row in q.all():
                eid = int(row.entity_id)
                if eid not in result:
                    result[eid] = {
                        "partner_chromosome": int(row.chromosome),
                        "partner_start_pos": int(row.start_pos),
                        "partner_end_pos": int(row.end_pos),
                    }

        return result

    # ------------------------------------------------------------------
    # Step 11 — Variant counts per gene
    # ------------------------------------------------------------------

    def _count_variants_per_gene(self, genes: list[dict[str, Any]]) -> dict[int, int]:
        """
        Count distinct genomic positions (position_start) in variant_masters
        that fall within each gene's chromosomal range.

        Strategy: group genes by chromosome → one wide-range query per
        chromosome → Python-side bisect count per gene.
        This avoids N individual DB round-trips for N genes.
        """
        metadata = MetaData()
        try:
            vm = Table("variant_masters", metadata, autoload_with=self.db.engine)
        except Exception:
            self.logger.log(
                "variant_masters not accessible — skipping variant counts", "WARNING"
            )
            return {}

        located = [
            g
            for g in genes
            if g.get("chromosome") is not None
            and g.get("start_pos") is not None
            and g.get("end_pos") is not None
        ]
        if not located:
            return {}

        by_chrom: dict[int, list[dict[str, Any]]] = {}
        for g in located:
            by_chrom.setdefault(int(g["chromosome"]), []).append(g)

        counts: dict[int, int] = {}

        for chrom, chrom_genes in by_chrom.items():
            min_start = min(g["start_pos"] for g in chrom_genes)
            max_end = max(g["end_pos"] for g in chrom_genes)

            stmt = (
                select(vm.c.position_start)
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
            unique_positions = sorted(int(r[0]) for r in rows)

            for g in chrom_genes:
                lo = bisect.bisect_left(unique_positions, int(g["start_pos"]))
                hi = bisect.bisect_right(unique_positions, int(g["end_pos"]))
                counts[int(g["entity_id"])] = hi - lo

        return counts

    # ------------------------------------------------------------------
    # Step 12 — Build output DataFrame
    # ------------------------------------------------------------------

    def _build_output(
        self,
        seed_info: dict[str, Any],
        seed_gene: dict[str, Any],
        partner_genes: dict[int, dict[str, Any]],
        group_meta: dict[int, dict[str, Any]],
        group_entity_type: str,
        variant_counts: dict[int, int],
    ) -> pd.DataFrame:
        seed_eid = seed_gene["entity_id"]
        total_groups = len(group_meta) if group_meta else 0

        rows = []
        for gene_id, partner in partner_genes.items():
            interm_ids = partner.get("intermediary_entity_ids", set())

            if interm_ids and group_meta:
                # 2-hop mode — shared intermediary entities
                shared_names = sorted(
                    group_meta[iid]["name"] for iid in interm_ids if iid in group_meta
                )
                shared_sources = sorted(
                    {
                        group_meta[iid]["data_source_name"]
                        for iid in interm_ids
                        if iid in group_meta and group_meta[iid]["data_source_name"]
                    }
                )
                shared_ids_str = "|".join(str(iid) for iid in sorted(interm_ids))
                shared_count = len(interm_ids)
            else:
                # 1-hop mode — link sources instead
                direct_sources = partner.get("direct_link_sources", [])
                shared_names = direct_sources
                shared_sources = direct_sources
                shared_ids_str = ""
                shared_count = len(direct_sources) if direct_sources else 1

            rows.append(
                {
                    "resolution_status": None,
                    # ── Seed variant ──────────────────────────────────────────
                    "seed_input": seed_info["input_raw"],
                    "seed_rsid": seed_info.get("rsid"),
                    "seed_chromosome": seed_info["chromosome"],
                    "seed_position": seed_info["position"],
                    "seed_allele_count": seed_info.get("allele_count"),
                    # ── Group type used ───────────────────────────────────────
                    "group_entity_type": group_entity_type,
                    # ── Seed gene ─────────────────────────────────────────────
                    "seed_gene_entity_id": seed_eid,
                    "seed_gene_symbol": seed_gene["gene_symbol"],
                    "seed_gene_chromosome": seed_gene.get("chromosome"),
                    "seed_gene_start": seed_gene.get("start_pos"),
                    "seed_gene_end": seed_gene.get("end_pos"),
                    "seed_gene_locus_group": seed_gene.get("locus_group"),
                    "seed_gene_groups": seed_gene.get("gene_groups"),
                    "seed_gene_total_groups": total_groups,
                    # ── Partner gene ──────────────────────────────────────────
                    "partner_gene_entity_id": gene_id,
                    "partner_gene_symbol": partner["gene_symbol"],
                    "partner_gene_chromosome": partner.get("partner_chromosome"),
                    "partner_gene_start": partner.get("partner_start_pos"),
                    "partner_gene_end": partner.get("partner_end_pos"),
                    "partner_gene_locus_group": partner.get("locus_group"),
                    "partner_gene_groups": partner.get("gene_groups"),
                    # ── Variant counts ────────────────────────────────────────
                    "seed_gene_variant_count": variant_counts.get(seed_eid),
                    "partner_gene_variant_count": variant_counts.get(gene_id),
                    # ── Shared groups ─────────────────────────────────────────
                    "shared_group_count": shared_count,
                    "shared_group_ids": shared_ids_str,
                    "shared_group_names": "|".join(shared_names),
                    "shared_group_sources": "|".join(shared_sources),
                }
            )

        if not rows:
            return _empty_output_row(seed_info["input_raw"], "no_partners_found")

        df = pd.DataFrame(rows)
        df.sort_values(
            ["shared_group_count", "partner_gene_symbol"],
            ascending=[False, True],
            inplace=True,
        )
        df.reset_index(drop=True, inplace=True)
        return df

    # ------------------------------------------------------------------
    # Shared DB helpers
    # ------------------------------------------------------------------

    def _resolve_primary_names(self, entity_ids: list[int]) -> dict[int, str]:
        """Return {entity_id: primary_alias_value} for a list of entity IDs."""
        if not entity_ids:
            return {}
        result: dict[int, str] = {}
        for i in range(0, len(entity_ids), self._BATCH):
            batch = entity_ids[i : i + self._BATCH]
            rows = (
                self.session.query(EntityAlias.entity_id, EntityAlias.alias_value)
                .filter(
                    EntityAlias.entity_id.in_(batch),
                    EntityAlias.is_primary.is_(True),
                )
                .all()
            )
            for row in rows:
                result[int(row.entity_id)] = _norm_str(row.alias_value)
        return result

    def _resolve_data_source_names(self, ds_ids: set[int]) -> dict[int, str]:
        """Return {data_source_id: name} for a set of ETLDataSource IDs."""
        if not ds_ids:
            return {}
        rows = (
            self.session.query(ETLDataSource.id, ETLDataSource.name)
            .filter(ETLDataSource.id.in_(ds_ids))
            .all()
        )
        return {int(r.id): _norm_str(r.name) for r in rows}
