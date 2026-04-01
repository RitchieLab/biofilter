from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import and_, func
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    EntityAlias,
    EntityGroup,
    EntityLocation,
    EntityRelationship,
    EntityRelationshipType,
    GeneGroup,
    GeneGroupMembership,
    GeneLocusType,
    GeneMaster,
    PathwayMaster,
)
from biofilter.modules.report.reports.base_report import ReportBase


def _norm(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    s = _norm(value).lower()
    if s in {"true", "1", "yes", "y", "on"}:
        return True
    if s in {"false", "0", "no", "n", "off"}:
        return False
    return default


def _parse_chr_to_int(chr_value: Any) -> int | None:
    s = _norm(chr_value).lower()
    if not s:
        return None

    s = s.replace("chromosome", "").replace("chrom", "").replace("chr", "").strip()
    if s == "x":
        return 23
    if s == "y":
        return 24
    if s in {"m", "mt", "mito", "mitochondria"}:
        return 25

    try:
        v = int(s)
        if 1 <= v <= 25:
            return v
        return None
    except Exception:
        return None


def _to_set(value: Any) -> set[str]:
    if value is None:
        return set()

    if isinstance(value, (list, tuple, set)):
        seq = value
    else:
        seq = [value]

    out: set[str] = set()
    for item in seq:
        s = _norm(item)
        if not s:
            continue
        if "," in s:
            out.update({part.strip() for part in s.split(",") if part.strip()})
        else:
            out.add(s)
    return out


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        s = _norm(value)
        if s:
            return s
    return None


class VariantBinningReport(ReportBase):
    name = "variant_binning"
    description = (
        "BioBin-style rare-variant aggregation from a cohort VCF into biological "
        "bins (gene, gene_group, locus_type, pathway), writing output artifacts "
        "to output_dir."
    )

    summary_columns = [
        "report_name",
        "output_dir",
        "group_by",
        "maf_cutoff",
        "variants_processed",
        "variants_rare",
        "variants_with_gene_overlap",
        "variants_binned",
        "bins_generated",
        "samples_selected",
        "artifact_bin_counts",
        "artifact_variant_to_bin",
        "artifact_bin_definitions",
        "artifact_bin_member_counts",
        "artifact_sample_bin_long",
        "artifact_summary_json",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.summary_columns

    @classmethod
    def example_input(cls):
        return {
            "vcf_path": "./cohort.vcf.gz",
            "phenotype_path": "./phenotype.csv",
            "phenotype_sample_column": "SampleID",
            "phenotype_value_column": "Phenotype",
            "phenotype_control_value": 0,
            "maf_cutoff": 0.01,
            "rare_case_control": True,
            "overall_major_allele": True,
            "group_by": "gene",
            "build": 38,
            "output_dir": "./outputs/variant_binning",
        }

    @classmethod
    def explain(cls) -> str:
        return str("DOC IN MD FILE")

    def _resolve_group_ids(self, names: set[str]) -> dict[str, int]:
        if not names:
            return {}

        rows = (
            self.session.query(EntityGroup.id, EntityGroup.name)
            .filter(func.lower(EntityGroup.name).in_([n.lower() for n in names]))
            .all()
        )
        found = {str(name): int(group_id) for group_id, name in rows}

        # Accept partial matches (e.g., default aliases like Gene/Genes, Pathway/Pathways).
        if not found:
            raise ValueError(
                f"Entity groups not found for requested names: {sorted(names)}. "
                "Check seeded EntityGroup names."
            )
        return found

    def _load_phenotype(
        self,
        phenotype_path: str | None,
        sample_column: str,
        value_column: str,
    ) -> tuple[dict[str, str], str | None]:
        if not phenotype_path:
            return {}, None

        path = Path(phenotype_path)
        if not path.exists():
            raise FileNotFoundError(f"phenotype_path not found: {path}")

        read_kwargs: dict[str, Any] = {}
        if path.suffix.lower() in {".tsv", ".tab"}:
            read_kwargs["sep"] = "\t"
        elif path.suffix.lower() in {".csv"}:
            read_kwargs["sep"] = ","
        else:
            read_kwargs["sep"] = None
            read_kwargs["engine"] = "python"

        df = pd.read_csv(path, **read_kwargs)
        if df.empty:
            raise ValueError(f"Phenotype file is empty: {path}")

        col_map = {str(c).strip().lower(): str(c) for c in df.columns}

        sample_col = col_map.get(sample_column.lower())
        value_col = col_map.get(value_column.lower())

        if sample_col is None:
            candidates = ["sampleid", "sample_id", "sample", "iid", "id"]
            for cand in candidates:
                if cand in col_map:
                    sample_col = col_map[cand]
                    break
        if value_col is None:
            candidates = ["phenotype", "pheno", "status", "case_control", "label"]
            for cand in candidates:
                if cand in col_map:
                    value_col = col_map[cand]
                    break

        if sample_col is None or value_col is None:
            raise ValueError(
                "Could not resolve phenotype columns. "
                f"Expected sample='{sample_column}' and value='{value_column}'. "
                f"Available columns: {list(df.columns)}"
            )

        out: dict[str, str] = {}
        for row in df[[sample_col, value_col]].itertuples(index=False):
            sample = _norm(row[0])
            value = _norm(row[1])
            if not sample:
                continue
            out[sample] = value

        if not out:
            raise ValueError(
                "No non-empty sample/value rows found in phenotype file after parsing."
            )

        return out, value_col

    def _load_gene_metadata(self) -> tuple[dict[int, dict[str, Any]], dict[int, list[str]]]:
        """
        Returns:
        - entity_id -> metadata (gene_id, symbol, locus_type)
        - entity_id -> list of gene_group names
        """
        locus_alias = aliased(GeneLocusType)

        gene_meta_rows = (
            self.session.query(
                GeneMaster.entity_id.label("entity_id"),
                GeneMaster.id.label("gene_id"),
                GeneMaster.symbol.label("symbol"),
                locus_alias.name.label("locus_type"),
            )
            .outerjoin(locus_alias, locus_alias.id == GeneMaster.locus_type_id)
            .all()
        )

        meta_by_entity: dict[int, dict[str, Any]] = {}
        for row in gene_meta_rows:
            entity_id = int(row.entity_id)
            meta_by_entity[entity_id] = {
                "gene_id": int(row.gene_id) if row.gene_id is not None else None,
                "symbol": _norm(row.symbol) or None,
                "locus_type": _norm(row.locus_type) or None,
            }

        membership_rows = (
            self.session.query(
                GeneMaster.entity_id.label("entity_id"),
                GeneGroup.name.label("group_name"),
            )
            .join(GeneGroupMembership, GeneGroupMembership.gene_id == GeneMaster.id)
            .join(GeneGroup, GeneGroup.id == GeneGroupMembership.group_id)
            .all()
        )

        groups_by_entity_set: dict[int, set[str]] = defaultdict(set)
        for row in membership_rows:
            groups_by_entity_set[int(row.entity_id)].add(str(row.group_name))

        groups_by_entity = {
            entity_id: sorted(names)
            for entity_id, names in groups_by_entity_set.items()
        }
        return meta_by_entity, groups_by_entity

    def _load_gene_intervals(
        self,
        build: int,
        gene_entity_group_names: set[str],
        window_size: int,
    ) -> tuple[
        dict[int, list[dict[str, Any]]],
        dict[int, dict[int, list[int]]],
        dict[int, dict[str, Any]],
    ]:
        group_ids = self._resolve_group_ids(gene_entity_group_names)
        gene_group_id_values = sorted(group_ids.values())

        meta_by_entity, groups_by_entity = self._load_gene_metadata()

        primary_alias = aliased(EntityAlias)
        rows = (
            self.session.query(
                EntityLocation.entity_id,
                EntityLocation.chromosome,
                EntityLocation.start_pos,
                EntityLocation.end_pos,
                primary_alias.alias_value.label("primary_alias"),
            )
            .outerjoin(
                primary_alias,
                and_(
                    primary_alias.entity_id == EntityLocation.entity_id,
                    primary_alias.is_primary.is_(True),
                ),
            )
            .filter(
                EntityLocation.build == int(build),
                EntityLocation.entity_group_id.in_(gene_group_id_values),
            )
            .all()
        )

        genes_by_chr: dict[int, list[dict[str, Any]]] = defaultdict(list)

        for row in rows:
            chrom = int(row.chromosome)
            entity_id = int(row.entity_id)
            start = int(row.start_pos)
            end = int(row.end_pos)
            if end < start:
                start, end = end, start

            meta = meta_by_entity.get(entity_id, {})
            symbol = _first_non_empty(meta.get("symbol"), row.primary_alias, f"GENE_ENTITY_{entity_id}")
            genes_by_chr[chrom].append(
                {
                    "entity_id": entity_id,
                    "start": start,
                    "end": end,
                    "symbol": symbol,
                    "gene_id": meta.get("gene_id"),
                    "locus_type": meta.get("locus_type"),
                    "gene_groups": groups_by_entity.get(entity_id, []),
                }
            )

        windows_by_chr: dict[int, dict[int, list[int]]] = {}
        for chrom, genes in genes_by_chr.items():
            genes.sort(key=lambda g: (int(g["start"]), int(g["end"]), int(g["entity_id"])))
            window_map: dict[int, list[int]] = defaultdict(list)
            for idx, gene in enumerate(genes):
                window_start = int(gene["start"]) // window_size
                window_end = int(gene["end"]) // window_size
                for window in range(window_start, window_end + 1):
                    window_map[window].append(idx)
            windows_by_chr[chrom] = dict(window_map)

        return dict(genes_by_chr), windows_by_chr, meta_by_entity

    def _find_overlapping_genes(
        self,
        chromosome: int,
        start_pos: int,
        end_pos: int,
        genes_by_chr: dict[int, list[dict[str, Any]]],
        windows_by_chr: dict[int, dict[int, list[int]]],
        window_size: int,
    ) -> list[dict[str, Any]]:
        genes = genes_by_chr.get(chromosome)
        if not genes:
            return []

        window_map = windows_by_chr.get(chromosome, {})
        w_start = start_pos // window_size
        w_end = end_pos // window_size

        candidate_indexes: set[int] = set()
        for window in range(w_start, w_end + 1):
            candidate_indexes.update(window_map.get(window, []))

        if not candidate_indexes:
            return []

        overlaps: list[dict[str, Any]] = []
        for idx in sorted(candidate_indexes):
            gene = genes[idx]
            if int(gene["end"]) < start_pos:
                continue
            if int(gene["start"]) > end_pos:
                continue
            overlaps.append(gene)
        return overlaps

    def _build_pathway_mapping(
        self,
        gene_entity_ids: set[int],
        pathway_entity_group_names: set[str],
        relationship_types: set[str],
    ) -> tuple[dict[int, list[str]], dict[str, dict[str, Any]]]:
        if not gene_entity_ids:
            return {}, {}

        pathway_group_ids = self._resolve_group_ids(pathway_entity_group_names)
        pathway_group_id_values = sorted(pathway_group_ids.values())

        rel_type_rows = (
            self.session.query(EntityRelationshipType.id, EntityRelationshipType.code)
            .filter(func.lower(EntityRelationshipType.code).in_([x.lower() for x in relationship_types]))
            .all()
        )
        rel_type_ids = [int(row.id) for row in rel_type_rows]
        if not rel_type_ids:
            raise ValueError(
                "No matching relationship types found for pathway mapping. "
                f"Requested: {sorted(relationship_types)}"
            )

        q_gene_to_pathway = (
            self.session.query(
                EntityRelationship.entity_1_id.label("gene_entity_id"),
                EntityRelationship.entity_2_id.label("pathway_entity_id"),
            )
            .filter(
                EntityRelationship.entity_1_id.in_(list(gene_entity_ids)),
                EntityRelationship.entity_2_group_id.in_(pathway_group_id_values),
                EntityRelationship.relationship_type_id.in_(rel_type_ids),
            )
            .all()
        )

        q_pathway_to_gene = (
            self.session.query(
                EntityRelationship.entity_2_id.label("gene_entity_id"),
                EntityRelationship.entity_1_id.label("pathway_entity_id"),
            )
            .filter(
                EntityRelationship.entity_2_id.in_(list(gene_entity_ids)),
                EntityRelationship.entity_1_group_id.in_(pathway_group_id_values),
                EntityRelationship.relationship_type_id.in_(rel_type_ids),
            )
            .all()
        )

        gene_to_pathway_ids: dict[int, set[int]] = defaultdict(set)
        pathway_entity_ids: set[int] = set()

        for row in list(q_gene_to_pathway) + list(q_pathway_to_gene):
            gene_entity_id = int(row.gene_entity_id)
            pathway_entity_id = int(row.pathway_entity_id)
            gene_to_pathway_ids[gene_entity_id].add(pathway_entity_id)
            pathway_entity_ids.add(pathway_entity_id)

        if not pathway_entity_ids:
            return {}, {}

        pathway_rows = (
            self.session.query(
                PathwayMaster.entity_id,
                PathwayMaster.pathway_id,
                PathwayMaster.description,
            )
            .filter(PathwayMaster.entity_id.in_(list(pathway_entity_ids)))
            .all()
        )

        pathway_name_map: dict[int, str] = {}
        pathway_meta_by_bin: dict[str, dict[str, Any]] = {}

        for row in pathway_rows:
            entity_id = int(row.entity_id)
            pathway_id = _norm(row.pathway_id)
            description = _norm(row.description)
            bin_name = pathway_id or f"PATHWAY_ENTITY_{entity_id}"
            pathway_name_map[entity_id] = bin_name
            pathway_meta_by_bin[bin_name] = {
                "pathway_entity_id": entity_id,
                "pathway_id": pathway_id or None,
                "pathway_label": description or pathway_id or None,
            }

        missing_ids = sorted(pathway_entity_ids - set(pathway_name_map.keys()))
        if missing_ids:
            alias_rows = (
                self.session.query(EntityAlias.entity_id, EntityAlias.alias_value)
                .filter(
                    EntityAlias.entity_id.in_(missing_ids),
                    EntityAlias.is_primary.is_(True),
                )
                .all()
            )
            alias_map = {int(row.entity_id): _norm(row.alias_value) for row in alias_rows}
            for entity_id in missing_ids:
                alias_value = alias_map.get(entity_id)
                bin_name = alias_value or f"PATHWAY_ENTITY_{entity_id}"
                pathway_name_map[entity_id] = bin_name
                pathway_meta_by_bin[bin_name] = {
                    "pathway_entity_id": entity_id,
                    "pathway_id": None,
                    "pathway_label": alias_value or None,
                }

        gene_to_bins: dict[int, list[str]] = {}
        for gene_entity_id, pathway_ids in gene_to_pathway_ids.items():
            bin_names = sorted(
                {
                    pathway_name_map[pathway_entity_id]
                    for pathway_entity_id in pathway_ids
                    if pathway_entity_id in pathway_name_map
                }
            )
            if bin_names:
                gene_to_bins[gene_entity_id] = bin_names

        return gene_to_bins, pathway_meta_by_bin

    def _resolve_bins_for_gene(
        self,
        group_by: str,
        gene: dict[str, Any],
        pathway_bins_by_gene: dict[int, list[str]],
    ) -> list[dict[str, Any]]:
        entity_id = int(gene["entity_id"])
        symbol = _norm(gene.get("symbol")) or f"GENE_ENTITY_{entity_id}"

        if group_by == "gene":
            return [
                {
                    "bin_name": symbol,
                    "bin_type": "gene",
                    "meta": {
                        "gene_entity_id": entity_id,
                        "gene_symbol": symbol,
                    },
                }
            ]

        if group_by == "gene_group":
            names = [str(x) for x in gene.get("gene_groups", []) if _norm(x)]
            return [
                {
                    "bin_name": name,
                    "bin_type": "gene_group",
                    "meta": {
                        "gene_entity_id": entity_id,
                        "gene_symbol": symbol,
                    },
                }
                for name in sorted(set(names))
            ]

        if group_by == "locus_type":
            locus_type = _norm(gene.get("locus_type"))
            if not locus_type:
                return []
            return [
                {
                    "bin_name": locus_type,
                    "bin_type": "locus_type",
                    "meta": {
                        "gene_entity_id": entity_id,
                        "gene_symbol": symbol,
                    },
                }
            ]

        if group_by == "pathway":
            names = pathway_bins_by_gene.get(entity_id, [])
            return [
                {
                    "bin_name": name,
                    "bin_type": "pathway",
                    "meta": {
                        "gene_entity_id": entity_id,
                        "gene_symbol": symbol,
                    },
                }
                for name in names
            ]

        raise ValueError(
            "group_by must be one of: gene, gene_group, locus_type, pathway"
        )

    def _load_vcf(self, vcf_path: str):
        try:
            from cyvcf2 import VCF
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "cyvcf2 is required to run VariantBinningReport. "
                "Install dependencies including cyvcf2."
            ) from exc

        return VCF(vcf_path)

    def _classify_sample(
        self,
        phenotype_value: str,
        control_values: set[str],
        case_values: set[str],
    ) -> str:
        value = _norm(phenotype_value)
        if value in control_values:
            return "control"
        if case_values:
            return "case" if value in case_values else "unknown"
        return "case"

    def run(self):
        vcf_path = _norm(self.param("vcf_path", required=True))
        output_dir_raw = _norm(self.param("output_dir", required=True))
        phenotype_path = _norm(self.param("phenotype_path")) or None

        group_by = _norm(self.param("group_by", "gene")).lower()
        if group_by not in {"gene", "gene_group", "locus_type", "pathway"}:
            raise ValueError("group_by must be one of: gene, gene_group, locus_type, pathway")

        build = int(self.param("build", 38) or 38)
        maf_cutoff = float(self.param("maf_cutoff", 0.01) or 0.01)

        rare_case_control = _parse_bool(self.param("rare_case_control", True), default=True)
        overall_major_allele = _parse_bool(self.param("overall_major_allele", True), default=True)
        include_zero_counts = _parse_bool(self.param("include_zero_counts", True), default=True)

        max_variants = self.param("max_variants")
        max_variants_int = int(max_variants) if max_variants not in (None, "") else None

        gene_window_size = int(self.param("gene_window_size", 500000) or 500000)

        gene_entity_group_names = _to_set(self.param("gene_entity_groups", ["Gene", "Genes"]))
        if not gene_entity_group_names:
            gene_entity_group_names = {"Genes"}

        pathway_entity_group_names = _to_set(self.param("pathway_entity_groups", ["Pathway", "Pathways"]))
        if not pathway_entity_group_names:
            pathway_entity_group_names = {"Pathways"}

        relationship_types = _to_set(self.param("relationship_types", ["in_pathway"]))
        if not relationship_types:
            relationship_types = {"in_pathway"}

        phenotype_sample_column = _norm(self.param("phenotype_sample_column", "SampleID")) or "SampleID"
        phenotype_value_column = _norm(self.param("phenotype_value_column", "Phenotype")) or "Phenotype"

        control_values = _to_set(self.param("phenotype_control_value", {"0"}))
        if not control_values:
            control_values = {"0"}
        case_values = _to_set(self.param("phenotype_case_values", set()))

        output_dir = Path(output_dir_raw)
        output_dir.mkdir(parents=True, exist_ok=True)

        artifact_bin_counts = output_dir / "bin_counts.csv"
        artifact_variant_to_bin = output_dir / "variant_to_bin.csv"
        artifact_bin_definitions = output_dir / "bin_definitions.csv"
        artifact_bin_member_counts = output_dir / "bin_member_counts.csv"
        artifact_sample_bin_long = output_dir / "sample_bin_long.csv"
        artifact_summary_json = output_dir / "summary.json"

        phenotype_by_sample, resolved_phenotype_column = self._load_phenotype(
            phenotype_path=phenotype_path,
            sample_column=phenotype_sample_column,
            value_column=phenotype_value_column,
        )

        genes_by_chr, windows_by_chr, meta_by_entity = self._load_gene_intervals(
            build=build,
            gene_entity_group_names=gene_entity_group_names,
            window_size=gene_window_size,
        )

        pathway_bins_by_gene: dict[int, list[str]] = {}
        pathway_meta_by_bin: dict[str, dict[str, Any]] = {}
        if group_by == "pathway":
            pathway_bins_by_gene, pathway_meta_by_bin = self._build_pathway_mapping(
                gene_entity_ids=set(meta_by_entity.keys()),
                pathway_entity_group_names=pathway_entity_group_names,
                relationship_types=relationship_types,
            )

        vcf = self._load_vcf(vcf_path)
        vcf_samples = list(vcf.samples)

        if not vcf_samples:
            raise ValueError("VCF has no samples. A multi-sample cohort VCF is required.")

        selected_sample_indexes: list[int] = []
        selected_samples: list[str] = []
        sample_phenotype_value: dict[str, str | None] = {}
        sample_class: dict[str, str] = {}

        if phenotype_by_sample:
            for idx, sample in enumerate(vcf_samples):
                if sample not in phenotype_by_sample:
                    continue
                p_value = phenotype_by_sample[sample]
                s_class = self._classify_sample(
                    phenotype_value=p_value,
                    control_values=control_values,
                    case_values=case_values,
                )
                if s_class == "unknown":
                    continue
                selected_sample_indexes.append(idx)
                selected_samples.append(sample)
                sample_phenotype_value[sample] = p_value
                sample_class[sample] = s_class

            if not selected_samples:
                raise ValueError(
                    "No VCF samples could be matched to phenotype labels (case/control)."
                )
        else:
            selected_sample_indexes = list(range(len(vcf_samples)))
            selected_samples = list(vcf_samples)
            for sample in selected_samples:
                sample_phenotype_value[sample] = None
                sample_class[sample] = "unknown"

        selected_pos_by_class: dict[str, list[int]] = defaultdict(list)
        for pos, sample in enumerate(selected_samples):
            selected_pos_by_class[sample_class[sample]].append(pos)

        case_positions = selected_pos_by_class.get("case", [])
        control_positions = selected_pos_by_class.get("control", [])

        rare_case_control_active = (
            rare_case_control and bool(case_positions) and bool(control_positions)
        )

        variant_to_bin_columns = [
            "variant_key",
            "variant_id_in_vcf",
            "chromosome",
            "position_start",
            "position_end",
            "reference_allele",
            "alternate_allele",
            "group_by",
            "bin_type",
            "bin_name",
            "gene_entity_id",
            "gene_symbol",
            "maf_filter",
            "maf_overall",
            "maf_case",
            "maf_control",
            "af_overall",
            "af_case",
            "af_control",
            "ac_overall",
            "an_overall",
            "ac_case",
            "an_case",
            "ac_control",
            "an_control",
        ]

        sample_bin_alt_counts: dict[tuple[str, str], int] = defaultdict(int)
        sample_bin_variant_counts: dict[tuple[str, str], int] = defaultdict(int)
        bin_variant_keys: dict[str, set[str]] = defaultdict(set)
        bin_meta: dict[str, dict[str, Any]] = {}

        variants_processed = 0
        variants_rare = 0
        variants_with_gene_overlap = 0
        variants_binned = 0

        with artifact_variant_to_bin.open("w", encoding="utf-8", newline="") as f_variant:
            writer = csv.DictWriter(f_variant, fieldnames=variant_to_bin_columns)
            writer.writeheader()

            stop = False
            for record in vcf:
                chromosome = _parse_chr_to_int(record.CHROM)
                if chromosome is None:
                    continue

                ref = _norm(record.REF)
                if not ref:
                    continue

                alts = [str(a) for a in (record.ALT or []) if _norm(a)]
                if not alts:
                    continue

                start_pos = int(record.POS)
                end_pos = start_pos + max(len(ref), 1) - 1

                genotypes = record.genotypes
                if not genotypes:
                    continue

                selected_alleles: list[tuple[int, int]] = []
                called_counts: list[int] = []

                for sample_idx in selected_sample_indexes:
                    call = genotypes[sample_idx] if sample_idx < len(genotypes) else None
                    if not call:
                        a1, a2 = -1, -1
                    else:
                        raw_a1 = call[0] if len(call) >= 1 else -1
                        raw_a2 = call[1] if len(call) >= 2 else -1
                        try:
                            a1 = int(raw_a1) if raw_a1 is not None else -1
                        except Exception:
                            a1 = -1
                        try:
                            a2 = int(raw_a2) if raw_a2 is not None else -1
                        except Exception:
                            a2 = -1
                    selected_alleles.append((a1, a2))
                    called_counts.append((1 if a1 >= 0 else 0) + (1 if a2 >= 0 else 0))

                for alt_idx, alt in enumerate(alts, start=1):
                    variants_processed += 1
                    if max_variants_int is not None and variants_processed > max_variants_int:
                        stop = True
                        break

                    alt_counts = [
                        (1 if a1 == alt_idx else 0) + (1 if a2 == alt_idx else 0)
                        for a1, a2 in selected_alleles
                    ]

                    ac_overall = int(sum(alt_counts))
                    an_overall = int(sum(called_counts))
                    if an_overall <= 0:
                        continue

                    af_overall = ac_overall / an_overall
                    maf_overall = min(af_overall, 1.0 - af_overall)

                    ac_case = an_case = None
                    ac_control = an_control = None
                    af_case = af_control = None
                    maf_case = maf_control = None

                    if case_positions:
                        ac_case = int(sum(alt_counts[p] for p in case_positions))
                        an_case = int(sum(called_counts[p] for p in case_positions))
                        af_case = (ac_case / an_case) if an_case > 0 else None
                        maf_case = min(af_case, 1.0 - af_case) if af_case is not None else None

                    if control_positions:
                        ac_control = int(sum(alt_counts[p] for p in control_positions))
                        an_control = int(sum(called_counts[p] for p in control_positions))
                        af_control = (ac_control / an_control) if an_control > 0 else None
                        maf_control = (
                            min(af_control, 1.0 - af_control)
                            if af_control is not None
                            else None
                        )

                    maf_filter = maf_overall
                    if rare_case_control_active:
                        if maf_case is not None and maf_control is not None:
                            maf_filter = max(maf_case, maf_control)
                    elif not overall_major_allele and maf_control is not None:
                        maf_filter = maf_control

                    is_rare = maf_filter <= maf_cutoff
                    if not is_rare:
                        continue

                    variants_rare += 1

                    overlapping_genes = self._find_overlapping_genes(
                        chromosome=chromosome,
                        start_pos=start_pos,
                        end_pos=end_pos,
                        genes_by_chr=genes_by_chr,
                        windows_by_chr=windows_by_chr,
                        window_size=gene_window_size,
                    )

                    if not overlapping_genes:
                        continue

                    variants_with_gene_overlap += 1

                    variant_key = f"{chromosome}:{start_pos}:{end_pos}:{ref}>{alt}"
                    variant_id_in_vcf = _norm(record.ID) or None

                    unique_bins_this_variant: set[str] = set()
                    any_mapping_written = False

                    for gene in overlapping_genes:
                        gene_entity_id = int(gene["entity_id"])
                        gene_symbol = _norm(gene.get("symbol")) or f"GENE_ENTITY_{gene_entity_id}"

                        bin_specs = self._resolve_bins_for_gene(
                            group_by=group_by,
                            gene=gene,
                            pathway_bins_by_gene=pathway_bins_by_gene,
                        )
                        if not bin_specs:
                            continue

                        for spec in bin_specs:
                            bin_name = str(spec["bin_name"])
                            bin_type = str(spec["bin_type"])
                            meta = dict(spec.get("meta") or {})

                            any_mapping_written = True
                            unique_bins_this_variant.add(bin_name)

                            if group_by == "pathway" and bin_name in pathway_meta_by_bin:
                                meta.update(pathway_meta_by_bin[bin_name])

                            if bin_name not in bin_meta:
                                bin_meta[bin_name] = {
                                    "bin_name": bin_name,
                                    "bin_type": bin_type,
                                    **meta,
                                }

                            writer.writerow(
                                {
                                    "variant_key": variant_key,
                                    "variant_id_in_vcf": variant_id_in_vcf,
                                    "chromosome": chromosome,
                                    "position_start": start_pos,
                                    "position_end": end_pos,
                                    "reference_allele": ref,
                                    "alternate_allele": alt,
                                    "group_by": group_by,
                                    "bin_type": bin_type,
                                    "bin_name": bin_name,
                                    "gene_entity_id": gene_entity_id,
                                    "gene_symbol": gene_symbol,
                                    "maf_filter": maf_filter,
                                    "maf_overall": maf_overall,
                                    "maf_case": maf_case,
                                    "maf_control": maf_control,
                                    "af_overall": af_overall,
                                    "af_case": af_case,
                                    "af_control": af_control,
                                    "ac_overall": ac_overall,
                                    "an_overall": an_overall,
                                    "ac_case": ac_case,
                                    "an_case": an_case,
                                    "ac_control": ac_control,
                                    "an_control": an_control,
                                }
                            )

                    if not any_mapping_written:
                        continue

                    variants_binned += 1

                    positive_sample_positions = [
                        p for p, alt_count in enumerate(alt_counts) if alt_count > 0
                    ]

                    for bin_name in unique_bins_this_variant:
                        bin_variant_keys[bin_name].add(variant_key)

                        for pos in positive_sample_positions:
                            sample = selected_samples[pos]
                            key = (sample, bin_name)
                            sample_bin_alt_counts[key] += int(alt_counts[pos])
                            sample_bin_variant_counts[key] += 1

                if stop:
                    break

        vcf.close()

        bins_sorted = sorted(bin_variant_keys.keys())

        bin_member_rows: list[dict[str, Any]] = []
        for bin_name in bins_sorted:
            meta = dict(bin_meta.get(bin_name, {}))
            bin_member_rows.append(
                {
                    "bin_name": bin_name,
                    "bin_type": meta.get("bin_type") or group_by,
                    "variant_count": len(bin_variant_keys[bin_name]),
                    **{k: v for k, v in meta.items() if k not in {"bin_name", "bin_type"}},
                }
            )

        bin_member_df = pd.DataFrame(bin_member_rows)
        if bin_member_df.empty:
            bin_member_df = pd.DataFrame(
                columns=[
                    "bin_name",
                    "bin_type",
                    "variant_count",
                ]
            )

        bin_member_df.to_csv(artifact_bin_member_counts, index=False)

        bin_def_df = bin_member_df.copy()
        bin_def_df.to_csv(artifact_bin_definitions, index=False)

        sample_long_rows: list[dict[str, Any]] = []
        for sample in selected_samples:
            sample_pheno = sample_phenotype_value.get(sample)
            sample_cls = sample_class.get(sample, "unknown")

            for bin_name in bins_sorted:
                key = (sample, bin_name)
                alt_allele_count = int(sample_bin_alt_counts.get(key, 0))
                variant_count = int(sample_bin_variant_counts.get(key, 0))
                if (not include_zero_counts) and alt_allele_count == 0 and variant_count == 0:
                    continue

                sample_long_rows.append(
                    {
                        "sample_id": sample,
                        "phenotype_value": sample_pheno,
                        "sample_class": sample_cls,
                        "bin_name": bin_name,
                        "group_by": group_by,
                        "variant_count": variant_count,
                        "alt_allele_count": alt_allele_count,
                    }
                )

        sample_long_df = pd.DataFrame(sample_long_rows)
        if sample_long_df.empty:
            sample_long_df = pd.DataFrame(
                columns=[
                    "sample_id",
                    "phenotype_value",
                    "sample_class",
                    "bin_name",
                    "group_by",
                    "variant_count",
                    "alt_allele_count",
                ]
            )

        sample_long_df.to_csv(artifact_sample_bin_long, index=False)

        if bins_sorted:
            matrix_df = (
                sample_long_df.pivot(
                    index="sample_id",
                    columns="bin_name",
                    values="alt_allele_count",
                )
                .fillna(0)
                .astype(int)
            )
            matrix_df = matrix_df.reindex(selected_samples, fill_value=0)
            matrix_df = matrix_df.reset_index()
        else:
            matrix_df = pd.DataFrame({"sample_id": selected_samples})

        phenotype_values_ordered = [sample_phenotype_value.get(sample) for sample in selected_samples]
        sample_class_ordered = [sample_class.get(sample, "unknown") for sample in selected_samples]

        matrix_df.insert(1, "sample_class", sample_class_ordered)
        matrix_df.insert(1, "phenotype_value", phenotype_values_ordered)
        matrix_df.to_csv(artifact_bin_counts, index=False)

        summary_payload = {
            "report_name": self.name,
            "group_by": group_by,
            "build": build,
            "maf_cutoff": maf_cutoff,
            "rare_case_control_requested": rare_case_control,
            "rare_case_control_active": rare_case_control_active,
            "overall_major_allele": overall_major_allele,
            "include_zero_counts": include_zero_counts,
            "phenotype_file": phenotype_path,
            "resolved_phenotype_column": resolved_phenotype_column,
            "samples_total_in_vcf": len(vcf_samples),
            "samples_selected": len(selected_samples),
            "samples_case": len(case_positions),
            "samples_control": len(control_positions),
            "variants_processed": variants_processed,
            "variants_rare": variants_rare,
            "variants_with_gene_overlap": variants_with_gene_overlap,
            "variants_binned": variants_binned,
            "bins_generated": len(bins_sorted),
            "artifacts": {
                "bin_counts": str(artifact_bin_counts),
                "variant_to_bin": str(artifact_variant_to_bin),
                "bin_definitions": str(artifact_bin_definitions),
                "bin_member_counts": str(artifact_bin_member_counts),
                "sample_bin_long": str(artifact_sample_bin_long),
                "summary_json": str(artifact_summary_json),
            },
            "notes": {
                "maf_internal_only": True,
                "gnomad_external_maf_applied": False,
                "rare_rule": (
                    "max(maf_case, maf_control) <= maf_cutoff"
                    if rare_case_control_active
                    else "maf_filter <= maf_cutoff"
                ),
            },
        }

        artifact_summary_json.write_text(
            json.dumps(summary_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        summary_df = pd.DataFrame(
            [
                {
                    "report_name": self.name,
                    "output_dir": str(output_dir),
                    "group_by": group_by,
                    "maf_cutoff": maf_cutoff,
                    "variants_processed": variants_processed,
                    "variants_rare": variants_rare,
                    "variants_with_gene_overlap": variants_with_gene_overlap,
                    "variants_binned": variants_binned,
                    "bins_generated": len(bins_sorted),
                    "samples_selected": len(selected_samples),
                    "artifact_bin_counts": str(artifact_bin_counts),
                    "artifact_variant_to_bin": str(artifact_variant_to_bin),
                    "artifact_bin_definitions": str(artifact_bin_definitions),
                    "artifact_bin_member_counts": str(artifact_bin_member_counts),
                    "artifact_sample_bin_long": str(artifact_sample_bin_long),
                    "artifact_summary_json": str(artifact_summary_json),
                }
            ]
        )

        return summary_df
