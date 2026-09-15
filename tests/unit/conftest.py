"""
A bundle small enough to commit, shaped like a real one.

Every report migrated to the native module tests against this. It stays
deliberately small — three genes and a handful of variants — but it
carries the structure that makes gene annotation interesting: a gene
with no build-38 location, an alias that resolves case-insensitively,
relationships pointing both ways, and a gene whose range contains
variants.

It also reproduces on purpose the trap that cost us 2.4 billion
invisible rows: a partitioned table with an empty same-named parent file
beside it. A reader that scans the directory picks the wrong one; a
reader that reads the manifest cannot.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Entity ids, grouped by what they are.
#
# DGENE exists so the disease fixtures can add relationships without
# changing the counts the gene tests assert on TP53 and BRCA1.
TP53, BRCA1, NOLOC, DGENE = 1, 2, 3, 4
PROTEIN, PROTEIN_ISO = 10, 11
PATHWAY = 20
DISEASE, DISEASE_BARE = 30, 31
GO_PARENT, GO_CHILD = 40, 41

GROUP_GENES, GROUP_PROTEINS, GROUP_PATHWAYS = 1, 2, 3
GROUP_DISEASES, GROUP_GO = 4, 5

# Data sources, so provenance and the ClinGen-only summary have something
# to distinguish.
DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN = 1, 2, 3, 4, 5, 6


def _t(**columns) -> pa.Table:
    return pa.table(columns)


def _tables() -> dict[str, pa.Table]:
    """The core tables, as a bundle spells them."""
    return {
        "entity_groups": _t(
            id=pa.array(
                [GROUP_GENES, GROUP_PROTEINS, GROUP_PATHWAYS, GROUP_DISEASES, GROUP_GO],
                pa.int64(),
            ),
            name=pa.array(
                ["Genes", "Proteins", "Pathways", "Diseases", "Gene Ontology"]
            ),
            description=pa.array([None] * 5, pa.string()),
        ),
        "entities": _t(
            id=pa.array(
                [
                    TP53, BRCA1, NOLOC, DGENE,
                    PROTEIN, PROTEIN_ISO, PATHWAY,
                    DISEASE, DISEASE_BARE, GO_PARENT, GO_CHILD,
                ],
                pa.int64(),
            ),
            group_id=pa.array(
                [
                    GROUP_GENES, GROUP_GENES, GROUP_GENES, GROUP_GENES,
                    GROUP_PROTEINS, GROUP_PROTEINS, GROUP_PATHWAYS,
                    GROUP_DISEASES, GROUP_DISEASES, GROUP_GO, GROUP_GO,
                ],
                pa.int64(),
            ),
            has_conflict=pa.array([False] * 11),
            is_active=pa.array([True] * 11),
        ),
        "etl_source_systems": _t(
            id=pa.array(
                [DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN],
                pa.int64(),
            ),
            name=pa.array(
                ["HGNC", "UniProt", "Reactome", "MONDO", "GO", "ClinGen"]
            ),
            description=pa.array([None] * 6, pa.string()),
        ),
        "etl_data_sources": _t(
            id=pa.array(
                [DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN],
                pa.int64(),
            ),
            name=pa.array(
                ["hgnc", "uniprot", "reactome", "mondo", "gene_ontology", "clingen"]
            ),
            source_system_id=pa.array(
                [DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN],
                pa.int64(),
            ),
            data_type=pa.array(
                ["Gene", "Protein", "Pathway", "Disease", "GO", "Disease Relationships"]
            ),
            active=pa.array([True] * 6),
        ),
        "omic_status": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["active"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_locus_groups": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["protein-coding gene"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_locus_types": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["gene with protein product"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_masters": _t(
            id=pa.array([101, 102, 103], pa.int64()),
            entity_id=pa.array([TP53, BRCA1, NOLOC], pa.int64()),
            symbol=pa.array(["TP53", "BRCA1", "NOLOC1"]),
            hgnc_status=pa.array(["Approved", "Approved", "Approved"]),
            omic_status_id=pa.array([1, 1, 1], pa.int64()),
            locus_group_id=pa.array([1, 1, 1], pa.int64()),
            locus_type_id=pa.array([1, 1, 1], pa.int64()),
            chromosome=pa.array([17, 17, 22], pa.int32()),
        ),
        "gene_groups": _t(
            id=pa.array([1, 2], pa.int64()),
            name=pa.array(["p53 family", "BRCA1 A complex"]),
            description=pa.array([None, None], pa.string()),
        ),
        "gene_group_memberships": _t(
            gene_id=pa.array([101, 102], pa.int64()),
            group_id=pa.array([1, 2], pa.int64()),
        ),
        "entity_aliases": _t(
            id=pa.array(list(range(1, 11)), pa.int64()),
            entity_id=pa.array(
                [TP53, TP53, TP53, TP53, TP53, BRCA1, BRCA1, BRCA1, NOLOC, NOLOC],
                pa.int64(),
            ),
            alias_value=pa.array(
                [
                    "TP53", "HGNC:11998", "ENSG00000141510", "7157", "p53",
                    "BRCA1", "HGNC:1100", "ENSG00000012048",
                    "NOLOC1", "no-location gene",
                ]
            ),
            alias_norm=pa.array(
                [
                    "tp53", "hgnc:11998", "ensg00000141510", "7157", "p53",
                    "brca1", "hgnc:1100", "ensg00000012048",
                    "noloc1", "no-location gene",
                ]
            ),
            alias_type=pa.array(
                [
                    "symbol", "code", "code", "code", "synonym",
                    "symbol", "code", "code",
                    "symbol", "name",
                ]
            ),
            xref_source=pa.array(
                [
                    "HGNC", "HGNC", "ENSEMBL", "ENTREZ", "HGNC",
                    "HGNC", "HGNC", "ENSEMBL",
                    "HGNC", "HGNC",
                ]
            ),
            is_primary=pa.array(
                [True, False, False, False, False,
                 True, False, False,
                 True, False]
            ),
            is_active=pa.array([True] * 10),
        ),
        "entity_aliases_extra": _t(
            id=pa.array(list(range(11, 22)), pa.int64()),
            entity_id=pa.array(
                [
                    DGENE,
                    PROTEIN, PROTEIN, PROTEIN_ISO,
                    PATHWAY,
                    DISEASE, DISEASE, DISEASE_BARE,
                    GO_PARENT, GO_CHILD, GO_CHILD,
                ],
                pa.int64(),
            ),
            alias_value=pa.array(
                [
                    "DGENE1",
                    "P04637", "TP53_HUMAN", "P04637-2",
                    "R-HSA-0001",
                    "MONDO:0001", "breast cancer", "MONDO:0002",
                    "GO:0000001", "GO:0000002", "apoptosis",
                ]
            ),
            alias_norm=pa.array(
                [
                    "dgene1",
                    "p04637", "tp53_human", "p04637-2",
                    "r-hsa-0001",
                    "mondo:0001", "breast cancer", "mondo:0002",
                    "go:0000001", "go:0000002", "apoptosis",
                ]
            ),
            alias_type=pa.array(
                [
                    "symbol",
                    "code", "name", "code",
                    "code",
                    "code", "name", "code",
                    "code", "code", "synonym",
                ]
            ),
            xref_source=pa.array(
                [
                    "HGNC",
                    "UNIPROT", "UNIPROT", "UNIPROT",
                    "REACTOME",
                    "MONDO", "MONDO", "MONDO",
                    "GO", "GO", "GO",
                ]
            ),
            is_primary=pa.array(
                [
                    True,
                    True, False, True,
                    True,
                    True, False, True,
                    True, True, False,
                ]
            ),
            is_active=pa.array([True] * 11),
        ),
        # NOLOC deliberately has none: it is the `partial` case.
        "entity_locations": _t(
            id=pa.array([1, 2], pa.int64()),
            entity_id=pa.array([TP53, BRCA1], pa.int64()),
            build=pa.array([38, 38], pa.int32()),
            chromosome=pa.array([17, 17], pa.int32()),
            start_pos=pa.array([100, 5000], pa.int64()),
            end_pos=pa.array([400, 5400], pa.int64()),
        ),
        # --- Diseases ------------------------------------------------
        "disease_masters": _t(
            id=pa.array([301, 302], pa.int64()),
            entity_id=pa.array([DISEASE, DISEASE_BARE], pa.int64()),
            disease_id=pa.array(["MONDO:0001", "MONDO:0002"]),
            label=pa.array(["breast cancer", "bare disease"]),
            description=pa.array(["a tumour of the breast", None]),
            omic_status_id=pa.array([1, 1], pa.int64()),
            data_source_id=pa.array([DS_MONDO, DS_MONDO], pa.int64()),
            etl_package_id=pa.array([71, 71], pa.int64()),
        ),
        "disease_groups": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["neoplasm"]),
            description=pa.array([None], pa.string()),
        ),
        "disease_group_memberships": _t(
            id=pa.array([1], pa.int64()),
            disease_id=pa.array([301], pa.int64()),
            group_id=pa.array([1], pa.int64()),
        ),
        # --- Gene Ontology -------------------------------------------
        # GO_PARENT is_a-parent of GO_CHILD, so one side of each pair has
        # a parent and the other a child.
        "go_masters": _t(
            id=pa.array([401, 402], pa.int64()),
            entity_id=pa.array([GO_PARENT, GO_CHILD], pa.int64()),
            go_id=pa.array(["GO:0000001", "GO:0000002"]),
            name=pa.array(["cell death", "apoptotic process"]),
            namespace=pa.array(["biological_process", "biological_process"]),
            data_source_id=pa.array([DS_GO, DS_GO], pa.int64()),
            etl_package_id=pa.array([72, 72], pa.int64()),
        ),
        "go_relations": _t(
            id=pa.array([1], pa.int64()),
            parent_id=pa.array([401], pa.int64()),
            child_id=pa.array([402], pa.int64()),
            relation_type=pa.array(["is_a"]),
        ),
        # --- Pathways -------------------------------------------------
        "pathway_masters": _t(
            id=pa.array([201], pa.int64()),
            entity_id=pa.array([PATHWAY], pa.int64()),
            pathway_id=pa.array(["R-HSA-0001"]),
            description=pa.array(["apoptosis signalling"]),
            data_source_id=pa.array([DS_REACTOME], pa.int64()),
            etl_package_id=pa.array([73], pa.int64()),
        ),
        # --- Proteins -------------------------------------------------
        # One protein, one isoform of it. The isoform has no relationships
        # of its own — following to the canonical entity is what makes an
        # isoform input report anything at all.
        "protein_masters": _t(
            id=pa.array([101], pa.int64()),
            protein_id=pa.array(["P04637"]),
            function=pa.array(["tumour suppressor"]),
            location=pa.array(["nucleus"]),
            tissue_expression=pa.array(["ubiquitous"]),
            pseudogene_note=pa.array([None], pa.string()),
            data_source_id=pa.array([DS_UNIPROT], pa.int64()),
            etl_package_id=pa.array([74], pa.int64()),
        ),
        "protein_entities": _t(
            id=pa.array([1, 2], pa.int64()),
            entity_id=pa.array([PROTEIN, PROTEIN_ISO], pa.int64()),
            protein_id=pa.array([101, 101], pa.int64()),
            is_isoform=pa.array([False, True]),
            isoform_accession=pa.array([None, "P04637-2"]),
        ),
        "protein_pfams": _t(
            id=pa.array([1, 2], pa.int64()),
            pfam_acc=pa.array(["PF00870", "PF08563"]),
            pfam_id=pa.array(["P53", "P53_TAD"]),
            description=pa.array(["P53 DNA-binding domain", "P53 transactivating"]),
            type=pa.array(["Domain", "Family"]),
        ),
        "protein_pfam_links": _t(
            protein_id=pa.array([101, 101], pa.int64()),
            pfam_pk_id=pa.array([1, 2], pa.int64()),
        ),
        # TP53 on the left once and on the right once, so the report has
        # to count both directions.
        "entity_relationships": _t(
            id=pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            entity_1_id=pa.array(
                [TP53, TP53, PATHWAY, DISEASE, DISEASE, DISEASE], pa.int64()
            ),
            entity_1_group_id=pa.array(
                [
                    GROUP_GENES, GROUP_GENES, GROUP_PATHWAYS,
                    GROUP_DISEASES, GROUP_DISEASES, GROUP_DISEASES,
                ],
                pa.int64(),
            ),
            entity_2_id=pa.array(
                [PROTEIN, PATHWAY, TP53, DGENE, DGENE, PROTEIN], pa.int64()
            ),
            entity_2_group_id=pa.array(
                [
                    GROUP_PROTEINS, GROUP_PATHWAYS, GROUP_GENES,
                    GROUP_GENES, GROUP_GENES, GROUP_PROTEINS,
                ],
                pa.int64(),
            ),
            relationship_type_id=pa.array([1] * 6, pa.int64()),
            # The disease has two ClinGen assertions naming the SAME gene
            # and one MONDO link to a protein: so its ClinGen gene count
            # is 1, its ClinGen relationship count 2, and its total 3.
            data_source_id=pa.array(
                [DS_HGNC, DS_REACTOME, DS_REACTOME, DS_CLINGEN, DS_CLINGEN, DS_MONDO],
                pa.int64(),
            ),
        ),
    }


def _variant_partitions() -> dict[int, pa.Table]:
    """Three variants inside TP53's range (100-400), two elsewhere."""
    return {
        17: _t(
            chromosome=pa.array([17] * 4, pa.int32()),
            position=pa.array([150, 200, 300, 9000], pa.int64()),
            reference_allele=pa.array(["A"] * 4),
            alternate_allele=pa.array(["G"] * 4),
            variant_key=pa.array([f"17:{p}:A:G" for p in (150, 200, 300, 9000)]),
        ),
        22: _t(
            chromosome=pa.array([22], pa.int32()),
            position=pa.array([777], pa.int64()),
            reference_allele=pa.array(["C"]),
            alternate_allele=pa.array(["T"]),
            variant_key=pa.array(["22:777:C:T"]),
        ),
    }


@pytest.fixture
def fixture_bundle(tmp_path: Path) -> Path:
    root = tmp_path / "20260101"
    tables_dir = root / "tables"
    entries: list[dict] = []

    def write(rel: str, data: pa.Table, **entry) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(data, path)
        entries.append(
            {
                "file": rel,
                "rows": data.num_rows,
                "bytes": path.stat().st_size,
                **entry,
            }
        )

    tables = _tables()
    # Written as one relation: split in the source only so each domain's
    # aliases sit next to the rest of that domain.
    tables["entity_aliases"] = pa.concat_tables(
        [tables["entity_aliases"], tables.pop("entity_aliases_extra")]
    )
    for name, table in tables.items():
        write(f"tables/{name}.parquet", table, name=name, branch="core")

    for chrom, table in _variant_partitions().items():
        write(
            f"tables/variant_masters/variant_masters_chr{chrom}.parquet",
            table,
            name=f"variant_masters_chr{chrom}",
            table="variant_masters",
            branch="variant",
        )

    # Undeclared, empty, wrong schema — the shadowing parent.
    pq.write_table(
        pa.table(
            {
                "variant_id": pa.array([], pa.int64()),
                "position_start": pa.array([], pa.int64()),
            }
        ),
        tables_dir / "variant_masters.parquet",
    )

    manifest = {
        "manifest_version": 2,
        "biofilter_version": "4.3.0",
        "schema_version": "4.3.0",
        "format": "parquet",
        "created_at": "2026-01-01T00:00:00+00:00",
        "bundle_id": "fixturebundle0001",
        "tables": entries,
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (root / "build_record.json").write_text(
        json.dumps({"built_at": "2026-01-01T00:00:00+00:00", "steps": []})
    )
    return root
