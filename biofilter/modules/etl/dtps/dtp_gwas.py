import os
import re
from dataclasses import dataclass
import shutil
import time  # DEBUG MODE
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.utils.file_hash import compute_file_hash

# from sqlalchemy.orm import joinedload


"""
# 1.2.0: Replace file to new ZIP format in dez/2025
"""


@dataclass
class GWASConfig:
    parquet_compression: str = "zstd"

    # One consolidated file: GWAS is small (about 1 M associations) and
    # 16.4% of rows have no position, so there is no chromosome to
    # partition them by.
    table_name: str = "variant_gwas"


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        db=None,
    ):  # noqa: E501
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.config = GWASConfig()
        self.db = db

        # DTP versioning
        self.dtp_name = "dtp_gwas"
        self.dtp_version = "1.2.0"
        self.compatible_schema_min = "4.0.0"
        self.compatible_schema_max = "4.2.0"

    # -------------------------------------------------------------------------
    #                            EXTRACT METHOD
    # -------------------------------------------------------------------------
    def extract(self, raw_dir: str):
        """
        Download flat_files from EBI FTP.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(msg, "INFO")

        try:
            # Check compatibility
            self.check_compatibility()

            # source_url = self.data_source.source_url
            # Donwload more files to extract data
            base_url = "https://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/"  # noqa E501

            associations_zip_name = "gwas-catalog-associations-full.zip"
            efo_tsv_name = "gwas-efo-trait-mappings.tsv"

            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)

            # 1) Download EFO mappings (TSV simples)
            efo_url = base_url + efo_tsv_name
            efo_out = os.path.join(landing_path, efo_tsv_name)

            try:
                self.logger.log(f"⬇️ Downloading {efo_url} ...", "INFO")
                r = requests.get(efo_url, stream=True)
                r.raise_for_status()
                with open(efo_out, "wb") as handle:
                    for chunk in r.iter_content(chunk_size=8192):
                        handle.write(chunk)
            except Exception as e:
                msg = f"❌ Failed to download {efo_tsv_name}: {e}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            # 2) Download associations ZIP
            zip_url = base_url + associations_zip_name
            zip_path = os.path.join(landing_path, associations_zip_name)

            try:
                self.logger.log(f"⬇️ Downloading {zip_url} ...", "INFO")
                r = requests.get(zip_url, stream=True)
                r.raise_for_status()
                with open(zip_path, "wb") as handle:
                    for chunk in r.iter_content(chunk_size=8192):
                        handle.write(chunk)
            except Exception as e:
                msg = f"❌ Failed to download {associations_zip_name}: {e}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            # 3) Extract TSV and rename
            associations_tsv_final = os.path.join(
                landing_path,
                "gwas-catalog-associations.tsv",
            )

            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    # Get first .tsv in the zip
                    members = [m for m in zf.namelist() if m.lower().endswith(".tsv")]  # noqa E501
                    if not members:
                        raise RuntimeError(
                            f"No TSV file found inside {associations_zip_name}"
                        )

                    inner_tsv = members[0]
                    self.logger.log(
                        f"📦 Extracting {inner_tsv} from ZIP into "
                        f"{associations_tsv_final}",
                        "INFO",
                    )

                    with zf.open(inner_tsv) as src, open(
                        associations_tsv_final, "wb"
                    ) as dst:
                        shutil.copyfileobj(src, dst)

                # Remove zip file
                try:
                    os.remove(zip_path)
                except OSError:
                    pass

            except Exception as e:
                msg = f"❌ Failed to extract TSV from {associations_zip_name}: {e}"  # noqa E501
                self.logger.log(msg, "ERROR")
                return False, msg, None

            current_hash = compute_file_hash(associations_tsv_final)

            msg = f"✅ GWAS Catalog files downloaded to {landing_path}"
            self.logger.log(msg, "INFO")

            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # -------------------------------------------------------------------------
    #                            TRANSFORM METHOD
    # -------------------------------------------------------------------------
    def transform(self, raw_dir: str, processed_dir: str):
        """ """

        msg = f"⚙️ Starting transform of {self.data_source.name}..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        if self.debug_mode:
            start_total = time.time()

        try:
            input_path = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            output_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )
            output_path.mkdir(parents=True, exist_ok=True)

            # --- GWAS Catalog File ---
            input_file = input_path / "gwas-catalog-associations.tsv"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            dtype_map = {
                "CHR_ID": str,
                "CHR_POS": str,  # pode ser único valor ou lista sep por ";"
                "SNPS": str,
                "SNP_ID_CURRENT": str,  # idem, pode ter múltiplos
                "RISK ALLELE FREQUENCY": str,  # pode ser número, range ou "NR"
                "P-VALUE": str,  # pode ser "NR" ou notação científica
                "OR or BETA": str,
            }

            gwas_catalog = pd.read_csv(
                input_file, sep="\t", dtype=dtype_map, low_memory=False
            )

            # --- Normalize numeric fields ---
            gwas_catalog["CHR_POS"] = (
                gwas_catalog["CHR_POS"]
                .str.split(";")
                .str[0]  # pega o primeiro se múltiplo
                .pipe(pd.to_numeric, errors="coerce")
            )

            gwas_catalog["SNP_ID_CURRENT"] = (
                gwas_catalog["SNP_ID_CURRENT"]
                .str.split(";")
                .str[0]
                .pipe(pd.to_numeric, errors="coerce")
            )

            # P-VALUE em float (quando possível)
            gwas_catalog["P-VALUE"] = pd.to_numeric(
                gwas_catalog["P-VALUE"], errors="coerce"
            )

            # --- Mapping Traits ---
            input_file = input_path / "gwas-efo-trait-mappings.tsv"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            gwas_trait_mapping = pd.read_csv(input_file, sep="\t")

            # Padronizar URIs -> IDs curtos (EFO, MONDO, HP)
            def normalize_uri(uri: str) -> str:
                if pd.isna(uri) or not isinstance(uri, str):
                    return ""
                return uri.split("/")[-1].replace("_", ":").upper()

            gwas_trait_mapping["efo_id"] = gwas_trait_mapping["EFO URI"].map(
                normalize_uri
            )
            gwas_trait_mapping["parent_id"] = gwas_trait_mapping["Parent URI"].map(  # noqa E501
                normalize_uri
            )

            gwas_trait_mapping = gwas_trait_mapping[
                ["Disease trait", "EFO term", "Parent term", "efo_id", "parent_id"]  # noqa E501
            ]

            # --- Aggregate mappings per Disease trait ---
            agg_cols = {
                "EFO term": lambda x: list(set(x.dropna().astype(str))),
                # "EFO URI": lambda x: list(set(x.dropna().astype(str))),
                "Parent term": lambda x: list(set(x.dropna().astype(str))),
                # "Parent URI": lambda x: list(set(x.dropna().astype(str))),
                "efo_id": lambda x: list(set(x.dropna().astype(str))),
                "parent_id": lambda x: list(set(x.dropna().astype(str))),
            }
            gwas_trait_mapping_grouped = gwas_trait_mapping.groupby(
                "Disease trait", as_index=False
            ).agg(agg_cols)

            # --- Merge with GWAS Catalog ---
            merged = gwas_catalog.merge(
                gwas_trait_mapping_grouped,
                left_on="DISEASE/TRAIT",
                right_on="Disease trait",
                how="left",  # keep all GWAS Catalog rows
            )

            # Garantir que colunas ausentes virem listas vazias
            for col in ["EFO term", "Parent term", "efo_id", "parent_id"]:
                merged[col] = merged[col].apply(
                    lambda x: x if isinstance(x, list) else []
                )

            # # Keep only Columns to load
            merged = merged[
                [
                    # "DATE ADDED TO CATALOG",
                    "PUBMEDID",  # pubmed_id
                    # "FIRST AUTHOR",
                    # "DATE",
                    # "JOURNAL",
                    # "LINK ",
                    # "STUDY",
                    # "DISEASE/TRAIT",
                    "INITIAL SAMPLE SIZE",  # initial_sample_size
                    "REPLICATION SAMPLE SIZE",  # replication_sample_size
                    # "REGION",
                    "CHR_ID",  # chr_id
                    "CHR_POS",  # chr_pos
                    "REPORTED GENE(S)",  # reported_gene
                    "MAPPED_GENE",  # mapped_gene
                    # "UPSTREAM_GENE_ID",
                    # "DOWNSTREAM_GENE_ID",
                    # "SNP_GENE_IDS",
                    # "UPSTREAM_GENE_DISTANCE",
                    # "DOWNSTREAM_GENE_DISTANCE",
                    "STRONGEST SNP-RISK ALLELE",  # snp_risk_allele
                    "SNPS",  # snp_id
                    # "MERGED",
                    # "SNP_ID_CURRENT",
                    "CONTEXT",  # context
                    "INTERGENIC",  # intergenic
                    "RISK ALLELE FREQUENCY",  # risk_allele_frequency
                    "P-VALUE",  # p_value
                    "PVALUE_MLOG",  # pvalue_mlog
                    # "P-VALUE (TEXT)",
                    "OR or BETA",  # odds_ratio_beta
                    "95% CI (TEXT)",  # ci_text
                    # 100% populated with 10,722 distinct values; it was
                    # being dropped here despite the model declaring it.
                    "PLATFORM [SNPS PASSING QC]",  # platform
                    # CNV is left out: populated on every row of the
                    # source but with a single distinct value, so it
                    # carries no information.
                    "Disease trait",  # raw_trait
                    "EFO term",  # mapped_trait
                    "Parent term",  # parent_trait
                    "efo_id",  # mapped_trait_id
                    "parent_id",  # parent_trait_id
                ]
            ]

            column_map = {
                "PUBMEDID": "pubmed_id",
                "Disease trait": "raw_trait",
                "EFO term": "mapped_trait",
                "efo_id": "mapped_trait_id",
                "Parent term": "parent_trait",
                "parent_id": "parent_trait_id",
                "CHR_ID": "chr_id",
                "CHR_POS": "chr_pos",
                "REPORTED GENE(S)": "reported_gene",
                "MAPPED_GENE": "mapped_gene",
                "SNPS": "snp_id",
                "STRONGEST SNP-RISK ALLELE": "snp_risk_allele",
                "RISK ALLELE FREQUENCY": "risk_allele_frequency",
                "CONTEXT": "context",
                "INTERGENIC": "intergenic",
                "P-VALUE": "p_value",
                "PVALUE_MLOG": "pvalue_mlog",
                "OR or BETA": "odds_ratio_beta",
                "95% CI (TEXT)": "ci_text",
                "INITIAL SAMPLE SIZE": "initial_sample_size",
                "REPLICATION SAMPLE SIZE": "replication_sample_size",
                "PLATFORM [SNPS PASSING QC]": "platform",
                "CNV": "cnv",
            }
            merged.rename(columns=column_map, inplace=True)

            # One table, exploded, unpartitioned (ADR-003). GWAS rows are
            # associations (study x trait x SNP), not variants, so there is
            # no 1:1 with variant_masters and no chromosome to partition
            # on for the 16.4% of rows that carry no position.
            merged = self._explode_snps(merged)
            merged = self._add_provenance(merged)
            merged = self._coerce_to_schema(merged, self._arrow_schema())
            # The source belongs in the file name and the footer: a
            # bundle's tables/ holds files from several sources side by
            # side, and `variant_gwas` says what the rows are, not where
            # they came from.
            source = "gwascatalog"
            out_file = (
                output_path / f"{self.config.table_name}_{source}.parquet"
            )
            schema = self._arrow_schema().with_metadata({
                b"biofilter_table": self.config.table_name.encode("utf-8"),
                b"biofilter_source": source.encode("utf-8"),
            })
            merged.to_parquet(
                out_file,
                index=False,
                compression=self.config.parquet_compression,
                schema=schema,
            )

            if self.debug_mode:
                merged.to_csv(output_path / "master_data.csv", index=False)
                end_time = time.time() - start_total
                msg = str(
                    f"processed {len(merged)} records / Time Total: {end_time:.2f}s |"  # noqa E501
                )  # noqa E501
                self.logger.log(msg, "DEBUG")

            size_mb = out_file.stat().st_size / 1024 ** 2
            msg = (
                f"✅ {out_file.name}: {len(merged):,} rows "
                f"({merged['snp_id'].nunique():,} distinct rsIDs), "
                f"{size_mb:.1f} MB"
            )
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Parquet output (ADR-003)
    # ------------------------------------------------------------------
    def _explode_snps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        One row per (association x SNP).

        The GWAS Catalog packs multi-locus associations into a single
        `snp_id` — "rs123 x rs456", "rs1;rs2". The old pipeline kept the
        packed string and derived a `variant_gwas_snp` helper table to
        make it searchable, because SQL cannot index inside a string.
        Exploding here removes the helper table and the join with it, the
        same way the VEP block is exploded rather than stored packed.

        Only 0.5% of rows carry more than one SNP, so the duplication this
        introduces is marginal.
        """
        col = df["snp_id"].fillna("").astype(str)
        parts = col.str.split(r"\s*[xX,;]\s*", regex=True)

        out = df.assign(_snps=parts).explode("_snps", ignore_index=True)
        out["snp_id"] = out["_snps"].str.strip()
        out = out.drop(columns=["_snps"])

        # Rank within the original association, so a multi-SNP row stays
        # reconstructable and the lead SNP is identifiable.
        out["snp_rank"] = (
            out.groupby(
                ["pubmed_id", "raw_trait", "chr_id", "chr_pos"], dropna=False
            )
            .cumcount()
            .astype("int32")
        )
        return out[out["snp_id"].astype(bool)].reset_index(drop=True)

    def _add_provenance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Stamp the source and package that produced these rows."""
        df = df.copy()
        df["data_source_id"] = getattr(self.data_source, "id", None)
        df["etl_package_id"] = getattr(self.package, "id", None)
        return df

    @staticmethod
    def _coerce_to_schema(df: pd.DataFrame, schema) -> pd.DataFrame:
        """
        Align the frame to the declared schema before writing.

        Pandas infers types from the TSV — `pubmed_id` arrives as int64,
        `chr_pos` as float because of its nulls — and pyarrow refuses to
        write a frame whose dtypes disagree with the schema. Coercing
        here, driven by the schema itself, means a new field cannot
        silently land with whatever dtype the parse happened to produce.
        """
        import pyarrow as pa

        out = df.copy()
        for field in schema:
            if field.name not in out.columns:
                out[field.name] = None
            col = out[field.name]
            if pa.types.is_integer(field.type):
                out[field.name] = pd.to_numeric(col, errors="coerce").astype("Int64")  # noqa: E501
            elif pa.types.is_floating(field.type):
                out[field.name] = pd.to_numeric(col, errors="coerce")
            elif pa.types.is_string(field.type):
                out[field.name] = col.astype("string")
        return out[[f.name for f in schema]]

    @staticmethod
    def _arrow_schema():
        """
        Declare the schema rather than letting pandas infer it.

        Every text field is written in full: the old load truncated *every*
        string to 255 characters to fit the relational columns, silently
        cutting fields like `initial_sample_size`, which is `Text` in the
        model and routinely longer. Parquet has no such limit.
        """
        import pyarrow as pa

        text = pa.string()
        return pa.schema([
            pa.field("pubmed_id", pa.int64()),
            pa.field("raw_trait", text),
            pa.field("mapped_trait", text),
            pa.field("mapped_trait_id", text),
            pa.field("parent_trait", text),
            pa.field("parent_trait_id", text),
            pa.field("chr_id", text),
            pa.field("chr_pos", pa.int64()),
            pa.field("reported_gene", text),
            pa.field("mapped_gene", text),
            pa.field("snp_id", text),
            pa.field("snp_rank", pa.int32()),
            pa.field("snp_risk_allele", text),
            pa.field("risk_allele_frequency", pa.float64()),
            pa.field("context", text),
            pa.field("intergenic", text),
            pa.field("p_value", pa.float64()),
            pa.field("pvalue_mlog", pa.float64()),
            pa.field("odds_ratio_beta", text),
            pa.field("ci_text", text),
            pa.field("initial_sample_size", text),
            pa.field("replication_sample_size", text),
            pa.field("platform", text),
            pa.field("data_source_id", pa.int64()),
            pa.field("etl_package_id", pa.int64()),
        ])

    def load(self, processed_dir=None):
        raise NotImplementedError(
            "load() is not used by this DTP under ADR-003. The variant "
            "branch writes parquet directly and is not staged through a "
            "relational database.\n\n"
            "The previous load did three things that no longer apply: it "
            "DELETEd the prior rows before inserting (bundles are "
            "immutable, every write is an insert); it rebuilt the "
            "variant_gwas_snp helper table by splitting snp_id, which the "
            "transform now does inline; and it truncated every string "
            "field to 255 characters to fit the relational columns, "
            "silently cutting fields that parquet stores in full."
        )


    # `_load_legacy` is gone with `variant_gwas_snp`.
    #
    # It was the 4.2.x relational load: DELETE the prior rows, insert,
    # rebuild the rsID helper table, truncate every string to 255 chars
    # to fit the columns. `load()` has raised since ADR-003, so none of
    # it had run — and it held the last reference to VariantGWASSNP,
    # whose foreign key pointed at a `variant_gwas.id` that the parquet
    # never had.
