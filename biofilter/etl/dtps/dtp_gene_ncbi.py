import os
import gc
import gzip
import shutil
import requests
import pandas as pd
from pathlib import Path

from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models.entity_models import (
    EntityGroup,
)  # noqa E501
from biofilter.db.models.genes_models import Gene, GeneGroup, GeneGroupMembership  # noqa E501
from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.etl.mixins.base_dtp import DTPBase


def extract_id(dbxrefs, prefix):
    for item in dbxrefs.split("|"):
        if item.startswith(prefix):
            return item.split(":")[-1]
    return None


class DTP(DTPBase, EntityQueryMixin, GeneQueryMixin):
    def __init__(
        self,
        logger=None,
        datasource=None,
        etl_process=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.data_source = datasource
        self.etl_process = etl_process
        self.session = session
        self.use_conflict_csv = use_conflict_csv

        # DTP versioning
        self.dtp_name = "dtp_gene_ncbi"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "3.0.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Downloads Genes data from NCBI. Uses the hash of 'genes_ncbi.txt' as
        reference. Only proceeds with full extraction if the hash has changed.
        """

        msg = f"⬇️ Starting extraction of {self.data_source.name} data..."

        self.logger.log(
            msg,
            "INFO",  # noqa: E501
        )  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url
        if force_steps:
            last_hash = ""
            msg = "Ignoring hash check, forcing download"
            self.logger.log(msg, "WARNING")
        else:
            last_hash = self.etl_process.raw_data_hash

        try:
            # Prepare download path
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            gz_path = os.path.join(landing_path, "gene_info.gz")
            txt_path = os.path.join(landing_path, "gene_info")

            # Download the data from FTP
            msg = f"⬇️  Fetching gzipped file from: {source_url}"
            self.logger.log(msg, "INFO")

            response = requests.get(source_url, stream=True)

            if response.status_code != 200:
                msg = f"❌ Failed to fetch data: {response.status_code}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            with open(gz_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Extract the gz file
            self.logger.log(f"🗜️  Unzipping to: {txt_path}", "INFO")
            with gzip.open(gz_path, "rb") as f_in, open(
                txt_path, "wb"
            ) as f_out:  # noqa: E501
                # Copy the decompressed content to the output file
                shutil.copyfileobj(f_in, f_out)

            # NOTE: We are checking the hash of the decompressed file to all
            # tax_id. Maybe we should check only 9606

            # Compute and compare hash
            current_hash = compute_file_hash(txt_path)
            if current_hash == last_hash:
                msg = f"⚠️  No changes detected in {txt_path}"
                self.logger.log(msg, "INFO")
                return False, msg, current_hash

            # Drop descompressed gz file
            os.remove(txt_path)

            msg = f"✅ File downloaded and extracted to {txt_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ Exception during extract: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):
        """ """

        msg = f"🔧 Transforming the {self.data_source.name} data ..."

        self.logger.log(msg, "INFO")  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        # Check if raw_dir and processed_dir are provided
        try:
            # Define input/output base paths
            input_path = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            output_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501

            # Ensure output directory exists
            output_path.mkdir(parents=True, exist_ok=True)

            # Input file path
            input_file = input_path / "gene_info.gz"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return None, False, msg

            # Output files paths
            output_file_master = output_path / "master_data"

            # Delete existing files if they exist (both .csv and .parquet)
            for f in [output_file_master]:
                for ext in [".csv", ".parquet"]:
                    target_file = f.with_suffix(ext)
                    if target_file.exists():
                        target_file.unlink()
                        self.logger.log(
                            f"🗑️  Removed existing file: {target_file}", "INFO"
                        )  # noqa E501

        except Exception as e:
            msg = f"❌ Error constructing paths: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

        # Start processing the source file
        # NOTE: Data has 9Gb

        try:
            chunks = []
            reader = pd.read_csv(
                input_file,
                sep="\t",
                compression="gzip",
                dtype=str,
                usecols=[
                    "#tax_id",
                    "GeneID",
                    "Symbol",
                    "Synonyms",
                    "dbXrefs",
                    "chromosome",
                    "map_location",
                    "description",
                    "type_of_gene",
                    "Full_name_from_nomenclature_authority",
                    "Other_designations",
                ],
                # Can be adjusted based on memory constraints
                chunksize=1_000_000,
            )

            for chunk in reader:
                filtered = chunk[chunk["#tax_id"] == "9606"].copy()
                chunks.append(filtered)
                del chunk
                gc.collect()

            df = pd.concat(chunks, ignore_index=True)

            df["entrez_id"] = df["GeneID"]
            df["symbol"] = df["Symbol"]
            df["synonyms"] = df["Synonyms"]
            df["hgnc_id"] = df["dbXrefs"].apply(
                lambda x: extract_id(x, "HGNC:HGNC")
            )  # noqa: E501
            df["hgnc_id"] = df["hgnc_id"].apply(
                lambda x: f"HGNC:{x}" if x else None
            )  # noqa: E501
            df["ensembl_id"] = df["dbXrefs"].apply(
                lambda x: extract_id(x, "Ensembl")
            )  # noqa: E501

            df["full_name"] = df["Full_name_from_nomenclature_authority"]
            # df["description"] = df["description"]
            df["other_designations"] = df["Other_designations"]
            # df["chromosome"] = df["chromosome"]
            # df["map_location"] = df["map_location"]
            # df["type_of_gene"] = df["type_of_gene"]
            # df["modification_date"] = df["Modification_date"]
            df["source"] = "ncbi"

            output_df = df[
                [
                    "entrez_id",
                    "symbol",
                    "synonyms",
                    "hgnc_id",
                    "ensembl_id",
                    "full_name",
                    "description",
                    "other_designations",
                    "chromosome",
                    "map_location",
                    "type_of_gene",
                    "source",
                ]
            ]

            # output_file = output_path / "master_data.csv"
            # output_df.to_csv(output_file, index=False)
            output_df.to_csv(
                output_file_master.with_suffix(".csv"), index=False
            )  # noqa: E501
            output_df.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )  # noqa: E501

            msg = f"✅ NCBI Gene transform completed: {len(output_df)} records"
            self.logger.log(msg, "INFO")
            return None, True, msg

        except Exception as e:
            msg = f"❌ Transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):
        """
        Load NCBI genes that are not present in HGNC, supplementing the
        Biofilter3R database.

        Notes:
        - Each gene will generate an Entity using the NCBI symbol as the
            primary name.
        - Aliases are extracted from synonyms, Ensembl ID and description.
        - Genes are linked to a placeholder GeneGroup ("NCBI Gene").
        - Genomic regions are parsed from `map_location`.
        - Only genes with missing HGNC ID are processed.
        """

        self.logger.log(
            f"📥 Loading {self.data_source.name} data into the database...",
            "INFO",  # noqa: E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_genes = 0
        total_gene_existing = 0
        load_status = False

        try:
            # Load data
            if df is None:
                if not processed_dir:
                    msg = "Either 'df' or 'processed_dir' must be provided."
                    self.logger.log(msg, "ERROR")
                    return total_genes, load_status, msg

                processed_path = self.get_path(processed_dir)
                processed_data = str(processed_path / "master_data.parquet")

                if not os.path.exists(processed_data):
                    msg = f"File not found: {processed_data}"
                    self.logger.log(msg, "ERROR")
                    return total_genes, load_status, msg

                self.logger.log(
                    f"📥 Reading data from {processed_data}", "INFO"  # noqa: E501
                )
                df = pd.read_parquet(processed_data, engine="pyarrow")

            # Filter only genes not curated by HGNC
            df = df[df["hgnc_id"].isnull()]

            # Get or create EntityGroup for Genes
            if not hasattr(self, "entity_group") or self.entity_group is None:
                group = (
                    self.session.query(EntityGroup).filter_by(name="Genes").first()  # noqa E501
                )  # noqa: E501
                if not group:
                    msg = "EntityGroup 'Genes' not found in the database."
                    self.logger.log(msg, "ERROR")
                    return total_genes, load_status, msg
                self.entity_group = group.id
                self.logger.log(
                    f"EntityGroup ID for 'Genes' is {self.entity_group}",
                    "DEBUG",  # noqa: E501
                )

            # Get or create GeneGroup placeholder
            gene_group = (
                self.session.query(GeneGroup)
                .filter_by(name="NCBI Gene")
                .first()  # noqa: E501
            )
            if not gene_group:
                gene_group = GeneGroup(
                    name="NCBI Gene",
                    description="Gene group for NCBI genes",
                )
                self.session.add(gene_group)
                self.session.commit()
                self.logger.log(
                    f"🧬 Created GeneGroup: {gene_group.name}", "INFO"
                )  # noqa: E501

            # Constants
            locus_group_id = 4  # "other"
            locus_type_id = 4  # "unknown"
            start = None
            end = None

            for _, row in df.iterrows():
                gene_master = row.get("symbol", "").strip()

                # Skip genes with invalid symbol
                if not gene_master or gene_master.lower() in {
                    "-",
                    "unknown",
                    "n/a",
                }:  # noqa: E501
                    msg = f"⚠️ Skipping gene with invalid symbol: {row.get('entrez_id')}"  # noqa: E501
                    self.logger.log(msg, "WARNING")
                    continue

                # Extract aliases
                aliases = []
                for key in ["synonyms", "ensembl_id", "description"]:
                    val = row.get(key)
                    if not val:
                        continue
                    if isinstance(val, str):
                        if key == "synonyms":
                            val_list = [
                                v.strip() for v in val.split("|") if v.strip()
                            ]  # noqa: E501
                        else:
                            val_list = [val.strip()]
                    elif isinstance(val, list):
                        val_list = [
                            v.strip() for v in val if isinstance(v, str)
                        ]  # noqa: E501
                    else:
                        val_list = [str(val).strip()]
                    aliases.extend(val_list)

                # Deduplicate and clean aliases
                aliases = list(
                    {
                        alias
                        for alias in aliases
                        if alias
                        and alias != gene_master
                        and alias != "-"
                        and alias.strip()
                    }
                )

                # Add Entity
                entity_id, _ = self.get_or_create_entity(
                    name=gene_master,
                    group_id=self.entity_group,
                    data_source_id=self.data_source.id,
                )

                # Add aliases
                for alias in aliases:
                    self.get_or_create_entity_name(
                        entity_id, alias, data_source_id=self.data_source.id
                    )

                # Get region info (first entry only for now)
                region_label = row.get("map_location")
                chromosome = row.get("chromosome")

                region_label_list = []
                if region_label:
                    if "|" in region_label:
                        region_label_list = [
                            v.strip()
                            for v in region_label.split("|")
                            if v.strip()  # noqa: E501
                        ]
                    else:
                        region_label_list = [region_label.strip()]

                region_instance = None
                for region in region_label_list:
                    region_instance = self.get_or_create_genomic_region(
                        label=region,
                        chromosome=chromosome,
                        start=start,
                        end=end,
                    )

                # Check if Gene already exists
                existing = (
                    self.session.query(Gene)
                    .filter_by(entity_id=entity_id, entrez_id=row["entrez_id"])
                    .first()
                )

                if existing:
                    total_gene_existing += 1
                    msg = f"Gene already exists: {gene_master} (Entrez ID: {row['entrez_id']})"  # noqa: E501
                    self.logger.log(msg, "INFO")
                    continue

                # Create Gene
                gene_instance = Gene(
                    omic_status_id=5,  # Arbitrary placeholder status
                    entity_id=entity_id,
                    hgnc_status="Gene from NCBI",
                    hgnc_id=None,
                    entrez_id=row.get("entrez_id"),
                    ensembl_id=row.get("ensembl_id"),
                    data_source_id=self.data_source.id,
                    locus_group_id=locus_group_id,
                    locus_type_id=locus_type_id,
                )
                self.session.add(gene_instance)
                self.session.flush()  # ensure gene_instance.id is available

                # Add to GeneGroupMembership
                membership = GeneGroupMembership(
                    gene_id=gene_instance.id,
                    group_id=gene_group.id,
                )
                self.session.add(membership)
                # self.session.flush()

                # Add GeneLocation
                if region_instance:
                    self.get_or_create_gene_location(
                        gene=gene_instance,
                        chromosome=chromosome,
                        start=start,
                        end=end,
                        strand=None,
                        region=region_instance,
                        data_source_id=self.data_source.id,
                    )

                total_genes += 1

            self.session.commit()
            msg = f"✅ Genes loaded from NCBI (not in HGNC): {total_genes} new, {total_gene_existing} existing"  # noqa: E501
            self.logger.log(msg, "INFO")
            return total_genes, True, msg

        except Exception as e:
            self.session.rollback()
            msg = f"❌ Load failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return 0, False, msg
