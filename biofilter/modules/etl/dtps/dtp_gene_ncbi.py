"""
Supplemental gene ingestion from NCBI.

This DTP complements the HGNC baseline in the Genes pipeline:
1) HGNC loads canonical genes.
2) NCBI adds genes that are not curated by HGNC (with filters).
3) Ensembl adds genomic coordinates for loaded genes.
"""

import gc
import gzip
import os
import shutil
import time  # DEBUG
from pathlib import Path

import pandas as pd
import requests

from biofilter.modules.db.models import GeneGroup, OmicStatus  # noqa E501
from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.modules.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.utils.file_hash import compute_file_hash


def extract_id(dbxrefs, prefix):
    for item in dbxrefs.split("|"):
        if item.startswith(prefix):
            return item.split(":")[-1]
    return None


class DTP(DTPBase, EntityQueryMixin, GeneQueryMixin):
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
        self.db = db

        # DTP versioning
        self.dtp_name = "dtp_gene_ncbi"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "4.0.0"

    # -------------------------------------------------------------------------
    #                            EXTRACT METHOD
    # -------------------------------------------------------------------------
    def extract(self, raw_dir: str):
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

            # Drop descompressed gz file
            os.remove(txt_path)

            msg = f"✅ File downloaded and extracted to {txt_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ Exception during extract: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # -------------------------------------------------------------------------
    #                            TRANSFORM METHOD
    # -------------------------------------------------------------------------
    def transform(self, raw_dir: str, processed_dir: str):
        """
        Parse NCBI `gene_info.gz` and emit normalized `master_data`.

        Transform behavior:
        - reads only selected columns from `gene_info.gz`
        - keeps only human genes (`#tax_id == 9606`)
        - derives cross-refs (`hgnc_id`, `ensembl_id`) from `dbXrefs`
        - writes `master_data.parquet` (and CSV for inspection)
        """

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
                return False, msg

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
            return False, msg

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
            return True, msg

        except Exception as e:
            msg = f"❌ Transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------
    def load(self, processed_dir=None):
        """
        Load NCBI genes as a complement to the HGNC baseline.

        Pipeline role:
        - This step should run after `dtp_gene_hgnc`.
        - It is designed to add genes not present in HGNC.

        Row-level filters applied before insert:
        - keep only rows with `hgnc_id` missing (not curated by HGNC)
        - drop invalid symbols (`-`, `unknown`, `n/a`)
        - drop rows without region hint (`map_location == "-"`)

        Loaded output:
        - creates/updates Entities in group "Genes" using NCBI symbol
        - stores aliases (symbols/synonyms/cross refs)
        - creates GeneMaster rows with `hgnc_status = "Gene from NCBI"`
        - uses fallback metadata: GeneGroup "NCBI Gene", LocusType "unknown"

        Notes:
        - human-only filtering (`tax_id = 9606`) happens in `transform`.
        - genomic start/end coordinates are not loaded here; Ensembl is the
          source used to populate `EntityLocation`.
        """

        self.logger.log(
            f"📥 Loading {self.data_source.name} data into the database...",
            "INFO",  # noqa: E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_gene = 0  # not considered conflict genes
        total_warnings = 0

        # VARIABLES TO LOAD PROCESS
        if self.debug_mode:
            start_total = time.time()
            prev_time = start_total

        # ALIASES MAP FROM PROCESS DATA FIELDS
        self.alias_schema = {
            "symbol": ("symbol", "NCBI", True),
            "hgnc_id": ("code", "HGNC", None),
            "ensembl_id": ("code", "ENSEMBL", None),
            "entrez_id": ("code", "ENTREZ", None),
            "full_name": ("synonym", "NCBI", None),
            "synonyms": ("symbol", "NCBI", None),
        }

        # READ PROCESSED DATA TO LOAD
        try:
            # Check if processed dir was set
            if not processed_dir:
                msg = "⚠️  processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR

            processed_path = os.path.join(
                processed_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )

            # Setting files names
            processed_file_name = processed_path + "/master_data.parquet"

            if not os.path.exists(processed_file_name):
                msg = f"⚠️  File not found: {processed_file_name}"
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR
            df = pd.read_parquet(processed_file_name, engine="pyarrow")

            # Filter only genes not curated by HGNC
            # NOTE: We can change to consider and check!
            df = df[df["hgnc_id"].isnull()]

            # Drop Symbol with problems
            invalid_symbols = {"-", "unknown", "n/a"}
            df = df[~df["symbol"].str.lower().isin(invalid_symbols)]

            # Drop Genes without Region
            df = df[df["map_location"] != "-"]

        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # GET ENTITY GROUP ID AND OMICS STATUS
        try:
            self.get_entity_group("Genes")
        except Exception as e:
            msg = f"Error on DTP to get Entity Group: {e}"
            return False, msg  # ⧮ Leaving with ERROR

        # TODO: Better Manage Control to Omics Status / Now all will be Active
        try:
            gene_status = (
                self.session.query(OmicStatus).filter_by(name="active").first()
            )
        except Exception as e:
            msg = f"Error on DTP to get the Omics Status: {e}"
            return False, msg  # ⧮ Leaving with ERROR
        if not gene_status:
            msg = "⚠️  OmicStatus Active not found."
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR
        gene_status_id = gene_status.id

        # SET DB AND DROP INDEXES
        try:
            self.db_write_mode()
            self.drop_indexes(self.get_gene_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # NCBI DataSource no has Gene Group / Locus Group and Type
        try:
            # --> Gene Groups
            gene_group = (
                self.session.query(GeneGroup)
                .filter_by(name="NCBI Gene")
                .first()  # noqa: E501
            )
            if not gene_group:
                gene_group = GeneGroup(
                    name="NCBI Gene",
                    description="Gene group for NCBI genes",
                    data_source_id=self.data_source.id,
                )
                self.session.add(gene_group)
                self.session.commit()
                self.logger.log(
                    f"🧬 Created GeneGroup: {gene_group.name}", "INFO"
                )  # noqa: E501
            gene_group_id = ["NCBI Gene"]

            # # --> Locus Groups
            # locus_group_instance, status = self.get_or_create_locus_group(
            #     name="other",
            #     data_source_id=self.data_source.id,
            # )  # noqa: E501
            # locus_group_other = locus_group_instance.id  # "4 - other"
            # if not status:
            #     msg = "⚠️  Error on Locus Group 'other'"
            #     self.logger.log(msg, "WARNING")

            # --> Locus Types
            locus_type_instance, status = self.get_or_create_locus_type(
                name="unknown",
                data_source_id=self.data_source.id,
                package_id=self.package.id,
            )  # noqa: E501
            # locus_type_id = locus_type_instance.id  # "4 - unknown"
            if not status:
                msg = "⚠️  Error on Locus Type 'unknown' "
                self.logger.log(msg, "WARNING")

        except Exception as e:
            self.session.rollback()
            msg = f"⚠️  Error in Gene Group insert, error: '{e}'"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # NCBI does not have this information
        # TODO: Check how to do this
        is_active = True

        for _, row in df.iterrows():

            gene_master = row.get("symbol", "").strip()

            # NOTE: Use to debugging
            # if gene_master == "FACL1":
            #     pass

            if not gene_master:
                msg = f"⚠️  Gene Master not found in row: {row}"
                self.logger.log(msg, "WARNING")
                total_warnings += 1
                # TODO: Add in ETLLOG Model
                continue

            # If in debug mode, show times
            if self.debug_mode:
                current_time = time.time()
                elapsed_total = current_time - start_total
                elapsed_since_last = (current_time - prev_time) * 1000
                prev_time = current_time
                msg = str(
                    f"{row.name} - {gene_master} | Total: {elapsed_total:.2f}s | Δ: {elapsed_since_last:.0f}ms"  # noqa E501
                )  # noqa E501
                self.logger.log(msg, "DEBUG")

            # Create a dict of Aliases
            alias_dict = self.build_alias(row)

            # Drop Alias Values invalid
            alias_dict = [
                a
                for a in alias_dict
                if str(a.get("alias_value", "")).strip() not in {"", "-"}
            ]

            # Only Primary Name
            is_primary_alias = next(
                (a for a in alias_dict if a.get("is_primary")), None
            )
            # Only Aliases Names
            not_primary_alias = [
                a for a in alias_dict if a != is_primary_alias
            ]  # noqa E501

            # --- CREATE THE ENTITY RECORDS ---

            # Add or Get Entity
            entity_id, _ = self.get_or_create_entity(
                name=is_primary_alias["alias_value"],
                group_id=self.entity_group,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type=is_primary_alias["alias_type"],
                xref_source=is_primary_alias["xref_source"],
                alias_norm=is_primary_alias["alias_norm"],
                is_active=is_active,
            )

            # Add or Get EntityName
            self.get_or_create_entity_name(
                group_id=self.entity_group,
                entity_id=entity_id,
                aliases=not_primary_alias,
                is_active=is_active,
                data_source_id=self.data_source.id,  # noqa E501
                package_id=self.package.id,
            )

            # -- CREATE THE GENES RECORDS ---

            # Define data values
            chromosome = row.get("chromosome")

            # Same Group Name from HGNC
            if row.get("type_of_gene") == "protein-coding":
                locus_group = "protein-coding gene"
            else:
                locus_group = row.get("type_of_gene")

            # --> Locus Groups
            locus_group_instance, status = self.get_or_create_locus_group(
                name=locus_group,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
            )  # noqa: E501
            if not status:
                msg = f"⚠️  Error on Locus Group to: {gene_master}"
                self.logger.log(msg, "WARNING")
                total_warnings += 1
                continue  # TODO: Add in ETLLOG Model

            # --> Genes
            gene, _, status = self.get_or_create_gene(
                status_id=gene_status_id,
                symbol=gene_master,
                hgnc_status="Gene from NCBI",
                entity_id=entity_id,
                chromosome=chromosome,
                data_source_id=self.data_source.id,
                locus_group=locus_group_instance,
                locus_type=locus_type_instance,
                gene_group_names=gene_group_id,
                package_id=self.package.id,
            )

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(self.get_gene_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to read mode or create indexes: {e}"  # noqa E501
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        #  ---> LOAD FINISHED WITH SUCCESS

        if total_warnings != 0:
            msg = f"⚠️  {total_warnings} Warning(s) to analysis in the LOG FILE"  # noqa E501
            self.logger.log(msg, "WARNING")

        msg = f"🧬 Loaded {total_gene} genes into database"
        self.logger.log(msg, "INFO")

        return True, msg
