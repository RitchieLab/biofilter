import os
import ast
import json
import time  # DEBUG
import requests
import pandas as pd
from pathlib import Path
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.db.models import (
    EntityGroup,
    CurationConflict,
    ConflictStatus,
)  # noqa E501

from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase


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
        self.conflict_mgr = ConflictManager(session, logger)

        # DTP versioning
        self.dtp_name = "dtp_gene_hgnc"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "3.0.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Download data from the HGNC API and stores it locally.
        Also computes a file hash to track content versioning.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
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
            # Landing directory
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            file_path = os.path.join(landing_path, "hgnc_data.json")

            # Download the file
            msg = f"⬇️  Fetching JSON from API: {source_url} ..."
            self.logger.log(msg, "INFO")

            headers = {"Accept": "application/json"}
            response = requests.get(source_url, headers=headers)

            if response.status_code != 200:
                msg = f"Failed to fetch data from HGNC: {response.status_code}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            with open(file_path, "w") as f:
                f.write(response.text)

            # Compute hash and compare
            current_hash = compute_file_hash(file_path)
            if current_hash == last_hash:
                msg = f"No change detected in {file_path}"
                self.logger.log(msg, "INFO")
                return False, msg, current_hash

            # Finish block
            msg = f"✅ HGNC file downloaded to {file_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    # def transform(self, raw_path, processed_path):
    def transform(self, raw_dir: str, processed_dir: str):

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
            input_file = input_path / "hgnc_data.json"
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

        try:
            # LOAD JSON
            with open(input_file, "r") as f:
                data = json.load(f)

            df = pd.DataFrame(data["response"]["docs"])

            df.to_csv(output_file_master.with_suffix(".csv"), index=False)  # noqa: E501
            df.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )  # noqa: E501

            msg = f"✅ HGNC data transformed and saved at {output_file_master}"  # noqa: E501
            self.logger.log(msg, "INFO")
            return None, True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):

        msg = f"📥 Loading {self.data_source.name} data into the database..."

        self.logger.log(
            msg,
            "INFO",  # noqa E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_gene = 0  # not considered conflict genes
        total_warnings = 0
        load_status = False

        # data_source_id = self.data_source.id

        # Models that will be used to store the data
        # - Entity
        # - EntityName
        # - LocusGroup
        # - LocusType
        # - GenomicRegion
        # - GeneLocation
        # - Gene
        # - GeneGroup
        # - GeneGroupMembership

        # Set DB and drop indexes
        try:
            index_specs = [
                # ──────────────── gene_groups ────────────────
                ("gene_groups", ["name"]),
                # ──────────────── locus_groups ────────────────
                ("gene_locus_groups", ["name"]),
                # ──────────────── locus_types ────────────────
                ("gene_locus_types", ["name"]),
                # ──────────────── omic_status ────────────────
                ("omic_status", ["name"]),
                # ──────────────── genomic_regions ────────────────
                ("gene_genomic_regions", ["label"]),
                ("gene_genomic_regions", ["chromosome"]),
                ("gene_genomic_regions", ["chromosome", "start", "end"]),
                # ──────────────── genes ────────────────
                ("gene_masters", ["entity_id"]),
                ("gene_masters", ["hgnc_id"]),
                ("gene_masters", ["entrez_id"]),
                ("gene_masters", ["ensembl_id"]),
                ("gene_masters", ["locus_group_id"]),
                ("gene_masters", ["locus_type_id"]),
                ("gene_masters", ["data_source_id"]),
                ("gene_masters", ["omic_status_id"]),
                # ──────────────── gene_group_membership ────────────────
                ("gene_group_memberships", ["group_id"]),
                ("gene_group_memberships", ["gene_id"]),
                # ──────────────── gene_locations ────────────────
                ("gene_locations", ["gene_id"]),
                ("gene_locations", ["region_id"]),
                ("gene_locations", ["assembly"]),
                ("gene_locations", ["chromosome"]),
                ("gene_locations", ["chromosome", "start", "end"]),
                ("gene_locations", ["data_source_id"]),
            ]

            index_specs_entity = [
                # Entity
                ("entities", ["group_id"]),
                ("entities", ["has_conflict"]),
                ("entities", ["is_deactive"]),
                # EntityName
                ("entity_names", ["entity_id"]),
                ("entity_names", ["name"]),
                ("entity_names", ["data_source_id"]),
                ("entity_names", ["data_source_id", "name"]),
                ("entity_names", ["data_source_id", "entity_id"]),
                ("entity_names", ["entity_id", "is_primary"]),
                # EntityRelationship
                ("entity_relationships", ["entity_1_id"]),
                ("entity_relationships", ["entity_2_id"]),
                ("entity_relationships", ["relationship_type_id"]),
                ("entity_relationships", ["data_source_id"]),
                (
                    "entity_relationships",
                    ["entity_1_id", "relationship_type_id"],
                ),  # noqa E501
                (
                    "entity_relationships",
                    ["entity_1_id", "entity_2_id", "relationship_type_id"],
                ),  # noqa E501
                # EntityRelationshipType
                ("entity_relationship_types", ["code"]),
            ]

            self.db_write_mode()
            # self.drop_indexes(index_specs) # Keep indices to improve checks
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️ Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        # Check source of data. It can be integrated either using a DataFrame
        # or by specifying the data path as a CSV file.
        if df is None:
            if not processed_dir:
                msg = "Either 'df' or 'processed_dir' must be provided."
                self.logger.log(msg, "ERROR")
                return total_gene, load_status
                # raise ValueError(msg)
            # msg = f"Loading data from {processed_path}"
            # self.logger.log(msg, "INFO")
            # # TODO: Fix the path to processed_path (avoid hardcode now)

            processed_path = os.path.join(
                processed_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )

            conflict_path = processed_path + "/master_data_conflict.csv"
            processed_path = processed_path + "/master_data.csv"

            # Switch to Conflict Mode
            # Reclace the processed_path with the conflict_path and load genes
            # with previous conflicts indentified
            if self.use_conflict_csv:
                processed_path = conflict_path

            if not os.path.exists(processed_path):
                msg = f"File not found: {processed_path}"
                self.logger.log(msg, "ERROR")
                return total_gene, load_status, msg

            # df = pd.read_csv(processed_path)
            df = pd.read_csv(processed_path, dtype=str)

        # Get Entity Group ID
        if not hasattr(self, "entity_group") or self.entity_group is None:
            group = (
                self.session.query(EntityGroup)
                .filter_by(name="Genes")
                .first()  # noqa: E501
            )  # noqa: E501
            if not group:
                msg = "EntityGroup 'Genes' not found in the database."
                self.logger.log(msg, "ERROR")
                return total_gene, load_status
                # raise ValueError(msg)
            self.entity_group = group.id
            msg = f"EntityGroup ID for 'Genes' is {self.entity_group}"
            self.logger.log(msg, "DEBUG")

        # Preload the HGNC IDs with resolved conflicts
        resolved_genes = {
            c.identifier
            for c in self.session.query(CurationConflict).filter_by(
                entity_type="gene", status=ConflictStatus.resolved
            )
        }

        # Gene List with resolved conflicts to be processed later
        genes_with_solved_conflict = []

        # Gene List with pending conflicts (to be processed later)
        genes_with_pending_conflict = []

        # DEBUG
        start_total = time.time()
        prev_time = start_total

        # Interaction to each Gene
        for _, row in df.iterrows():

            # DEBUG
            current_time = time.time()
            # Tempo desde o início
            elapsed_total = current_time - start_total
            # Tempo desde a última iteração
            elapsed_since_last = (current_time - prev_time) * 1000
            prev_time = current_time
            gene_master = row.get("hgnc_id")
            print(
                f"{row.name} - {gene_master} | Total: {elapsed_total:.2f}s | Δ: {elapsed_since_last:.0f}ms"  # noqa E501
            )  # noqa E501

            # Define the Gene Master
            # NOTE 1: We can use the symbol, entrez_id or ensembl_id
            # NOTE 2: Maybe convert to variable from settings
            # gene_master = row.get("symbol")
            gene_master = row.get("hgnc_id")
            if not gene_master:
                msg = f"Gene Master not found in row: {row}"
                self.logger.log(msg, "WARNING")
                continue

            # Skip genes with resolved conflicts in lote
            if gene_master in resolved_genes:
                self.logger.log(
                    f"Gene '{gene_master}' skipped, conflict already resolved",
                    "DEBUG",  # noqa E501
                )
                genes_with_solved_conflict.append(row)
                continue

            # Collect Genes Aliases
            aliases = []

            for key in [
                "hgnc_id",
                "symbol",
                "name",
                "prev_symbol",
                "prev_name",
                "alias_symbol",
                "alias_name",
                "ucsc_id",
                "ensembl_gene_id",
            ]:
                val = row.get(key)
                if val:
                    if isinstance(val, str):
                        try:
                            val_list = ast.literal_eval(val)
                            if not isinstance(val_list, list):
                                val_list = [val_list]
                        except (ValueError, SyntaxError):
                            val_list = [val]
                    elif isinstance(val, list):
                        val_list = val
                    else:
                        val_list = [val]
                    aliases.extend(val_list)

            # Clean and deduplicate aliases
            aliases = [a for a in aliases if isinstance(a, str) and a.strip()]
            aliases = list(
                {
                    alias.strip()
                    for alias in aliases
                    # Master Gene was already added
                    if alias.strip() != gene_master.strip()
                }
            )

            # BLOCK TO CREATE THE ENTITY RECORDS

            # Add or Get Entity
            entity_id, _ = self.get_or_create_entity(
                name=gene_master,
                group_id=self.entity_group,
                # category_id=self.gene_category,
                data_source_id=self.data_source.id,
            )

            # Add or Get EntityName
            for alias in aliases:
                if alias.strip() != gene_master.strip():
                    self.get_or_create_entity_name(
                        entity_id, alias, data_source_id=self.data_source.id
                    )

            # BLOCK TO CREATE THE GENES RECORDS

            # Define data values
            locus_group_name = row.get("locus_group")
            locus_type_name = row.get("locus_type")
            region_label = row.get("location_sortable")
            chromosome = self.extract_chromosome(row.get("location_sortable"))
            start = row.get("start")
            end = row.get("end")

            locus_group_instance = self.get_or_create_locus_group(
                name=locus_group_name,
                data_source_id=self.data_source.id,
            )  # noqa: E501
            locus_type_instance = self.get_or_create_locus_type(
                name=locus_type_name,
                data_source_id=self.data_source.id,
            )  # noqa: E501
            region_instance = self.get_or_create_genomic_region(
                label=region_label,
                chromosome=chromosome,
                start=start,
                end=end,
                data_source_id=self.data_source.id,
            )  # noqa: E501

            group_names_list = self.parse_gene_groups(row.get("gene_group"))

            gene, conflict_flag = self.get_or_create_gene(
                symbol=row.get("symbol"),
                hgnc_status=row.get("status"),
                hgnc_id=row.get("hgnc_id"),
                entrez_id=row.get("entrez_id"),
                ensembl_id=row.get("ensembl_gene_id"),
                entity_id=entity_id,
                data_source_id=self.data_source.id,
                locus_group=locus_group_instance,
                locus_type=locus_type_instance,
                gene_group_names=group_names_list,
            )

            if conflict_flag:
                msg = f"Gene '{gene_master}' has conflicts"
                self.logger.log(msg, "WARNING")
                # Add to the list of genes with resolved conflicts
                genes_with_pending_conflict.append(row)

            if gene is not None:
                total_gene += 1

                location = self.get_or_create_gene_location(
                    gene=gene,
                    chromosome=chromosome,
                    start=row.get("start"),
                    end=row.get("end"),
                    strand=row.get("strand"),
                    region=region_instance,
                    data_source_id=self.data_source.id,
                )

            # Check if location was created successfully
            if not location:
                msg = f"Failed to create Location for gene {gene_master}"
                self.logger.log(msg, "WARNING")

        # Process the pending conflicts
        if genes_with_pending_conflict:
            conflict_df = pd.DataFrame(genes_with_pending_conflict)

            # Se o arquivo já existir, vamos sobrescrevê-lo
            if os.path.exists(conflict_path):
                msg = f"⚠️ Overwriting existing conflict file: {conflict_path}"  # noqa: E501
                self.logger.log(msg, "WARNING")

            conflict_df.to_csv(conflict_path, index=False)
            msg = f"✅ Saved {len(conflict_df)} gene conflicts to {conflict_path}"  # noqa: E501
            self.logger.log(msg, "INFO")

            # TODO: 🧠 Sugestão adicional (opcional)
            # Generalizar esse comportamento em um helper como
            # save_pending_conflicts(entity_type: str, rows: List[Dict],
            # path: str) para facilitar reutilização em SNPs, Proteins etc.

        # post-processing the resolved conflicts
        for row in genes_with_solved_conflict:
            msg = f"Check and apply conflict rules to  {row.get('hgnc_id')}"
            self.logger.log(msg, "INFO")

            # Apply conflict resolution
            self.conflict_mgr.apply_resolution(row)

        # Set DB to Read Mode and Create Index
        try:
            # Drop Indexs
            self.drop_indexes(index_specs)
            self.drop_indexes(index_specs_entity)
            # Stating Indexs
            self.create_indexes(index_specs)
            self.create_indexes(index_specs_entity)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        load_status = True

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"Loaded {total_gene} genes into database"
        self.logger.log(msg, "INFO")

        return total_gene, True, msg
