import os
import ast
import json
import requests
import pandas as pd
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.base.entity_query_mixin import EntityQueryMixin
from biofilter.etl.base.gene_query_mixin import GeneQueryMixin
# from biofilter.etl.base.conflict_resolution_mixin import (
#     ConflictResolutionMixin,
# )  # noqa E501
from biofilter.db.models.entity_models import EntityGroup
from biofilter.db.models.curation_models import (
    CurationConflict,
    ConflictStatus,
)  # noqa E501
from biofilter.etl.conflict_manager import ConflictManager


# class DTP(EntityQueryMixin, GeneQueryMixin, ConflictResolutionMixin):
class DTP(EntityQueryMixin, GeneQueryMixin):
    def __init__(
        self,
        logger=None,
        datasource=None,
        etl_process=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.datasource = datasource
        self.etl_process = etl_process
        self.session = session
        self.use_conflict_csv = use_conflict_csv

        self.HGNC_API_URL = "https://rest.genenames.org/fetch/all"
        self.file_name = "hgnc_data.json"

        self.conflict_mgr = ConflictManager(session, logger)




    def extract(self, download_path):
        """
        Extracts data from the HGNC API and stores it locally.
        Also computes a file hash to track content versioning.
        """

        message = ""

        try:
            raw_file = os.path.join(download_path, "hgnc", self.file_name)
            os.makedirs(os.path.dirname(raw_file), exist_ok=True)

            headers = {"Accept": "application/json"}
            response = requests.get(self.HGNC_API_URL, headers=headers)

            if response.status_code != 200:
                msg = f"Failed to fetch data from HGNC: {response.status_code}"
                raise Exception(msg)

            with open(raw_file, "w") as f:
                f.write(response.text)

            message = f"HGNC data extracted and saved at {raw_file}"

            file_hash = compute_file_hash(raw_file)

            return True, message, file_hash

        except Exception as e:
            message = f"❌ Error during extraction: {e}"

            return False, message, None

    def transform(self, raw_path, processed_path):
        message = ""
        try:
            # Paths
            json_file = os.path.join(raw_path, "hgnc", "hgnc_data.json")
            csv_file = os.path.join(processed_path, "hgnc", "hgnc_data.csv")

            # Check if the JSON file exists
            if not os.path.exists(json_file):
                msg = f"File not found: {json_file}"
                raise Exception(msg)

            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)

            # Remove CSV file if it exists
            if os.path.exists(csv_file):
                os.remove(csv_file)
                self.logger.log(
                    f"⚠️ Previous CSV file deleted: {csv_file}", "DEBUG"
                )  # noqa: E501

            # LOAD JSON
            with open(json_file, "r") as f:
                data = json.load(f)

            df = pd.DataFrame(data["response"]["docs"])

            # Save DataFrame to CSV
            df.to_csv(csv_file, index=False)

            self.logger.log(
                f"✅ HGNC data transformed and saved at {csv_file}", "INFO"
            )  # noqa: E501

            return df, True, message

        except Exception as e:
            message = f"❌ Error during transformation: {e}"
            return None, False, message

    def load(self, df=None, processed_path=None):
        total_gene = 0  # not considered conflict genes
        load_status = False
        message = ""

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

        # Check source of data. It can be integrated either using a DataFrame
        # or by specifying the data path as a CSV file.
        if df is None:
            if not processed_path:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_gene, load_status
                # raise ValueError(msg)
            msg = f"Loading data from {processed_path}"
            self.logger.log(msg, "INFO")
            # TODO: Fix the path to processed_path (avoid hardcode now)
            conflict_path = processed_path + "hgnc/hgnc_data_conflict.csv"
            processed_path = processed_path + "hgnc/hgnc_data.csv"

            # Switch to Conflict Mode
            if self.use_conflict_csv:
                processed_path = conflict_path

            if not os.path.exists(processed_path):
                msg = f"File not found: {processed_path}"
                self.logger.log(msg, "ERROR")
                return total_gene, load_status, msg

            df = pd.read_csv(processed_path)

        # Get Entity Group ID
        if not hasattr(self, "entity_group") or self.entity_group is None:
            group = (
                self.session.query(EntityGroup)
                .filter_by(name="Genomics")
                .first()  # noqa: E501
            )  # noqa: E501
            if not group:
                msg = "EntityGroup 'Genomics' not found in the database."
                self.logger.log(msg, "ERROR")
                return total_gene, load_status
                # raise ValueError(msg)
            self.entity_group = group.id
            msg = f"EntityGroup ID for 'Genomics' is {self.entity_group}"
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

        # Interaction to each Gene
        for _, row in df.iterrows():

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
                data_source_id=self.datasource.id,
            )

            # Add or Get EntityName
            for alias in aliases:
                if alias.strip() != gene_master.strip():
                    self.get_or_create_entity_name(
                        entity_id, alias, data_source_id=self.datasource.id
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
                locus_group_name
            )  # noqa: E501
            locus_type_instance = self.get_or_create_locus_type(
                locus_type_name
            )  # noqa: E501
            region_instance = self.get_or_create_genomic_region(
                label=region_label,
                chromosome=chromosome,
                start=start,
                end=end,
            )  # noqa: E501

            group_names_list = self.parse_gene_groups(row.get("gene_group"))

            gene = self.get_or_create_gene(
                symbol=row.get("symbol"),
                hgnc_status=row.get("status"),
                hgnc_id=row.get("hgnc_id"),
                entrez_id=row.get("entrez_id"),
                ensembl_id=row.get("ensembl_gene_id"),
                entity_id=entity_id,
                data_source_id=self.datasource.id,
                locus_group=locus_group_instance,
                locus_type=locus_type_instance,
                gene_group_names=group_names_list,
            )

            if gene == "CONFLICT":
                msg = f"Gene '{gene_master}' has conflicts"
                self.logger.log(msg, "WARNING")
                # Add to the list of genes with resolved conflicts
                genes_with_pending_conflict.append(row)
                continue

            if gene is not None:
                total_gene += 1

                location = self.create_gene_location(
                    gene=gene,
                    chromosome=chromosome,
                    start=row.get("start"),
                    end=row.get("end"),
                    strand=row.get("strand"),
                    region=region_instance,
                    data_source_id=self.datasource.id,
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
            # self.apply_resolution(row)
            self.conflict_mgr.apply_resolution(row)

        # TODO: Apagar o arquivo de conflictos, para evitar ser reprocessado.

        message = f"Loaded {total_gene} genes into database"
        self.logger.log(msg, "INFO")
        return total_gene, True, message
