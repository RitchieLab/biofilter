import os
import re
import gc
import ast
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
from biofilter.db.models.genes_models import Gene
from biofilter.etl.mixins.base_dtp import DTPBase


def extract_id(dbxrefs, prefix):
    for item in dbxrefs.split("|"):
        if item.startswith(prefix):
            return item.split(":")[-1]
    return None


class DTP(DTPBase, EntityQueryMixin):
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
            with gzip.open(gz_path, "rb") as f_in, open(txt_path, "wb") as f_out:  # noqa: E501
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
        """
        
        """

        msg = f"🔧 Transforming the {self.data_source.name} data ..."

        self.logger.log(msg, "INFO")  # noqa: E501

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

        # Header Input File:
        # tax_id / GeneID / Symbol / LocusTag / Synonyms / dbXrefs / chromosome
        # map_location / description / type_of_gene / Symbol_from_nomenclature_authority /
        # Full_name_from_nomenclature_authority / Nomenclature_status / Other_designations /
        # Modification_date / Feature_type

        # Data Example:
        # 9606 / 26099 / SZRD1 / - / C1orf144 /
        # MIM:620682|HGNC:HGNC:30232|Ensembl:ENSG00000055070|AllianceGenome:HGNC:30232 /
        # 1 / 1p36.13 / SUZ RNA binding domain containing 1 / protein-coding / SZRD1 /
        # SUZ RNA binding domain containing 1 / O /
        # SUZ RNA-binding domain-containing|SUZ domain-containing protein 1|UPF0485 protein C1orf144|putative MAPK activating protein PM20,PM21|putative MAPK-activating protein PM18/PM20/PM22 / 
        # 20250612 / -

        try:
            chunks = []
            reader = pd.read_csv(
                input_file,
                sep="\t",
                compression="gzip",
                dtype=str,
                usecols=[
                    "#tax_id", "GeneID", "Symbol", "Synonyms", "dbXrefs",
                    "chromosome", "map_location", "description",
                    "type_of_gene", "Full_name_from_nomenclature_authority",
                    "Other_designations"
                ],
                chunksize=1_000_000  # Can be adjusted based on memory constraints
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
            df["hgnc_id"] = df["dbXrefs"].apply(lambda x: extract_id(x, "HGNC:HGNC"))
            df["hgnc_id"] = df["hgnc_id"].apply(lambda x: f"HGNC:{x}" if x else None)
            df["ensembl_id"] = df["dbXrefs"].apply(lambda x: extract_id(x, "Ensembl"))

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
                    "entrez_id", "symbol", "synonyms", "hgnc_id", "ensembl_id",
                    "full_name", "description", "other_designations",
                    "chromosome", "map_location", "type_of_gene",
                    "source"
                ]
            ]

            # output_file = output_path / "master_data.csv"
            # output_df.to_csv(output_file, index=False)
            output_df.to_csv(output_file_master.with_suffix(".csv"), index=False)  # noqa: E501
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
        ⚠️ Important Note:

        """

        msg = f"📥 Loading {self.data_source.name} data into the database..."

        self.logger.log(
            msg,
            "INFO",  # noqa E501
        )

        total_genes = 0
        load_status = False


        if df is None:
            if not processed_dir:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_genes, load_status, msg

            processed_path = self.get_path(processed_dir)
            # processed_data = str(processed_path / "master_data.csv")
            processed_data = str(processed_path / "master_data.parquet")

            if not os.path.exists(processed_data):
                msg = f"File not found: {processed_data}"
                self.logger.log(msg, "ERROR")
                return total_genes, load_status, msg

            self.logger.log(
                f"📥 Reading data in chunks from {processed_data}", "INFO"
            )  # noqa E501

            # df = pd.read_csv(processed_data, dtype=str)
            df = pd.read_parquet(processed_data, engine="pyarrow")

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
                return total_genes, load_status
                # raise ValueError(msg)
            self.entity_group = group.id
            msg = f"EntityGroup ID for 'Genes' is {self.entity_group}"
            self.logger.log(msg, "DEBUG")

        # Keep only genes without HGNC ID
        df = df[df["hgnc_id"].isnull()]

        # Interaction to each Gene
        for _, row in df.iterrows():

            # Define the Gene Master
            # NOTE 1: We can use the symbol, entrez_id or ensembl_id
            # NOTE 2: Maybe convert to variable from settings
            gene_master = row["symbol"]
            # gene_master = row.get("hgnc_id")
            if not gene_master:
                msg = f"Gene Master not found in row: {row}"
                self.logger.log(msg, "WARNING")
                continue

            # Skip genes with resolved conflicts in lote
            # if gene_master in resolved_genes:
            #     self.logger.log(
            #         f"Gene '{gene_master}' skipped, conflict already resolved",
            #         "DEBUG",  # noqa E501
            #     )
            #     genes_with_solved_conflict.append(row)
            #     continue

            # Collect Genes Aliases
            aliases = []

            for key in ["synonyms", "ensembl_id", "description"]:
                val = row.get(key)
                if not val:
                    continue

                if isinstance(val, str):
                    # Split synonyms if it's a '|' separated string
                    if key == "synonyms":
                        val_list = [v.strip() for v in val.split("|") if v.strip()]
                    else:
                        val_list = [val.strip()]
                elif isinstance(val, list):
                    val_list = [v.strip() for v in val if isinstance(v, str) and v.strip()]
                else:
                    val_list = [str(val).strip()]

                aliases.extend(val_list)

            # Clean and deduplicate
            aliases = list({
                alias
                for alias in aliases
                if alias and alias != gene_master.strip()
            })

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


        #  NOTE: Eu parei AQUI!!!
        
        #         locus_group_name = 4  # other
        #         locus_type_name = 4   # unknown

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

        msg = f"Loaded {total_gene} genes into database"
        self.logger.log(msg, "INFO")
        return total_gene, True, msg
