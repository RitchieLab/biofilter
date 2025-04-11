import os
import ast
import json
import requests
import pandas as pd
from biofilter.etl.base.entity_query_mixin import EntityQueryMixin
from biofilter.etl.base.gene_query_mixin import GeneQueryMixin
from biofilter.db.models.entity_models import EntityGroup
from biofilter.db.models.curation_models import (
    CurationConflict,
    ConflictStatus
)


class DTP(EntityQueryMixin, GeneQueryMixin):
    def __init__(
        self, logger=None, datasource=None, etl_process=None, session=None
    ):  # noqa: E501
        self.logger = logger
        self.datasource = datasource
        self.etl_process = etl_process
        self.session = session

        self.HGNC_API_URL = "https://rest.genenames.org/fetch/all"
        self.file_name = "hgnc_data.json"
        # self.gene_group = 1  # Exemplo: grupo para Gene
        # self.gene_category = 1  # Exemplo: categoria para Gene

    def extract(self, download_path):

        # NOTE DEV: Vou manter os nomes dos arquivos como hardcoded para
        # facilitar o desenvolvimento, mas depois podemos usar os nomes
        # dos arquivos que estão no banco de dados. Adicionar melhores
        # controles e flow inteligentes.

        try:
            raw_file = os.path.join(download_path, "hgnc", self.file_name)
            os.makedirs(os.path.dirname(raw_file), exist_ok=True)

            # Log e status
            self.etl_process.extract_status = "running"
            self.etl_process.extract_start = pd.Timestamp.now()
            self.session.commit()

            headers = {"Accept": "application/json"}
            response = requests.get(self.HGNC_API_URL, headers=headers)

            if response.status_code != 200:
                msg = f"Failed to fetch data from HGNC: {response.status_code}"
                raise Exception(msg)

            with open(raw_file, "w") as f:
                f.write(response.text)

            # Log e status
            self.logger.log(
                f"✅ HGNC raw data saved at {raw_file}", "INFO"
            )  # noqa: E501
            self.etl_process.extract_status = "completed"
            self.etl_process.extract_end = pd.Timestamp.now()
            self.session.commit()

            return True

        except Exception as e:
            msg = f"❌ Error during extraction: {e}"
            self.logger.log(msg, "ERROR")
            self.etl_process.extract_status = "failed"
            self.etl_process.extract_end = pd.Timestamp.now()
            self.session.commit()
            return False

    def transform(self, raw_path, processed_path):
        try:
            # Log e status
            self.etl_process.transform_status = "running"
            self.etl_process.transform_start = pd.Timestamp.now()
            self.session.commit()

            # Paths
            json_file = os.path.join(raw_path, "hgnc", "hgnc_data.json")
            csv_file = os.path.join(processed_path, "hgnc", "hgnc_data.csv")

            # Check if the JSON file exists
            if not os.path.exists(json_file):
                msg = f"File not found: {json_file}"
                raise Exception(msg)

            # Cria diretório de saída, se não existir
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)

            # Remove CSV anterior (se existir)
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

            # Log e status
            self.logger.log(
                f"✅ HGNC data transformed and saved at {csv_file}", "INFO"
            )  # noqa: E501
            self.etl_process.transform_status = "completed"
            self.etl_process.transform_end = pd.Timestamp.now()
            self.session.commit()

            return df, True

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            self.logger.log(msg, "ERROR")
            self.etl_process.transform_status = "failed"
            self.etl_process.transform_end = pd.Timestamp.now()
            self.session.commit()
            return None, False

    def load(self, df=None, processed_path=None):
        total_gene = 0
        load_status = False

        # Table that will be used to store the data
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
            # TODO: Ajustar o caminho do arquivo CSV
            processed_path = processed_path + "/hgnc/hgnc_data.csv"
            df = pd.read_csv(processed_path)

        # Change Process Status
        self.etl_process.load_status = "running"
        self.etl_process.load_start = pd.Timestamp.now()
        self.session.commit()

        # Get Entity Group ID
        if not hasattr(self, "entity_group") or self.entity_group is None:
            group = self.session.query(EntityGroup).filter_by(name="Genomics").first()
            if not group:
                msg = "EntityGroup 'Genomics' not found in the database."
                self.logger.log(msg, "ERROR")
                return total_gene, load_status
                # raise ValueError(msg)
            self.entity_group = group.id
            msg = f"EntityGroup ID for 'Genomics' is {self.entity_group}"
            self.logger.log(msg, "DEBUG")

        # Pré-carrega os HGNC IDs com conflitos resolvidos
        resolved_genes = {
            c.identifier
            for c in self.session.query(CurationConflict)
            .filter_by(entity_type="gene", status=ConflictStatus.resolved)
        }

        # Armazena os genes com conflitos resolvidos para processar depois
        genes_com_conflito_resolvido = []

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
                    f"⏭️ Gene '{gene_master}' skipped - conflict already resolved",
                    "DEBUG"
                )
                genes_com_conflito_resolvido.append(row)
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

            if gene:
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


        # Pós-processamento dos conflitos resolvidos
        for row in genes_com_conflito_resolvido:
            # Aqui você aplica a regra (merge, delete, etc.)
            self.logger.log(
                f"🔁 Pós-processando gene com conflito resolvido: {row.get('hgnc_id')}",
                "INFO"
            )
            self.apply_resolution(row)  # ou outro nome/método específico


        self.session.commit()
        msg = f"Loaded {total_gene} genes into database"
        self.logger.log(msg, "INFO")
        load_status = True
        return total_gene, load_status
