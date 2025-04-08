import os
import json
import requests
import pandas as pd
from biofilter.etl.base.master_entity_loader import MasterEntityLoader  # Novo mixin


class DTP(MasterEntityLoader):
    def __init__(self, logger=None, datasource=None, etl_process=None, session=None):  # noqa: E501
        self.logger = logger
        self.datasource = datasource
        self.etl_process = etl_process
        self.session = session

        self.HGNC_API_URL = "https://rest.genenames.org/fetch/all"
        self.file_name = "hgnc_data.json"
        self.gene_group = 1  # Exemplo: grupo para Gene
        self.gene_category = 1  # Exemplo: categoria para Gene

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
                msn = f"Failed to fetch data from HGNC: {response.status_code}"
                raise Exception(msn)

            with open(raw_file, "w") as f:
                f.write(response.text)

            # Log e status
            self.logger.log(f"✅ HGNC raw data saved at {raw_file}", "INFO")  # noqa: E501
            self.etl_process.extract_status = "completed"
            self.etl_process.extract_end = pd.Timestamp.now()
            self.session.commit()

            return True

        except Exception as e:
            msn = f"❌ Error during extraction: {e}"
            self.logger.log(msn, "ERROR")
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
                msn = f"File not found: {json_file}"
                raise Exception(msn)

            # Cria diretório de saída, se não existir
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)

            # Remove CSV anterior (se existir)
            if os.path.exists(csv_file):
                os.remove(csv_file)
                self.logger.log(f"⚠️ Previous CSV file deleted: {csv_file}", "DEBUG")  # noqa: E501

            # LOAD JSON
            with open(json_file, "r") as f:
                data = json.load(f)

            df = pd.DataFrame(data["response"]["docs"])

            # Save DataFrame to CSV
            df.to_csv(csv_file, index=False)

            # Log e status
            self.logger.log(f"✅ HGNC data transformed and saved at {csv_file}", "INFO")  # noqa: E501
            self.etl_process.transform_status = "completed"
            self.etl_process.transform_end = pd.Timestamp.now()
            self.session.commit()

            return df, True

        except Exception as e:
            msn = f"❌ Error during transformation: {e}"
            self.logger.log(msn, "ERROR")
            self.etl_process.transform_status = "failed"
            self.etl_process.transform_end = pd.Timestamp.now()
            self.session.commit()
            return None, False

    def load(self, df=None, processed_path=None):
        from biofilter.db.models.omics_models import Gene

        total = 0

        # Log e status
        self.etl_process.load_status = "running"
        self.etl_process.load_start = pd.Timestamp.now()
        self.session.commit()

        for _, row in df.iterrows():

            symbol = row.get("symbol")
            if not symbol:
                self.logger.log("⚠️ Linha ignorada: símbolo vazio", "WARNING")
                continue

            # 🧪 Extrai aliases de várias colunas
            aliases = []

            # Aliases explícitos (ex: "['ABC', 'XYZ']")
            # TODO : Ajustar os nomes de Genes
            for key in ["alias_symbol", "prev_symbol", "alias_name", "prev_name", "name", "ucsc_id", "hgnc_id", "ensembl_gene_id", "symbol"]:
                val = row.get(key)
                if val:
                    if isinstance(val, str):
                        try:
                            val_list = json.loads(val)
                        except json.JSONDecodeError:
                            val_list = [val]
                    elif isinstance(val, list):
                        val_list = val
                    else:
                        val_list = [val]
                    aliases.extend(val_list)

            # ⚙️ Normaliza e filtra
            aliases = list({
                alias.strip().upper()
                for alias in aliases
                if alias and alias.strip().upper() != symbol.strip().upper()
            })

            entity_id, _ = self.get_or_create_entity(
                name=symbol,
                group_id=self.gene_group,
                category_id=self.gene_category,
                aliases=[]  # tratamento abaixo
            )

            # Adiciona aliases
            for alias in aliases:
                if alias.strip().upper() != symbol.strip().upper():
                    self.add_entity_name(entity_id, alias)

            gene = Gene(
                entity_id=entity_id,
                hgnc_id=row.get("hgnc_id"),
                entrez_id=row.get("entrez_id"),
                ensembl_id=row.get("ensembl_gene_id"),

                chromosome=row.get("location_sortable"),  # ou "location" se preferir
                strand=row.get("strand"),  # precisa existir na base
                start=row.get("start"),    # idem
                end=row.get("end"),        # idem

                locus_group=row.get("locus_group"),
                locus_type=row.get("locus_type"),

                gene_group_id=self.gene_group,  # ou row.get("gene_group_id")
                data_source_id=self.data_source.id if hasattr(self, "data_source") else None,
            )
            self.session.add(gene)
            total += 1

        self.session.commit()
        self.logger.log(f"✅ Loaded {total} genes into database", "INFO")
        return total

