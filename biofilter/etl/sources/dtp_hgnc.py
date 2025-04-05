import os
import json
import requests
import pandas as pd

# from utils.logger import Logger
# from etl.base import BaseETL


class DTP:
    def __init__(self, logger=None, datasource=None, etl_process=None, session=None):
        self.logger = logger
        self.datasource = datasource
        self.etl_process = etl_process
        self.session = session

    HGNC_API_URL = "https://rest.genenames.org/fetch/all"

    def extract(self, download_path):

        # NOTE DEV: Vou manter os nomes dos arquivos como hardcoded para
        # facilitar o desenvolvimento, mas depois podemos usar os nomes
        # dos arquivos que estão no banco de dados. Adicionar melhores
        # controles e flow inteligentes.

        raw_json_file = os.path.join(download_path, "hgnc", "hgnc_data.json")
        os.makedirs(os.path.dirname(raw_json_file), exist_ok=True)

        self.etl_process.extract_status = "started"
        self.session.commit()

        headers = {"Accept": "application/json"}
        response = requests.get(self.HGNC_API_URL, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from HGNC: {response.status_code}")

        with open(raw_json_file, "w") as f:
            f.write(response.text)

        self.logger.log(f"✅ HGNC raw data saved at {raw_json_file}", "INFO")
        return True

    def transform(self, download_path, processed_path):
        # Caminhos
        json_file = os.path.join(download_path, "hgnc", "hgnc_data.json")
        csv_file = os.path.join(processed_path, "hgnc", "hgnc_data.csv")

        # Verifica se o JSON existe
        if not os.path.exists(json_file):
            msn = f"File not found: {json_file}"
            self.logger.log(msn, "ERROR")
            self.etl_process.transform_status = "ERROR: Download file not found"
            self.session.commit()
            return False

        # Cria diretório de saída, se não existir
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)

        # Remove CSV anterior (se existir)
        if os.path.exists(csv_file):
            os.remove(csv_file)
            self.logger.log(f"⚠️ Previous CSV file deleted: {csv_file}", "DEBUG")

        # Carrega JSON
        with open(json_file, "r") as f:
            data = json.load(f)

        df = pd.DataFrame(data["response"]["docs"])

        # Salva CSV
        df.to_csv(csv_file, index=False)

        # Log e status
        self.logger.log(f"✅ HGNC data transformed and saved at {csv_file}", "INFO")
        self.etl_process.transform_status = "completed"
        self.session.commit()

        return df, True

    def load(self, df=None, processed_path=None):
        if df is None:
            # Fallback: carregar do CSV
            csv_file = os.path.join(processed_path, "hgnc_data.csv")
            if not os.path.exists(csv_file):
                msn = f"❌ CSV not found at {csv_file}"
                self.logger.log(msn, "ERROR")
                self.etl_process.load_status = "ERROR: CSV not found"
                self.session.commit()
                return 0
            df = pd.read_csv(csv_file)

        # Aqui segue a lógica de inserção no banco
        self.logger.log(f"📥 Loading {len(df)} rows into database", "INFO")

        # TODO: mapear para o modelo correto e inserir
        # exemplo fictício:
        # for row in df.itertuples():
        #     self.session.add(Gene(**row._asdict()))
        # self.session.commit()

        return len(df)

    # def load(df, data_source, etl_process, session):
    #     from biofilter.db.models.omics_models import Gene

    #     records = 0
    #     for _, row in df.iterrows():
    #         if "hgnc_id" not in row:
    #             continue

    #         gene = Gene(
    #             hgnc_id=row.get("hgnc_id"),
    #             symbol=row.get("symbol"),
    #             entrez_id=row.get("entrez_id"),
    #             ensembl_id=row.get("ensembl_gene_id"),
    #             name=row.get("name"),
    #             alias_symbols=",".join(row.get("alias_symbol", [])) if isinstance(row.get("alias_symbol"), list) else None,
    #             data_source_id=data_source.id,
    #         )
    #         session.add(gene)
    #         records += 1

    #     session.commit()
    #     logger.log(f"📥 {records} genes loaded into database.", "INFO")
    #     return records
