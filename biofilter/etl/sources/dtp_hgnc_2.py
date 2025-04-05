import os
import json
import pandas as pd
from etl.base.master_entity_loader import MasterEntityLoader  # Novo mixin


class DTP(MasterEntityLoader):
    def __init__(self, logger=None, datasource=None, etl_process=None, session=None):
        self.logger = logger
        self.datasource = datasource
        self.etl_process = etl_process
        self.session = session

        self.HGNC_API_URL = "https://rest.genenames.org/fetch/all"
        self.file_name = "hgnc_data.json"
        self.gene_group = 1  # Exemplo: grupo para Gene
        self.gene_category = 1  # Exemplo: categoria para Gene

    def extract(self, download_path):
        raw_file = os.path.join(download_path, "hgnc", self.file_name)
        os.makedirs(os.path.dirname(raw_file), exist_ok=True)

        self.etl_process.extract_status = "started"
        self.session.commit()

        headers = {"Accept": "application/json"}
        response = requests.get(self.HGNC_API_URL, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from HGNC: {response.status_code}")

        with open(raw_file, "w") as f:
            f.write(response.text)

        self.logger.log(f"✅ HGNC raw data saved at {raw_file}", "INFO")
        return raw_file

    def transform(self, raw_file, processed_path):
        with open(raw_file, "r") as f:
            data = json.load(f)

        df = pd.DataFrame(data["response"]["docs"])

        os.makedirs(processed_path, exist_ok=True)
        csv_file = os.path.join(processed_path, "hgnc_data.csv")
        df.to_csv(csv_file, index=False)

        self.logger.log(f"✅ CSV saved at {csv_file}", "INFO")
        return df

    def load(self, df):
        from biofilter.db.models.omics_models import Gene

        total = 0

        for _, row in df.iterrows():
            symbol = row.get("symbol")
            entrez_id = row.get("entrez_id")
            aliases = row.get("alias_symbol") or []
            if isinstance(aliases, str):
                aliases = json.loads(aliases)  # no caso de estar como string json

            # 🔎 Verifica e cria Entity + Names
            entity_id = self.get_or_create_entity(
                name=symbol,
                group_id=self.gene_group,
                category_id=self.gene_category,
                aliases=aliases,
            )

            # 🧬 Cria Gene
            gene = Gene(
                entity_id=entity_id,
                symbol=symbol,
                hgnc_id=row.get("hgnc_id"),
                entrez_id=entrez_id,
                ensembl_id=row.get("ensembl_gene_id"),
                location=row.get("location"),
                status=row.get("status"),
            )
            self.session.add(gene)
            total += 1

        self.session.commit()
        self.logger.log(f"✅ Loaded {total} genes into database", "INFO")
        return total
