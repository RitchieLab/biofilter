import os
import json
import requests
import pandas as pd
from datetime import datetime

# Diretórios para armazenar arquivos temporários e processados
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_LAKE_RAW = os.path.join(BASE_DIR, "data_lake", "raw", "hgnc")
DATA_LAKE_PROCESSED = os.path.join(BASE_DIR, "data_lake", "processed", "hgnc")

os.makedirs(DATA_LAKE_RAW, exist_ok=True)
os.makedirs(DATA_LAKE_PROCESSED, exist_ok=True)

HGNC_API_URL = "https://rest.genenames.org/fetch/all"
RAW_JSON_FILE = os.path.join(DATA_LAKE_RAW, "hgnc_data.json")
LAST_SIZE_FILE = os.path.join(DATA_LAKE_RAW, "last_size.txt")


def check_data_updated():
    """Verifica se os dados mudaram com base no tamanho do JSON."""
    try:
        response = requests.get(HGNC_API_URL, headers={"Accept": "application/json"})
        if response.status_code == 200:
            data_length = len(response.text)

            print(f"Tamanho atual dos dados: {data_length} bytes")

            last_saved_size = None
            if os.path.exists(LAST_SIZE_FILE):
                with open(LAST_SIZE_FILE, "r") as f:
                    last_saved_size = int(f.read().strip())

            if data_length == last_saved_size:
                print("Nenhuma mudança detectada nos dados. Pulando download.")
                return False

            with open(LAST_SIZE_FILE, "w") as f:
                f.write(str(data_length))

            return True

        print("Erro ao acessar a API HGNC. Forçando download por segurança.")
        return True

    except Exception as e:
        print(f"Erro durante a verificação: {e}")
        return True


def download_hgnc():
    """Realiza o download dos dados do HGNC."""
    print("Baixando dados do HGNC...")
    response = requests.get(HGNC_API_URL, headers={"Accept": "application/json"})
    if response.status_code == 200:
        with open(RAW_JSON_FILE, "w") as f:
            f.write(response.text)
        print(f"Dados salvos em {RAW_JSON_FILE}")
    else:
        raise Exception(f"Falha no download: {response.status_code}")


def convert_json_to_parquet():
    """Converte os dados JSON para formato Parquet."""
    with open(RAW_JSON_FILE, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data["response"]["docs"])

    parquet_file = os.path.join(DATA_LAKE_PROCESSED, "hgnc_data.parquet")
    df.to_parquet(parquet_file, index=False)
    print(f"Arquivo Parquet salvo em {parquet_file}")


def convert_json_to_csv():
    """Converte os dados JSON para formato CSV."""
    with open(RAW_JSON_FILE, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data["response"]["docs"])

    COLUMNS_TO_STRING = [
        "hgnc_id",
        "entrez_id",
        "ensembl_gene_id",
        "vega_id",
        "ucsc_id",
    ]
    for col in COLUMNS_TO_STRING:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    csv_file = os.path.join(DATA_LAKE_PROCESSED, "hgnc_data.csv")
    df.to_csv(csv_file, index=False)
    print(f"Arquivo CSV salvo em {csv_file}")


def run_hgnc_etl():
    print(f"\n🧬 Iniciando ETL para HGNC - {datetime.now().isoformat()}\n")
    if check_data_updated():
        download_hgnc()
        # convert_json_to_parquet()
        convert_json_to_csv()
    else:
        print("ETL ignorado pois não houve atualização.")
    print(f"\n✅ Processo ETL finalizado.\n")


if __name__ == "__main__":
    run_hgnc_etl()
