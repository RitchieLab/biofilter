import os
import ast
import json
import requests
import pandas as pd
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.db.models.entity_models import EntityGroup
from biofilter.db.models.curation_models import (
    CurationConflict,
    ConflictStatus,
)  # noqa E501
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase


class DTP(DTPBase):
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

        self.API_URL = "https://reactome.org/download/current/"
        # self.file_name = ""
        self.files_to_download = [
            "ReactomePathways.txt",
            "ReactomePathwaysRelation.txt",
            "ReactomePathways.gmt.zip",
            "Ensembl2Reactome.txt",
            "UniProt2Reactome.txt",
        ]

        self.conflict_mgr = ConflictManager(session, logger)
            
    def extract(self, raw_dir: str, source_url: str, last_hash: str):
        """
        Downloads Reactome data. Uses the hash of 'ReactomePathways.txt' as reference.
        Only proceeds with full extraction if the hash has changed.
        """
        try:
            message = ""

            # Landing directory
            landing_path = os.path.join(
                raw_dir,
                self.datasource.source_system.name,
                self.datasource.name,
            )
            os.makedirs(landing_path, exist_ok=True)

            # Step 1: Download only the main file
            main_file = "ReactomePathways.txt"
            main_file_url = f"{self.API_URL}{main_file}"
            main_file_path = os.path.join(landing_path, main_file)

            self.logger.log(f"⬇️ Downloading main file: {main_file} ...", "INFO")
            response = requests.get(main_file_url)
            if response.status_code != 200:
                raise Exception(f"Failed to download {main_file}: status {response.status_code}")

            with open(main_file_path, "wb") as f:
                f.write(response.content)

            self.logger.log(f"✅ Main file saved to {main_file_path}", "INFO")

            # Step 2: Compute hash
            current_hash = compute_file_hash(main_file_path)

            # Step 3: Compare with last hash
            if current_hash == last_hash:
                message = f"⏭️ No change detected in {main_file}. Hash = {current_hash}"
                self.logger.log(message, "INFO")
                return False, message, current_hash  # Skip further downloads

            # Step 4: Download the remaining files
            for file_name in self.files_to_download:
                if file_name == main_file:
                    continue  # Already downloaded

                file_url = f"{self.API_URL}{file_name}"
                dest_path = os.path.join(landing_path, file_name)

                self.logger.log(f"⬇️ Downloading {file_name} ...", "INFO")
                response = requests.get(file_url)

                if response.status_code != 200:
                    raise Exception(f"Failed to download {file_name}: status {response.status_code}")

                with open(dest_path, "wb") as f:
                    f.write(response.content)

                self.logger.log(f"✅ Saved {file_name} to {dest_path}", "INFO")

            message = f"✅ All Reactome files downloaded to {landing_path}"
            return True, message, current_hash  # <- will be saved to ETLProcess

        except Exception as e:
            message = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(message, "ERROR")
            return False, message, None


"""

Perfeito! Vamos montar o **DTP (Data Transfer Processor)** do Reactome, seguindo os princípios do Biofilter 3R. Com base nas definições que você já consolidou, o DTP terá as seguintes responsabilidades:

---

## 📦 Objetivo do `DTPReactome`

* **Extrair** arquivos do Reactome (via HTTP)
* **Transformar** os dados:

  * Criar entidades únicas para cada pathway (`Entity`, `EntityName`, `Pathway`)
  * Associar genes existentes via `EntityRelation`
  * Armazenar a hierarquia entre pathways como relações entre entidades
* **Carregar** os dados com rastreabilidade via `DataSource` e controle via `ETLManager`

---

## 📁 Estrutura de arquivos

Você pode criar o arquivo:

```
biofilter/dtp/reactome_dtp.py
```

E a classe:

```python
class DTPReactome(DTPBase):
```

---

## 🔧 Componentes principais do DTP

### 1. `download()`

Baixa arquivos como:

```python
ReactomePathways.txt  
ReactomePathwaysRelation.txt  
ReactomePathways.gmt.zip  
Ensembl2Reactome.txt  
UniProt2Reactome.txt
```

Pode usar:

```python
self._download_http_file(url, local_path)
```

---

### 2. `load()`

Executa o pipeline completo com:

* `get_or_create_entity()` para cada pathway
* `EntityName` com `name_type='external_id'` e alias
* `Pathway(...)` com `reactome_id`, nome, `data_source_id`
* `EntityRelation` para gene → pathway
* `EntityRelation` para pathway hierarquia

---

## 🔁 Integração com infra do Biofilter 3R

* Usar `self.datasource` para referenciar o `DataSource` ativo
* Usar `self.logger` para logs detalhados
* Criar relacionamentos apenas com genes válidos (`Gene.entity_id`)
* Validar `species == Homo sapiens`

---

## ✅ Próximos passos

1. Deseja que eu gere um esqueleto completo do arquivo `reactome_dtp.py` com os métodos `download()` e `load()` preparados?
2. Você já quer começar pela ingestão de `ReactomePathways.txt` ou pelo `gmt.zip` com genes?

Podemos ir parte por parte. Qual deles prefere começar?
"""
