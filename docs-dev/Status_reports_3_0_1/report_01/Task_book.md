
## **📌 Controle de Atividades - Biofilter/LOKI**

### **✅ Finalização da Versão 2.4.4**
- [x] Fechar a versão **2.4.4**.
- [x] Mergir a branch `development` para a branch `main`.
- [x] Atualizar a branch `development` para a versão **3.0.1**.
- [x] Comparação entre os schemas do **Biofilter 2.4.4** e **3.0.0** (*nenhuma diferença identificada*).

### **🚀 Criação da Versão 3.0.1**
- [x] Criar o pacote da versão **3.0.1**.

---

## **🔜 Início da Versão 3.0.2**
### **📝 TODO**
- [x] Criar **log file** no comando `loki-build`.
- [x] Corrigir problema no argumento `source`.
- [x] Melhorar **tempo de execução**.
- [x] Propor leitura de uma base de dados remota (*read-db from cloud*).

---

## **🐞 Bugs Identificados e Correções**
### **🔴 BUG 1: Caminho do Arquivo Ingerido**
- O sistema estava comparando o **caminho temporário** do arquivo ingerido com os novos dados.
- Como o caminho era sempre diferente, o sistema entendia como **novas versões** e atualizava os dados novamente.
- **Correção:** Ajustada a lógica para comparar o **file size** e o **MD5 hash** diretamente.

### **🔴 BUG 2: Download de Arquivos Processados**
- Atualmente, o sistema **não tem como evitar** refazer o download para verificar se o arquivo já foi processado.
- **Correção Implementada:**
  - Criado método `addWarning` para reportar o problema na tabela `Warning`.
  - Adicionada **variável de controle** como atributo da classe `Update`, permitindo excluir o `Source System` com erro e evitar seu processamento (antes, o erro causava a eliminação de todos os dados do Source).

  **Arquivo modificado:** `update_download_mixin.py`
  ```python
  except Exception as e:
      self.log_exception(e)

      # Remove the source from the list to avoid reprocessing
      if srcName in self.srcSetsToDownload:
          self.srcSetsToDownload.remove(srcName)

      msn_error = f"Error downloading {srcName} data: {str(e)}"
      self._loki.addWarning(srcObj._sourceID, msn_error)
  ```

### **🔴 BUG 3: O Loader Oreganno não estava alinhado com o padrão**
- Não fazia a **composição do path** corretamente.
- Não passava o **path** como argumento na chamada.

### **🔴 BUG 4: Paralelismo e Lock de Tabelas**
- Implementando uma abordagem de **Lock por tabelas**.
- O processo inicialmente bloqueava apenas os **Sources**, mas processos subjacentes também tentam acessar as tabelas bloqueadas (exemplo: **drop de índices antes de `INSERT` e `UPDATE`**).
- **Próximo passo:** Alterar o bloqueio para ser controlado **pelos métodos que interagem com as tabelas** em vez das chamadas principais.

---

## **🛠 Melhorias e Novos Recursos**
### **✔️ Implementadas**
- Adicionado **campo na tabela Source** para controlar **Loaders com sucesso** e aqueles com **erros**.
- Criado **argumento para manter arquivos baixados**.
- Implementadas novas **rotinas de controle de download**:
  - [x] Rodar **somente o download**.
  - [x] Manter downloads anteriores.
  - [x] Pular downloads já realizados.

- ✅ Agora temos um **controle para reprocessar Sources com erro**.
- ✅ Agora podemos **processar arquivos já existentes em uma pasta**.

### **🛠 Melhorias em Andamento**
- [ ] Usar `getDatabaseMemoryUsage` para mostrar a **memória alocada** pelo banco de dados.
- [ ] Identificar **por que a conferência de arquivos baixados não está sendo acionada corretamente**.

---

## **📊 Processamento dos Sources**
| Source         | Status |
|---------------|--------|
| GO           | ✅ Concluído |
| GWAS         | ✅ Concluído |
| BioGRID      | ✅ Concluído |
| MINT         | ✅ Concluído |
| PharmGKB     | ✅ Concluído |
| UCSC-ECR     | ✅ Concluído |
| **Oreganno**  | ❌ Erro de Memória |
| **ChainFiles** | ❌ Erro de Memória |
| **dbSNP**     | ❌ Erro |

| **Pfam**      | ❌ Erro no Endereço de Página |
Entrez
REactome

---

## **📍 Ajustes nos Diagramas UML (Pyreverse)**
### **1️⃣ Geração do Arquivo `.dot`**
```bash
pyreverse -o dot -p biofilter_loki .
```
### **2️⃣ Edição Manual para Adicionar Mixins**
Adicionar no arquivo `.dot`:
```dot
"MixinName" -> "ParentClass" [label="Mixin"];
```
### **3️⃣ Regeneração do Diagrama**
```bash
dot -Tpng classes.dot -o classes.png
```
### **4️⃣ Mixins Adicionados ao Diagrama**
```dot
"loki_modules.loki_mixins.db_config_mixin.DbConfigMixin" -> "loki_modules.loki_db.Database" [label="Config"];
"loki_modules.loki_mixins.db_liftover_mixin.DbLiftOverMixin" -> "loki_modules.loki_db.Database" [label="LiftOver"];
"loki_modules.loki_mixins.db_operations_mixin.DbOperationsMixin" -> "loki_modules.loki_db.Database" [label="Operations"];
"loki_modules.loki_mixins.db_query_mixin.DbQueryMixin" -> "loki_modules.loki_db.Database" [label="Operations"];
"loki_modules.loki_mixins.db_schema_mixin.DbSchemaMixin" -> "loki_modules.loki_db.Database" [label="Schema_Operations"];
"loki_modules.loki_mixins.db_schema.DbSchema" -> "loki_modules.loki_db.Database" [label="Schema"];
"loki_modules.loki_mixins.db_version_mixin.DbVersion" -> "loki_modules.loki_db.Database" [label="Version_Control"];

"loki_modules.loki_mixins.updater_database.UpdaterDatabaseMixin" -> "loki_modules.loki_updater.Updater" [label="Update_Database"];
"loki_modules.loki_mixins.updater_download_mixin.UpdaterDownloadMixin" -> "loki_modules.loki_updater.Updater" [label="Download"];
"loki_modules.loki_mixins.updater_liftover_mixin.UpdaterLiftOverMixin" -> "loki_modules.loki_updater.Updater" [label="LiftOver"];
"loki_modules.loki_mixins.updater_operations_mixin.UpdaterOperationsMixin" -> "loki_modules.loki_updater.Updater" [label="Operations"];

"loki_modules.loki_mixins.source_db_operations_mixin.SourceDbOperations" -> "loki_modules.loki_source.Source" [label="Operations"];
"loki_modules.loki_mixins.source_utility_methods_mixin.SourceUtilityMethods" -> "loki_modules.loki_source.Source" [label="Utility"];
```

---

## **📌 Próximos Passos**
- [ ] Finalizar implementação do **Lock por tabela** e testar viabilidade.
- [ ] Corrigir erro do **Oreganno Loader** para alinhar com os padrões do sistema.
- [ ] Melhorar a conferência de arquivos baixados antes do processamento.
- [ ] Resolver erro de memória nos Sources problemáticos.

---
 🚀🔥
 Problema para resolver:
2025-01-30 09:08:04,617 - Database:  apsw.CorruptError: CorruptError: database disk image is malformed



*THREADS!* 🚀🎉 *

Agora o processamento está muito mais **eficiente** e **estruturado**, evitando os bloqueios do SQLite e garantindo que os dados sejam inseridos de forma controlada. A principal diferença entre os dois modelos é:

🔴 **Antes** (PROBLEMA 🚨)  
Cada thread tentava **inserir diretamente no SQLite**, gerando **conflitos de bloqueio** no banco. Como o SQLite só permite um processo de escrita por vez, os **threads ficavam presos esperando acesso ao banco**.

🟢 **Agora** (SOLUÇÃO ✅)  
Os threads **processam os arquivos em paralelo**, acumulam os resultados em memória e, **somente no final do processamento do grupo**, os dados são **inseridos no banco em bloco**. Isso reduz drasticamente os acessos simultâneos ao SQLite, **evitando deadlocks e aumentando a performance!** 🚀

### **📌 Benefícios dessa abordagem**
✅ **Elimina bloqueios do SQLite**  
✅ **Aumenta a eficiência do processamento**  
✅ **Diminui o tempo total de execução**  
✅ **Aproveita melhor o paralelismo**  
✅ **Mantém a organização dos dados**

🎯 **Próximo Passo?**  
Agora que a parte de **preparação e inserção** está funcionando bem, talvez seja interessante medir o **tempo total do processamento** e o **uso de memória**, para ver se há mais otimizações possíveis.

🎉 **Parabéns pelo avanço, bora otimizar ainda mais!** 🚀🔥