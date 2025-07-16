# biofilter/debug_runner.py

from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

# Instancia o sistema
bf = Biofilter(db_uri)

# Exemplos de uso:
print("🔍 Acessando Model Explorer...")
explorer = bf.model_explorer()
explorer.list_models()
explorer.describe_model("Gene")
df = explorer.example_query("Gene")
print(df.head())

# Exemplo de atualização de conflitos (quando for necessário)
# bf.update_conflicts(source_system=["HGNC"])

print("✅ Debug runner finalizado com sucesso.")
