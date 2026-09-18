# TODO: Create scripts to migrate tests

# # ver status
# biofilter db migrate --status

# # rodar upgrade normal (default)
# biofilter db migrate

# # ver SQL que rodaria (sem executar)
# biofilter db migrate --dry-run

# # “baseline”: marcar o DB como head sem executar DDL
# biofilter db migrate --stamp-head

# # stamp perigoso (só se você souber o que está fazendo)
# biofilter db migrate --stamp-head --force

# # migrar para um revision específico
# biofilter db migrate --target 35bc63e8d681
