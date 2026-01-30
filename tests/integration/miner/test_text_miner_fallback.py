"""
Docstring for tests.integration.text_miner.test_text_miner_fallback

3) Testes de integração – Fallback (SQLite ou Postgres)

Crie tests/integration/test_text_miner_fallback.py.

Como o fallback depende do resolver, você testa principalmente:

se consegue encontrar tokens relevantes no texto

se o cache não quebra

se o overlap cleanup reduz duplicatas

Casos:

texto curto com 2–3 entidades conhecidas

texto com repetições (“A1BG … A1BG …”) → retorna 2 spans diferentes ou dedupa (dependendo do seu postprocess_mentions)
"""

