# BF4 Assistant Kit

A ready-to-use knowledge and instruction kit for a **ChatGPT (GPT Builder)**
assistant focused on Biofilter 4 (BF4).

## Audience

This assistant is for **end users** — researchers and analysts who use BF4 to:

1. **run reports** and get results (CSV),
2. **update the database** (ETL),
3. **install / create a new database** — either by pointing at a shared
   **Parquet bundle** (`parquet://`, read-only, no server) or by building their
   own PostgreSQL/SQLite database with migrations + ETL.

It is **not** a developer assistant. The knowledge base intentionally excludes
Python source code; grounding comes from user docs, operational guides,
per-report explain docs, and runnable notebook examples.

## Folder contents

- `assistant_system_prompt.md` — system prompt (assistant behavior + personas).
- `assistant_response_contract.md` — output style and answer-quality rules.
- `assistant_context_manifest.yaml` — source selection and retrieval policy.
- `assistant_faq_seed.md` — curated, high-signal support Q&A.
- `assistant_eval_set.md` — acceptance prompts to validate quality before release.
- `sync_to_openai_vector_store.py` — upload manifest-selected files to an
  OpenAI vector store (File Search).
- `OPENAI_SYNC.md` — how to run the sync script.
- `gpt_builder/` — GPT Builder setup:
  - `build_gpt_builder_bundle.py` — generate a knowledge bundle zip + manifest.
  - `GPT_BUILDER_INSTRUCTIONS.md` — step-by-step ChatGPT GPT Builder setup.
  - `gpt_builder_knowledge_bundle.zip` / `gpt_builder_knowledge_manifest.json` —
    generated artifacts (do not edit by hand; regenerate with the script).

## What goes into the knowledge base

Selected by `assistant_context_manifest.yaml` (and mirrored by the GPT Builder
bundle script), **highest to lowest priority**:

1. `docs/source/` — official user docs (dev-only pages excluded).
2. `biofilter_agents/` — operational task guides.
3. `biofilter/modules/report/reports_explain/` — per-report usage docs.
4. `notebooks/Templates/` — runnable examples (incl. the LPC quickstart).
5. `assistant_faq_seed.md` — curated support answers.

Python source files (`**/*.py`) are excluded on purpose.

## Recommended usage

1. Review sources in `assistant_context_manifest.yaml`.
2. Use `assistant_system_prompt.md` as the assistant's system instructions.
3. Append `assistant_response_contract.md` as additional behavior constraints.
4. Load the knowledge files (via GPT Builder bundle or the OpenAI sync script).
5. Run `assistant_eval_set.md` as acceptance checks before publishing.

## Update workflow

When BF4 changes (new report, new data source, CLI flag change, DB backend
change such as `parquet://`, or docs updates):

1. Update the underlying docs first (`docs/source`, `biofilter_agents`,
   `reports_explain`).
2. Refresh `assistant_faq_seed.md` and `assistant_eval_set.md` if behavior
   changed.
3. Regenerate the knowledge base:
   - GPT Builder: `python assistent/gpt_builder/build_gpt_builder_bundle.py`
   - OpenAI vector store: see `OPENAI_SYNC.md`
4. Re-run the eval prompts and compare results.

## Scope

This kit is deliberately practical and user-facing. It does not answer
implementation/source-code questions — those defer to the maintainer or the
project repository.
