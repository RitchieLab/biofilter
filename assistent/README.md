# BF4 Assistant Kit

This folder contains a ready-to-use knowledge and instruction kit for a GPT-based assistant focused on Biofilter 4 (BF4).

Use this kit as the "ground truth" layer for user support on:

- CLI usage
- ETL operations
- reports and parameters
- setup and troubleshooting

## Folder Contents

- `assistant_system_prompt.md`
  - System prompt for the GPT assistant behavior.
- `assistant_context_manifest.yaml`
  - Source priority and ingestion policy for retrieval.
- `assistant_response_contract.md`
  - Output style and answer quality rules.
- `assistant_faq_seed.md`
  - High-value seed Q&A to bootstrap assistant memory.
- `assistant_eval_set.md`
  - Test prompts to evaluate assistant quality before release.
- `sync_to_openai_vector_store.py`
  - Script to upload selected context files to an OpenAI vector store.
- `OPENAI_SYNC.md`
  - Quick guide to run the sync script.
- `build_gpt_builder_bundle.py`
  - Generates a GPT Builder-ready knowledge bundle zip + manifest.
- `GPT_BUILDER_INSTRUCTIONS.md`
  - Step-by-step setup guide for ChatGPT GPT Builder.

## Recommended Usage

1. Ingest/index sources described in `assistant_context_manifest.yaml`.
2. Use `assistant_system_prompt.md` as the assistant system instructions.
3. Use `assistant_response_contract.md` as additional behavior constraints.
4. Load `assistant_faq_seed.md` into your knowledge base (or examples library).
5. Run `assistant_eval_set.md` as acceptance checks.

## Update Workflow

When BF4 changes:

1. Update docs first (`docs/source` and `biofilter_agents`).
2. Refresh retrieval index.
3. Regenerate/adjust FAQ and eval set.
4. Re-run eval prompts and compare results.

## Scope

This kit is intentionally practical and user-facing. It does not replace code-level source inspection when a question depends on implementation details.
