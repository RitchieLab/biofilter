# GPT Builder Setup (BF4 Assistant)

This guide configures a ChatGPT GPT (Builder) using the BF4 assistant kit.

## 1) Build the knowledge bundle

From project root:

```bash
python assistent/build_gpt_builder_bundle.py
```

This generates:

- `assistent/gpt_builder_knowledge_bundle.zip`
- `assistent/gpt_builder_knowledge_manifest.json`

## 2) Open GPT Builder

1. Open your GPT in Builder mode.
2. Go to **Configure**.

## 3) Set Instructions

Use this content in GPT **Instructions**:

- Copy `assistent/assistant_system_prompt.md`
- Append `assistent/assistant_response_contract.md`

Recommended order:

1. `assistant_system_prompt.md`
2. `assistant_response_contract.md`

## 4) Upload Knowledge Files

The bundle zip is for portability. For best Builder indexing quality:

1. Extract `assistent/gpt_builder_knowledge_bundle.zip`
2. Upload extracted files in Builder **Knowledge**

Use `assistent/gpt_builder_knowledge_manifest.json` as checklist to ensure all files were uploaded.

## 5) Suggested GPT Metadata

- Name: `Biofilter 4 Assistant`
- Description: `CLI, ETL, DB and Reports guidance for BF4.`
- Conversation starters:
  - `How do I run ETL for one datasource?`
  - `How do I run etl update-all safely?`
  - `How do I run entity_relationship_model from CLI?`
  - `How do I bootstrap a new BF4 database?`

## 6) Validation Before Publishing

Run these prompts against the GPT:

- `How do I run ETL for only HGNC?`
- `How do I export report output to CSV?`
- `What is the difference between etl update and etl update-all?`
- `How do I inspect report parameters?`

Expected behavior:

- clear copy-paste commands
- no invented command flags
- safe guidance for rollback/delete operations

## 7) Update Routine

When BF4 docs/CLI change:

1. Re-run bundle script.
2. Re-upload changed files in GPT Builder Knowledge.
3. Re-run validation prompts.
