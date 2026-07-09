# GPT Builder Setup (BF4 Assistant)

This guide configures a ChatGPT GPT (Builder) using the BF4 assistant kit.
The assistant targets **end users** (researchers/analysts), not developers.

## 1) Build the knowledge files

From the project root:

```bash
python assistent/gpt_builder/build_gpt_builder_bundle.py
```

This generates (inside `assistent/gpt_builder/`):

- `consolidated/` — **5 merged Markdown files** (`bf4_docs.md`, `bf4_agents.md`,
  `bf4_reports_explain.md`, `bf4_notebooks.md`, `bf4_faq.md`). **Upload these
  to the GPT.** ChatGPT custom GPTs cap Knowledge at 20 files, so the 72 source
  files are merged into 5 (each embedded file keeps a `SOURCE FILE:` provenance
  header so the assistant can still cite where a passage came from).
- `gpt_builder_knowledge_bundle.zip` / `gpt_builder_knowledge_manifest.json` —
  the full 72-file set (useful for the API vector-store path; not for GPT
  Builder because of the 20-file cap).

All content is user docs, operational guides, per-report explain docs, notebook
examples (converted to Markdown), and the FAQ seed — **no Python source code**.

## 2) Open GPT Builder

1. Open your GPT in Builder mode.
2. Go to **Configure**.

## 3) Set Instructions

Use this content in GPT **Instructions**, in this order:

1. `assistent/assistant_system_prompt.md`
2. `assistent/assistant_response_contract.md`

## 4) Upload Knowledge Files

In Builder **Knowledge**, upload the 5 files from
`assistent/gpt_builder/consolidated/`:

- `bf4_docs.md`
- `bf4_agents.md`
- `bf4_reports_explain.md`
- `bf4_notebooks.md`
- `bf4_faq.md`

That is the whole knowledge base (72 source files merged into 5), comfortably
under ChatGPT's 20-file Knowledge limit.

## 5) Suggested GPT Metadata

- Name: `Biofilter 4 Assistant`
- Description: `Run BF4 reports, connect to data, and set up a database — no coding needed.`
- Conversation starters:
  - `I have a list of genes — how do I annotate them into a CSV?`
  - `I just want to run reports against a shared snapshot, no install.`
  - `How do I create a new BF4 database and load data?`
  - `Which reports are available and what inputs do they take?`

## 6) Validation Before Publishing

Run these prompts against the GPT:

- `How do I annotate a list of genes and save a CSV?`
- `How do I point BF4 at a Parquet bundle to run reports?`
- `What is the difference between etl update and etl update-all?`
- `How do I bootstrap a new database from scratch?`

Expected behavior:

- clear copy-paste commands
- no invented reports, commands, or flags
- safe guidance for rollback/delete operations (with a caution note)
- defers implementation/source-code questions to the maintainer/repo

You can also run the full `assistent/assistant_eval_set.md` prompts.

## 7) Update Routine

When BF4 docs/CLI/data change:

1. Re-run the bundle script.
2. Re-upload changed files in GPT Builder Knowledge.
3. Re-run validation prompts.
