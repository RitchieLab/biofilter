# GPT Builder Setup (BF4 Assistant)

Configuring the ChatGPT GPT from this kit.

The assistant serves people who were **given a bundle** and want an answer out
of it. Building a bundle and running the ETL are deliberate deferrals — see
`../README.md` for why.

## What goes where

Two different things, and mixing them up is the common mistake:

| Kit file | Goes into |
|---|---|
| `assistant_system_prompt.md` | GPT **Instructions** box |
| `assistant_response_contract.md` | GPT **Instructions** box, appended after the prompt |
| `consolidated/*.md` (5 files) | GPT **Knowledge** (uploaded files) |
| everything else in `assistent/` | **nowhere** — manifest, eval set and scripts are tooling, not content |

The knowledge base is not the `assistent/` folder. It is the five merged files
the build script produces from across the repository.

## 1) Build the knowledge files

From the project root:

```bash
python assistent/gpt_builder/build_gpt_builder_bundle.py
```

This reads `assistent/assistant_context_manifest.yaml` — the single source of
truth for what the assistant knows — and writes into `assistent/gpt_builder/`:

- `consolidated/` — **the 5 files you upload**: `bf4_docs.md`,
  `bf4_agents.md`, `bf4_reports_explain.md`, `bf4_notebooks.md`, `bf4_faq.md`.
  ChatGPT caps Knowledge at 20 files, so the source files are merged into
  five; each embedded file keeps a `SOURCE FILE:` header so the assistant can
  still say where a passage came from.
- `gpt_builder_knowledge_bundle.zip` and its manifest — the full unmerged set,
  for the API vector-store path. Not for GPT Builder.

Watch the output for `warning: source '<id>' matched no files`. That means a
path moved, and a silently empty source is how the LPC quickstart once left
the knowledge base without anyone noticing.

## 2) Set Instructions

In **Configure → Instructions**, paste in this order:

1. the whole of `assistent/assistant_system_prompt.md`
2. then the whole of `assistent/assistant_response_contract.md`

Replace what is there; do not append to the previous version. These are
instructions, and an old line contradicting a new one does not get resolved by
the knowledge base — the model just follows whichever it reads as more
specific.

## 3) Replace the Knowledge files

**Delete every existing Knowledge file first, then upload the five.**

This matters more than it looks. ChatGPT does not reliably replace an uploaded
file by name, so re-uploading over an old set can leave both copies in
retrieval. And any file from an older kit whose name is no longer produced
would stay forever — still retrievable, still describing 4.2.x, with nothing
to indicate it is stale.

Then upload, from `assistent/gpt_builder/consolidated/`:

- `bf4_docs.md`
- `bf4_agents.md`
- `bf4_reports_explain.md`
- `bf4_notebooks.md`
- `bf4_faq.md`

## 4) GPT metadata

- **Name:** `Biofilter 4 Assistant`
- **Description:** `Run Biofilter reports against a bundle and read the
  results — no coding needed.`
- **Conversation starters:**
  - `I have a list of genes — how do I annotate them into a CSV?`
  - `Someone gave me a bundle. How do I point Biofilter at it?`
  - `Which variants in these genes are likely damaging?`
  - `My report came back empty — what does that mean?`

The last starter is deliberate. Telling `not_found` from `no_variants` from a
source that was never built is the question users most need answered and least
think to ask.

## 5) Validate before publishing

Run `assistent/assistant_eval_set.md` — twenty prompts written against the
current CLI, with a failure-signal list naming the mistakes this assistant has
actually made.

A fast subset if you only have a minute:

| Prompt | Must not |
|---|---|
| `How do I point Biofilter at a bundle?` | suggest a `parquet://` URI, or point at `tables/` |
| `Which reports can I run?` | name `entity_filter`, `etl_status`, `annotation_master_*` or any other 4.2.x name |
| `How do I create a database and load data?` | walk through it — it should defer, with the cost |
| `I have genes and want the damaging variants in them` | miss the `mapping` choice in `expand_gene_to_variant` |
| `Can I use these entity ids with another bundle?` | say yes |

## 6) Update routine

When reports, the CLI or the docs change:

1. Fix the underlying docs first — the manifest points at them.
2. Re-run the build script.
3. **Delete the Knowledge files and re-upload**, per §3.
4. Re-paste Instructions if the prompt or contract changed.
5. Re-run the eval set.
