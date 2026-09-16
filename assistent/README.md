# BF4 Assistant Kit

Knowledge and instruction kit for a ChatGPT assistant that helps people use
Biofilter 4.

## Audience

The assistant serves **someone who was given a bundle and wants an answer out
of it**: a gene list annotated, variants filtered, a cohort matched. That is
the whole job.

Building a bundle and running the ETL are **out of scope** by design. Both are
maintainer tasks — a full build needs about 150 GB of working space and two
days — and an assistant that walks a scientist through a two-day pipeline they
did not ask for has failed, however accurate the steps. The correct behaviour
there is to name the guide, state the cost, and defer.

Source code is excluded for a related reason: the audience is not developers.

## Folder contents

| File | What it is |
|---|---|
| `assistant_system_prompt.md` | the assistant's instructions |
| `assistant_response_contract.md` | answer-quality policy, layered on top |
| `assistant_context_manifest.yaml` | **the single source of truth** for what goes into the knowledge base |
| `assistant_faq_seed.md` | curated, high-signal answers |
| `assistant_eval_set.md` | acceptance prompts — run before publishing |
| `sync_to_openai_vector_store.py` | upload to an OpenAI vector store (File Search) |
| `OPENAI_SYNC.md` | how to run that script |
| `gpt_builder/` | GPT Builder path: bundle script, instructions, generated artifacts |

Both delivery paths — the vector store and the GPT Builder bundle — read
`assistant_context_manifest.yaml`. Change the selection there and nowhere
else.

## What goes into the knowledge base

Selected by the manifest, highest to lowest priority:

1. `docs/source/` — the user-facing pages. Most of `technical/` is excluded;
   the exceptions are the pages the assistant needs in order to *defer*
   accurately rather than vaguely.
2. `biofilter_agents/ag_start.md`, `ag_report_en.md` — the two operational
   guides written for bundle readers. `ag_db_en.md` and `ag_etl_en.md` are
   maintainer material and stay out.
3. `biofilter/modules/report/reports_explain/` — per-report truth: parameters,
   accepted inputs, output columns. Prefer these over anything else when they
   disagree about a specific report.
4. `notebooks/templates/*.ipynb` and `notebooks/lpc__quickstart.md` — runnable
   examples.
5. `assistant_faq_seed.md` — curated answers.

Excluded everywhere: `**/*.py`, and `biofilter_legacy/**` — the 4.2.x snapshot
is correct for its own release and wrong for this one, which makes it the
likeliest source of a confidently outdated answer.

## Update workflow

1. **Fix the underlying docs first.** The manifest points at them; most
   staleness is fixed there, not here.
2. Refresh `assistant_faq_seed.md` and `assistant_eval_set.md` if behaviour
   changed.
3. Regenerate:
   - GPT Builder: `python assistent/gpt_builder/build_gpt_builder_bundle.py`
   - Vector store: see `OPENAI_SYNC.md`
4. **Run `assistant_eval_set.md` and compare answers before publishing.**

Step 4 is the one that catches regressions. The eval set is written against
the current CLI and names the failure modes this assistant has actually
produced — recommending a report that no longer exists, presenting
`parquet://` as the way to point at data, inventing migrations.

## A note on renamed reports

Several reports were renamed in 4.3.0, and the explain guides say so — for
example `report_resolve_entity.md` opens with *"Called `entity_filter` before
4.3.0"*. Those redirects are deliberate: someone following an older note has
to land somewhere.

They are also a retrieval hazard. A chunk containing the old name can be
retrieved without the sentence that retires it, so the instructions carry an
explicit rule — verify any report name against `biofilter report list` before
recommending it — and the eval set lists the dead names as failure signals.
Do not remove the redirects; keep the guardrail.

## Scope boundary

This kit does not answer implementation questions. Those defer to the
maintainer or the repository.
