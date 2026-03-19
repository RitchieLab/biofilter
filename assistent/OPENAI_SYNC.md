# Sync BF4 Context to OpenAI Vector Store

This guide automates uploading BF4 assistant context files to OpenAI File Search (vector store).

## Important

The ChatGPT GPT link (for example `https://chatgpt.com/g/...`) is not an API endpoint for automated knowledge sync.

Use this script for API-based assistants and retrieval workflows.

## Script

- `assistent/sync_to_openai_vector_store.py`

It reads:

- `assistent/assistant_context_manifest.yaml`

and uploads selected files to an OpenAI vector store.

## Prerequisites

1. Environment variable:

```bash
export OPENAI_API_KEY="your_api_key"
```

2. Python deps:

```bash
pip install requests pyyaml
```

(`requests` is already present in this project; `pyyaml` may need install.)

## Dry Run (no API calls)

```bash
python assistent/sync_to_openai_vector_store.py --dry-run --print-files
```

## Create New Vector Store and Upload

```bash
python assistent/sync_to_openai_vector_store.py --vector-store-name "BF4 Assistant Knowledge"
```

## Upload to Existing Vector Store

```bash
python assistent/sync_to_openai_vector_store.py --vector-store-id vs_1234567890
```

## Restrict Upload to Specific Sources

Source IDs come from `assistant_context_manifest.yaml` (`agents`, `docs`, `cli`, `etl`, `reports`, `notebooks`):

```bash
python assistent/sync_to_openai_vector_store.py --vector-store-id vs_123 --source agents --source docs
```

## Test with Small Batch

```bash
python assistent/sync_to_openai_vector_store.py --vector-store-id vs_123 --max-files 20 --print-files
```

## Output

The script prints:

- vector store ID used/created
- each uploaded file id
- file batch ingestion status
- completed/failed ingestion counts

## Suggested Workflow

1. Update docs and agent guides.
2. Run dry-run and review selected files.
3. Sync to vector store.
4. Run `assistent/assistant_eval_set.md` prompts to validate assistant quality.
