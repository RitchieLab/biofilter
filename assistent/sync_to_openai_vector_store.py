#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import io
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass
class SourceSpec:
    source_id: str
    path: Path
    include_globs: list[str]
    exclude_globs: list[str]


def _load_manifest(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Manifest file not found: {path}")

    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")
    if suffix == ".json":
        return json.loads(text)

    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "YAML manifest requires PyYAML. Install with: pip install pyyaml"
        ) from exc
    return yaml.safe_load(text) or {}


def _to_posix_relative(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def ipynb_to_markdown(path: Path) -> str:
    """
    Convert a Jupyter notebook to plain Markdown for retrieval.

    OpenAI File Search does not accept the .ipynb extension, and raw notebook
    JSON is noisy (base64 outputs, metadata). This emits markdown cells as-is
    and code cells fenced as ```python, dropping outputs — clean and
    high-signal for RAG.
    """
    nb = json.loads(path.read_text(encoding="utf-8"))
    blocks: list[str] = []
    for cell in nb.get("cells", []):
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        src = src.rstrip()
        if not src:
            continue
        if cell.get("cell_type") == "code":
            blocks.append(f"```python\n{src}\n```")
        else:  # markdown / raw
            blocks.append(src)
    return "\n\n".join(blocks) + "\n"


def _match_any_glob(rel_path: str, globs: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(rel_path, pattern) for pattern in globs)


def _collect_source_specs(manifest: dict, repo_root: Path) -> list[SourceSpec]:
    specs: list[SourceSpec] = []
    for item in manifest.get("source_priority", []):
        source_path = repo_root / str(item.get("path", "")).strip()
        if not source_path.exists():
            continue
        specs.append(
            SourceSpec(
                source_id=str(item.get("id", "")).strip() or source_path.name,
                path=source_path,
                include_globs=list(item.get("include_globs", ["**/*"])),
                exclude_globs=list(item.get("exclude_globs", [])),
            )
        )
    return specs


def _iter_files_for_source(spec: SourceSpec) -> Iterable[Path]:
    seen: set[Path] = set()
    for pattern in spec.include_globs:
        for candidate in spec.path.glob(pattern):
            if candidate.is_file() and candidate not in seen:
                seen.add(candidate)
                yield candidate


def collect_files(
    *,
    manifest: dict,
    repo_root: Path,
    include_sources: set[str] | None = None,
    max_files: int | None = None,
) -> list[Path]:
    global_exclusions = list(manifest.get("global_exclusions", []))
    specs = _collect_source_specs(manifest, repo_root)

    out: list[Path] = []
    seen: set[Path] = set()

    for spec in specs:
        if include_sources and spec.source_id not in include_sources:
            continue

        for file_path in _iter_files_for_source(spec):
            rel_path = _to_posix_relative(file_path, repo_root)

            if _match_any_glob(rel_path, global_exclusions):
                continue
            if spec.exclude_globs and _match_any_glob(rel_path, spec.exclude_globs):
                continue
            if file_path in seen:
                continue

            seen.add(file_path)
            out.append(file_path)
            if max_files and len(out) >= max_files:
                return out
    return out


class OpenAIClient:
    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1"):
        try:
            import requests  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "Missing dependency 'requests'. Install with: pip install requests"
            ) from exc

        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                # Vector store / file-batch endpoints live under the Assistants
                # beta surface; harmless once they are GA.
                "OpenAI-Beta": "assistants=v2",
            }
        )

    def _parse(self, response: requests.Response) -> dict:
        """
        Validate an OpenAI response and return its JSON body.

        Surfaces the real HTTP status, content-type and a body snippet when
        the response is an error OR is not JSON (e.g. an HTML page injected by
        a corporate proxy, or an empty body), instead of a bare
        JSONDecodeError.
        """
        ctype = response.headers.get("content-type", "")
        body = response.text or ""
        snippet = body.strip().replace("\n", " ")[:400] or "<empty body>"

        if response.status_code >= 400:
            try:
                message = response.json().get("error", {}).get("message", snippet)
            except Exception:
                message = snippet
            raise RuntimeError(
                f"OpenAI API error {response.status_code} "
                f"(content-type: {ctype or 'n/a'}): {message}"
            )

        if "application/json" not in ctype.lower():
            raise RuntimeError(
                f"Expected JSON but got status {response.status_code} "
                f"content-type '{ctype or 'n/a'}'. Body: {snippet}\n"
                "This usually means a proxy/gateway intercepted the request "
                "(check REQUESTS_CA_BUNDLE / network) rather than OpenAI."
            )

        try:
            return response.json()
        except Exception as exc:
            raise RuntimeError(
                f"Malformed JSON from OpenAI (status {response.status_code}). "
                f"Body: {snippet}"
            ) from exc

    def create_vector_store(self, name: str) -> str:
        resp = self.session.post(
            f"{self.base_url}/vector_stores",
            json={"name": name},
            timeout=120,
        )
        return self._parse(resp)["id"]

    def upload_file(self, path: Path, purpose: str = "assistants") -> str:
        # .ipynb is not an accepted File Search extension: upload a converted
        # markdown rendering instead, keeping the origin visible in the name.
        if path.suffix.lower() == ".ipynb":
            payload = ipynb_to_markdown(path).encode("utf-8")
            file_tuple = (f"{path.stem}.ipynb.md", io.BytesIO(payload))
            resp = self.session.post(
                f"{self.base_url}/files",
                data={"purpose": purpose},
                files={"file": file_tuple},
                timeout=300,
            )
            return self._parse(resp)["id"]

        with path.open("rb") as f:
            resp = self.session.post(
                f"{self.base_url}/files",
                data={"purpose": purpose},
                files={"file": (path.name, f)},
                timeout=300,
            )
        return self._parse(resp)["id"]

    def create_file_batch(self, vector_store_id: str, file_ids: list[str]) -> str:
        resp = self.session.post(
            f"{self.base_url}/vector_stores/{vector_store_id}/file_batches",
            json={"file_ids": file_ids},
            timeout=120,
        )
        return self._parse(resp)["id"]

    def get_file_batch(self, vector_store_id: str, batch_id: str) -> dict:
        resp = self.session.get(
            f"{self.base_url}/vector_stores/{vector_store_id}/file_batches/{batch_id}",
            timeout=120,
        )
        return self._parse(resp)


def _chunk(values: list[str], n: int) -> Iterable[list[str]]:
    for i in range(0, len(values), n):
        yield values[i : i + n]


def _poll_batch(
    client: OpenAIClient,
    *,
    vector_store_id: str,
    batch_id: str,
    interval_sec: float,
    timeout_sec: int,
) -> dict:
    started = time.time()
    while True:
        batch = client.get_file_batch(vector_store_id, batch_id)
        status = str(batch.get("status", "")).lower()
        if status in {"completed", "failed", "cancelled"}:
            return batch
        if time.time() - started > timeout_sec:
            raise TimeoutError(
                f"Timeout waiting for file batch {batch_id} after {timeout_sec}s."
            )
        time.sleep(interval_sec)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync BF4 knowledge files to an OpenAI vector store."
    )
    parser.add_argument(
        "--manifest",
        default="assistent/assistant_context_manifest.yaml",
        help="Path to context manifest (.yaml or .json).",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root used to resolve source paths.",
    )
    parser.add_argument(
        "--vector-store-id",
        help="Existing vector store ID (vs_...). If omitted, a new one is created.",
    )
    parser.add_argument(
        "--vector-store-name",
        default="BF4 Assistant Knowledge",
        help="Name for a new vector store when --vector-store-id is not provided.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Restrict sync to one source id from manifest (repeatable).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Limit number of files to upload (for testing).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of file_ids per vector store file batch (<=500 recommended).",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Polling interval in seconds for batch status.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1800,
        help="Polling timeout in seconds per batch.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only resolve and print files; do not call the API.",
    )
    parser.add_argument(
        "--print-files",
        action="store_true",
        help="Print each selected file path.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    api_key = os.getenv("OPENAI_API_KEY")
    if not args.dry_run and not api_key:
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        return 2

    manifest_path = Path(args.manifest)
    repo_root = Path(args.repo_root).resolve()

    try:
        manifest = _load_manifest(manifest_path)
    except Exception as exc:
        print(f"ERROR loading manifest: {exc}", file=sys.stderr)
        return 2

    include_sources = set(args.source) if args.source else None
    files = collect_files(
        manifest=manifest,
        repo_root=repo_root,
        include_sources=include_sources,
        max_files=args.max_files,
    )

    if not files:
        print("No files selected from manifest. Nothing to upload.")
        return 0

    print(f"Selected {len(files)} files from manifest.")
    if args.print_files:
        for path in files:
            rel = _to_posix_relative(path, repo_root)
            print(f"- {rel}")

    if args.dry_run:
        print("Dry run completed. No API calls were made.")
        return 0

    client = OpenAIClient(api_key=api_key)
    vector_store_id = args.vector_store_id or client.create_vector_store(
        args.vector_store_name
    )
    print(f"Using vector store: {vector_store_id}")

    uploaded_ids: list[str] = []
    for index, path in enumerate(files, start=1):
        rel = _to_posix_relative(path, repo_root)
        file_id = client.upload_file(path)
        uploaded_ids.append(file_id)
        print(f"[{index}/{len(files)}] uploaded {rel} -> {file_id}")

    total_completed = 0
    total_failed = 0
    for chunk_ids in _chunk(uploaded_ids, max(1, min(args.batch_size, 500))):
        batch_id = client.create_file_batch(vector_store_id, chunk_ids)
        print(f"Created file batch {batch_id} with {len(chunk_ids)} files. Polling...")
        batch = _poll_batch(
            client,
            vector_store_id=vector_store_id,
            batch_id=batch_id,
            interval_sec=args.poll_interval,
            timeout_sec=args.timeout,
        )
        counts = batch.get("file_counts", {}) or {}
        completed = int(counts.get("completed", 0))
        failed = int(counts.get("failed", 0))
        total_completed += completed
        total_failed += failed
        print(
            f"Batch {batch_id} status={batch.get('status')} "
            f"(completed={completed}, failed={failed})"
        )

    print("Sync finished.")
    print(f"Vector store ID: {vector_store_id}")
    print(f"Uploaded files: {len(uploaded_ids)}")
    print(f"Ingest completed: {total_completed}")
    print(f"Ingest failed: {total_failed}")
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
