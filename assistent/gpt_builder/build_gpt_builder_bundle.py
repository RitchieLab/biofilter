#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def _default_sources() -> list[dict]:
    return [
        {"id": "agents", "path": "biofilter_agents", "ext": {".md"}},
        {"id": "docs", "path": "docs/source", "ext": {".md"}},
        {"id": "cli", "path": "biofilter/api/cli", "ext": {".py", ".md"}},
        {"id": "etl", "path": "biofilter/modules/etl", "ext": {".py", ".md"}},
        {"id": "reports", "path": "biofilter/modules/report", "ext": {".py", ".md"}},
        {"id": "notebooks", "path": "notebooks/Templates", "ext": {".ipynb", ".md"}},
    ]


def _collect_files(repo_root: Path) -> list[Path]:
    exclusions = [
        "**/.git/**",
        "**/.venv/**",
        "**/__pycache__/**",
        "**/*.log",
        "**/docs/build/**",
        "**/AGENTS.md",
        "**/reports_bkp/**",
        "**/.ipynb_checkpoints/**",
        "**/.DS_Store",
    ]

    files: list[Path] = []
    seen: set[Path] = set()
    for src in _default_sources():
        base = repo_root / src["path"]
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in src["ext"]:
                continue
            rel = p.resolve().relative_to(repo_root.resolve()).as_posix()
            if any(fnmatch.fnmatch(rel, pat) for pat in exclusions):
                continue
            if p in seen:
                continue
            seen.add(p)
            files.append(p)

    # Include FAQ seed as a compact high-signal support source.
    faq = repo_root / "assistent/assistant_faq_seed.md"
    if faq.exists() and faq not in seen:
        files.append(faq)

    return sorted(files)


def _write_manifest(files: list[Path], repo_root: Path, output_json: Path) -> None:
    payload = {
        "count": len(files),
        "files": [
            p.resolve().relative_to(repo_root.resolve()).as_posix() for p in files
        ],
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_zip(files: list[Path], repo_root: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as zf:
        for p in files:
            arcname = p.resolve().relative_to(repo_root.resolve()).as_posix()
            zf.write(p, arcname=arcname)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build GPT Builder knowledge bundle zip from BF4 sources."
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Project root directory.",
    )
    parser.add_argument(
        "--zip-path",
        default="assistent/gpt_builder_knowledge_bundle.zip",
        help="Output zip path.",
    )
    parser.add_argument(
        "--manifest-path",
        default="assistent/gpt_builder_knowledge_manifest.json",
        help="Output JSON file list path.",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    files = _collect_files(repo_root)
    if not files:
        print("No files selected. Nothing to bundle.")
        return 1

    zip_path = (repo_root / args.zip_path).resolve()
    manifest_path = (repo_root / args.manifest_path).resolve()

    _build_zip(files, repo_root, zip_path)
    _write_manifest(files, repo_root, manifest_path)

    size_mb = zip_path.stat().st_size / 1024 / 1024
    print(f"Bundle created: {zip_path}")
    print(f"Manifest created: {manifest_path}")
    print(f"Files included: {len(files)}")
    print(f"Zip size: {size_mb:.2f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
