#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


# User-facing assistant: knowledge base is docs + operational guides +
# per-report explain docs + runnable notebook examples. No Python source code.
# Keep this list in sync with assistant_context_manifest.yaml.
def _default_sources() -> list[dict]:
    return [
        {"id": "docs", "path": "docs/source", "ext": {".md"}},
        {"id": "agents", "path": "biofilter_agents", "ext": {".md"}},
        {
            "id": "reports_explain",
            "path": "biofilter/modules/report/reports_explain",
            "ext": {".md"},
        },
        {"id": "notebooks", "path": "notebooks/Templates", "ext": {".ipynb", ".md"}},
    ]


def _collect_files(repo_root: Path) -> list[Path]:
    exclusions = [
        "**/.git/**",
        "**/.venv/**",
        "**/__pycache__/**",
        "**/*.log",
        "**/*.py",  # user-facing assistant: never bundle source code
        "**/docs/build/**",
        "**/AGENTS.md",
        "**/reports_bkp/**",
        "**/.ipynb_checkpoints/**",
        "**/.DS_Store",
        # Developer-only material excluded from the end-user knowledge base.
        # NOTE: fnmatch treats "*" as matching "/" too, so a "**/name.md"
        # pattern requires at least one leading path segment. All targets
        # below live under a parent dir, so this matches them correctly.
        "**/developer_extensions.md",
        "**/schema.md",
        "**/biobin_technical_reference.md",
        "**/report_template.md",
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


def _ipynb_to_markdown(path: Path) -> str:
    """
    Render a Jupyter notebook as plain Markdown.

    OpenAI File Search (used by GPT Builder Knowledge) rejects the .ipynb
    extension, and raw notebook JSON is noisy. Emit markdown cells as-is and
    code cells fenced as ```python, dropping outputs.
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
        else:
            blocks.append(src)
    return "\n\n".join(blocks) + "\n"


def _arcname(path: Path, repo_root: Path) -> str:
    """Archive/manifest name; notebooks become <name>.ipynb.md (accepted ext)."""
    rel = path.resolve().relative_to(repo_root.resolve()).as_posix()
    if path.suffix.lower() == ".ipynb":
        return rel[: -len(".ipynb")] + ".ipynb.md"
    return rel


def _write_manifest(files: list[Path], repo_root: Path, output_json: Path) -> None:
    payload = {
        "count": len(files),
        "files": [_arcname(p, repo_root) for p in files],
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_zip(files: list[Path], repo_root: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as zf:
        for p in files:
            arcname = _arcname(p, repo_root)
            if p.suffix.lower() == ".ipynb":
                zf.writestr(arcname, _ipynb_to_markdown(p))
            else:
                zf.write(p, arcname=arcname)


def _file_text(path: Path) -> str:
    if path.suffix.lower() == ".ipynb":
        return _ipynb_to_markdown(path)
    return path.read_text(encoding="utf-8")


def _source_group(arcname: str) -> str:
    if arcname.startswith("docs/"):
        return "docs"
    if arcname.startswith("biofilter_agents/"):
        return "agents"
    if "reports_explain/" in arcname:
        return "reports_explain"
    if arcname.startswith("notebooks/"):
        return "notebooks"
    return "faq"


_GROUP_TITLES = {
    "docs": "BF4 User Documentation",
    "agents": "BF4 Operational Guides",
    "reports_explain": "BF4 Report Reference (per report)",
    "notebooks": "BF4 Example Notebooks",
    "faq": "BF4 Support FAQ",
}


def _build_consolidated(
    files: list[Path], repo_root: Path, out_dir: Path
) -> list[tuple[Path, int]]:
    """
    Merge the selected files into a handful of grouped Markdown documents.

    ChatGPT custom GPTs cap the Knowledge tab at 20 files; 72 individual
    files do not fit. Merging by source group (docs / agents / reports /
    notebooks / faq) keeps it well under the cap. Each embedded file keeps a
    provenance header so the assistant can still cite where a passage came
    from.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    groups: dict[str, list[Path]] = {}
    for p in files:
        groups.setdefault(_source_group(_arcname(p, repo_root)), []).append(p)

    written: list[tuple[Path, int]] = []
    for gid in sorted(groups):
        gpaths = sorted(groups[gid])
        parts = [f"# {_GROUP_TITLES.get(gid, gid)}\n"]
        for p in gpaths:
            arc = _arcname(p, repo_root)
            parts.append(
                f"\n\n<!-- ===== SOURCE FILE: {arc} ===== -->\n\n"
                + _file_text(p).rstrip()
                + "\n"
            )
        target = out_dir / f"bf4_{gid}.md"
        target.write_text("\n".join(parts), encoding="utf-8")
        written.append((target, len(gpaths)))
    return written


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
        default="assistent/gpt_builder/gpt_builder_knowledge_bundle.zip",
        help="Output zip path.",
    )
    parser.add_argument(
        "--manifest-path",
        default="assistent/gpt_builder/gpt_builder_knowledge_manifest.json",
        help="Output JSON file list path.",
    )
    parser.add_argument(
        "--consolidate-dir",
        default="assistent/gpt_builder/consolidated",
        help=(
            "Directory for consolidated grouped Markdown files "
            "(fits ChatGPT's 20-file Knowledge cap). Upload THESE to a GPT."
        ),
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

    consolidate_dir = (repo_root / args.consolidate_dir).resolve()
    consolidated = _build_consolidated(files, repo_root, consolidate_dir)

    size_mb = zip_path.stat().st_size / 1024 / 1024
    print(f"Bundle created: {zip_path}")
    print(f"Manifest created: {manifest_path}")
    print(f"Files included: {len(files)}")
    print(f"Zip size: {size_mb:.2f} MB")
    print(f"\nConsolidated (upload these to a ChatGPT GPT) -> {consolidate_dir}")
    for target, n in consolidated:
        kb = target.stat().st_size / 1024
        print(f"  • {target.name}  ({n} files merged, {kb:.0f} KB)")
    print(f"  = {len(consolidated)} files total (well under ChatGPT's 20-file cap)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
