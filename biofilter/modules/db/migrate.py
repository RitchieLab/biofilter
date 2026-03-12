from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from typing import Optional

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext

from biofilter.modules.db.models import BiofilterMetadata


# -----------------------------------------------------------------------------
# Paths / Alembic helpers
# -----------------------------------------------------------------------------


def get_script_location() -> str:
    # Works both in editable installs and wheels
    return str(files("biofilter") / "alembic")


def _make_alembic_config(script_location: str, db_uri: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", script_location)
    cfg.set_main_option("sqlalchemy.url", db_uri)
    return cfg


def get_head_revision(script_location: str) -> str:
    cfg = Config()
    cfg.set_main_option("script_location", script_location)
    script = ScriptDirectory.from_config(cfg)
    heads = script.get_heads()
    if len(heads) != 1:
        raise RuntimeError(f"Expected single Alembic head, got: {heads}")
    return heads[0]


def get_db_revision(engine) -> Optional[str]:
    with engine.connect() as conn:
        context = MigrationContext.configure(conn)
        return context.get_current_revision()  # None if never stamped


def is_db_versioned(engine) -> bool:
    # Detect if alembic_version table exists (Postgres: schema-aware)
    try:
        has_tbl = engine.dialect.has_table(engine.connect(), "alembic_version")  # type: ignore
        return bool(has_tbl)
    except Exception:
        # Fallback: if get_current_revision() works, it's versioned
        return get_db_revision(engine) is not None


@dataclass
class MigrationStatus:
    script_location: str
    head: str
    current: Optional[str]
    is_versioned: bool

    @property
    def is_up_to_date(self) -> bool:
        return self.current is not None and self.current == self.head


def get_status(engine, db_uri: str) -> MigrationStatus:
    script_location = get_script_location()
    head = get_head_revision(script_location)
    current = get_db_revision(engine)
    versioned = is_db_versioned(engine)
    return MigrationStatus(
        script_location=script_location,
        head=head,
        current=current,
        is_versioned=versioned,
    )


# -----------------------------------------------------------------------------
# Core migration runner
# -----------------------------------------------------------------------------


def _mirror_revision_to_metadata(session_factory, revision: str) -> None:
    """
    Optional: update BiofilterMetadata.schema_revision for quick visibility.
    (Alembic remains the source of truth via alembic_version.)
    """
    if not session_factory:
        return

    session = session_factory()
    try:
        meta = session.query(BiofilterMetadata).first()
        if meta:
            meta.schema_revision = revision
            session.commit()
    finally:
        session.close()


def run_migration(
    session_factory=None,
    engine=None,
    db_uri: str | None = None,
    *,
    dry_run: bool = False,
    action: str = "upgrade",  # "upgrade" | "status" | "stamp-head" | "dry-run"
    target: str = "head",
    force: bool = False,
) -> bool:
    """
    Backwards-compatible entrypoint.

    Existing behavior:
      run_migration(session_factory, engine, db_uri, dry_run=False)

    New behavior:
      action="status"       -> prints status only
      action="stamp-head"   -> stamps alembic head (baseline) without executing DDL
      action="dry-run"      -> prints SQL for upgrade (no execution)
      action="upgrade"      -> upgrades to target (default head)
    """
    if engine is None:
        raise ValueError("engine is required for migrations/status.")
    if not db_uri:
        raise ValueError("db_uri is required for migrations/status.")

    # normalize action from older flag
    if dry_run and action == "upgrade":
        action = "dry-run"

    st = get_status(engine, db_uri)

    # --- STATUS ---
    if action == "status":
        print("📄 Alembic status")
        print(f"  DB URI:         {db_uri}")
        print(f"  Script location:{st.script_location}")
        print(f"  Repo head:      {st.head}")
        print(f"  DB revision:    {st.current}")
        print(f"  Versioned DB?:  {st.is_versioned}")
        if st.is_up_to_date:
            print("✅ Up-to-date.")
        else:
            print("⚠️  Pending migrations or unstamped DB.")
        return True

    # If user requested stamp-head, they probably have an existing schema already.
    if action == "stamp-head":
        if st.is_up_to_date:
            print(f"✅ Already at head (revision={st.current})")
            return True

        # Safety: stamping overwrites "current revision" concept without applying DDL.
        # Only allow if DB is unversioned OR force=True.
        if st.is_versioned and not force:
            raise RuntimeError(
                "Refusing to stamp: DB already has alembic_version. "
                "Use --force to overwrite stamp (dangerous)."
            )

        cfg = _make_alembic_config(st.script_location, db_uri)
        print(f"🏷️  Stamping DB to head revision: {st.head}")
        command.stamp(cfg, st.head)
        _mirror_revision_to_metadata(session_factory, st.head)
        print("✅ Stamp completed.")
        return True

    # --- DRY RUN ---
    if action == "dry-run":
        cfg = _make_alembic_config(st.script_location, db_uri)
        print("🧪 Dry-run (SQL only)")
        print(f"  From: {st.current}")
        print(f"  To:   {target}")
        # This prints SQL to stdout; no execution
        command.upgrade(cfg, target, sql=True)
        return True

    # --- UPGRADE ---
    if action != "upgrade":
        raise ValueError(f"Unknown action: {action}")

    # no-op
    if st.is_up_to_date and target in ("head", st.head):
        print(f"✅ Schema up-to-date (revision={st.current})")
        return True

    # If DB is NOT versioned (no alembic_version), upgrading will fail/confuse.
    if not st.is_versioned and st.current is None and not force:
        raise RuntimeError(
            "Database is not Alembic-versioned (no alembic_version row). "
            "If this DB already has tables, run `biofilter db migrate --stamp-head` first. "
            "If you really want to run upgrade anyway, use --force."
        )

    cfg = _make_alembic_config(st.script_location, db_uri)
    print("🚀 Running Alembic migrations")
    print(f"  From: {st.current}")
    print(f"  To:   {target}")
    command.upgrade(cfg, target)

    # Refresh status and mirror
    st2 = get_status(engine, db_uri)
    if st2.current:
        _mirror_revision_to_metadata(session_factory, st2.current)

    print(f"✅ Migration completed: {st.current} → {st2.current}")
    return True


def get_repo_heads(script_location: str) -> list[str]:
    cfg = Config()
    cfg.set_main_option("script_location", script_location)
    script = ScriptDirectory.from_config(cfg)
    return script.get_heads()


"""
biofilter db migrate --status

biofilter db migrate --stamp-head [--force]

biofilter db migrate --dry-run

biofilter db migrate (default = upgrade)
"""


# def alembic_upgrade_head(db_uri: str, script_location: str, logger=None) -> None:
#     """
#     Run 'alembic upgrade head' programmatically.

#     Notes:
#     - We set sqlalchemy.url to the runtime db_uri.
#     - script_location must point to your Alembic env (alembic.ini or equivalent).
#     """
#     cfg = Config()
#     cfg.set_main_option("script_location", script_location)
#     cfg.set_main_option("sqlalchemy.url", db_uri)

#     if logger:
#         logger.log("Running Alembic command: upgrade head", "INFO")

#     command.upgrade(cfg, "head")


# import os
# from pathlib import Path
# from importlib.resources import files
# from alembic import command
# from alembic.config import Config
# from alembic.script import ScriptDirectory
# from alembic.runtime.migration import MigrationContext
# from sqlalchemy import inspect

# from packaging.version import parse as parse_version

# from biofilter.modules.db.models import BiofilterMetadata
# # from biofilter.utils.version import __version__ as current_version

# from importlib.resources import files

# def get_script_location() -> str:
#     return str(files("biofilter") / "alembic")
# # def get_script_location() -> str:
# #     pkg_root = Path(__file__).resolve().parents[1]  # biofilter/
# #     return str(pkg_root / "alembic")


# def get_head_revision(script_location: str) -> str:
#     cfg = Config()
#     cfg.set_main_option("script_location", script_location)
#     script = ScriptDirectory.from_config(cfg)
#     heads = script.get_heads()
#     if len(heads) != 1:
#         raise RuntimeError(f"Expected single Alembic head, got: {heads}")
#     return heads[0]


# def get_db_revision(engine) -> str | None:
#     with engine.connect() as conn:
#         context = MigrationContext.configure(conn)
#         return context.get_current_revision()  # None if never stamped


# def run_migration(session_factory, engine, db_uri: str, *, dry_run: bool = False) -> bool:
#     script_location = get_script_location()
#     head = get_head_revision(script_location)
#     current = get_db_revision(engine)

#     # If DB has no alembic_version row yet, you can choose to "stamp" baseline.
#     # But in your approach you already created a baseline revision; so "current" should exist.
#     if current == head:
#         print(f"✅ Schema up-to-date (revision={current})")
#         return True

#     print(f"📦 DB revision: {current} | Code head: {head}")
#     if dry_run:
#         print("🧪 Dry-run enabled: would run `alembic upgrade head`")
#         return False

#     cfg = Config()
#     cfg.set_main_option("script_location", script_location)
#     cfg.set_main_option("sqlalchemy.url", db_uri)

#     print("🚀 Running Alembic migrations → head")
#     command.upgrade(cfg, "head")

#     # Optional: mirror the revision inside BiofilterMetadata (nice for UI/quick checks)
#     session = session_factory()
#     try:
#         meta = session.query(BiofilterMetadata).first()
#         if meta:
#             meta.schema_revision = head
#             session.commit()
#     finally:
#         session.close()

#     print(f"✅ Migration completed: {current} → {head}")
#     return True


# # def get_script_location() -> str:
# #     # repo_root/biofilter/utils/alembic_utils.py -> repo_root/biofilter
# #     pkg_root = Path(__file__).resolve().parents[1]
# #     alembic_dir = pkg_root / "alembic"
# #     return str(alembic_dir)


# def get_repo_heads(script_location: str) -> list[str]:
#     cfg = Config()
#     cfg.set_main_option("script_location", script_location)
#     script = ScriptDirectory.from_config(cfg)
#     return script.get_heads()
