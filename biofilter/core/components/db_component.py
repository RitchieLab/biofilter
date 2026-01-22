from __future__ import annotations

from pathlib import Path
from typing import Optional, Literal
from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.db.migrate import run_migration
from biofilter.modules.db.database import Database

from biofilter.modules.db.transfer import (
    backup_db,
    restore_db,
    export_full_clone,
    import_full_clone,
)

ExportFormat = Literal["parquet", "csv"]


class DBComponent(BaseComponent):
    """
    Database lifecycle component.

    Owns the ONLY place where core.db is created/replaced.
    All other components must consume core.db via core.require_db().
    """

    def connect(self, new_uri: Optional[str] = None) -> Database:
        if new_uri:
            self.core.db_uri = new_uri

        self.core.db = Database(self.core.db_uri)

        # Optional: eager session test (helps catch bad URIs early)
        # with self.core.db.get_session() as _:
        #     pass

        # self.core.logger.log(f"✅ Connected to DB: {self.core.db_uri}", "INFO")
        return self.core.db

    def create_db(self, db_uri: Optional[str] = None, overwrite: bool = False):
        """
        Create a new database file/schema and connect to it.

        Note:
        - Uses Database().create_db(), which is expected to bootstrap core tables/mappings.
        """
        if db_uri:
            self.core.db_uri = db_uri

        # Create DB using your existing Database/CreateDBMixin logic.
        db = Database()  # do not pass uri in ctor, following your current pattern
        db.db_uri = self.core.db_uri
        db.create_db(overwrite=overwrite)

        # Ensure this becomes the active shared instance (core tables/mappings live here)
        self.core.db = db

        self.core.logger.log(f"🏗️ Database created at: {self.core.db_uri}", "INFO")
        return True

    # def migrate(self) -> bool:
    #     db = self.require_db()
    #     # run_migration(db.session_factory, db.db_uri)  # ou db.SessionLocal
    #     run_migration(db.SessionLocal, db.engine, db.db_uri)
    #     self.core.logger.log("✅ Migration completed.", "INFO")
    #     return True
    def migrate(
        self,
        *,
        action: str = "upgrade",   # "upgrade" | "status" | "stamp-head" | "dry-run"
        target: str = "head",
        force: bool = False,
    ) -> bool:
        db = self.require_db()
        if not db.engine:
            raise RuntimeError("Database engine not initialized. Call connect() first.")

        ok = run_migration(
            session_factory=db.SessionLocal,
            engine=db.engine,
            db_uri=db.db_uri,
            action=action,
            target=target,
            force=force,
        )

        # log only if we actually ran something (opcional)
        if ok:
            self.core.logger.log("✅ Migration completed.", "INFO")
        return ok

    def get_session(self):
        """
        Convenience passthrough to the shared Database session context manager.
        """
        return self.require_db().get_session()


    # ---------------------------
    # Snapshot (physical)
    # ---------------------------
    def backup(self, output_path: str | Path) -> Path:
        """
        Create a physical backup snapshot of the current DB.
        """
        db = self.core.require_db()
        if not db.engine:
            raise RuntimeError("Database engine not initialized. Connect first.")

        out = Path(output_path).expanduser().resolve()
        self.core.logger.log(f"💾 Creating DB backup snapshot → {out}", "INFO")

        created = backup_db(db.engine, out)
        self.core.logger.log(f"✅ Backup created: {created}", "INFO")
        return created

    def restore(self, input_path: str | Path) -> None:
        """
        Restore a physical backup snapshot into the current DB target.
        """
        db = self.core.require_db()
        if not db.engine:
            raise RuntimeError("Database engine not initialized. Connect first.")

        inp = Path(input_path).expanduser().resolve()
        self.core.logger.log(f"♻️ Restoring DB snapshot from → {inp}", "WARNING")

        restore_db(db.engine, inp)
        self.core.logger.log("✅ Restore completed.", "INFO")

        # After restore, reconnect to ensure bootstrap_models is re-applied
        # and to avoid stale engine/session state.
        db.connect(check_exists=True)
        self.core.logger.log("🔁 Reconnected after restore (bootstrapped models).", "INFO")

    # ---------------------------
    # Full clone bundle (logical)
    # ---------------------------
    def export(
        self,
        out_dir: str | Path,
        *,
        fmt: ExportFormat = "parquet",
        biofilter_version: Optional[str] = None,
        schema_version: str = "unknown",
        chunksize: int = 250_000,
    ) -> Path:
        """
        Export a logical full-clone bundle (manifest + one file per table).
        """
        db = self.core.require_db()
        if not db.engine:
            raise RuntimeError("Database engine not initialized. Connect first.")

        out = Path(out_dir).expanduser().resolve()
        out.mkdir(parents=True, exist_ok=True)

        bf_ver = biofilter_version or getattr(self.core, "version", "unknown")

        self.core.logger.log(f"📦 Exporting full clone bundle → {out} (fmt={fmt})", "INFO")

        bundle_dir = export_full_clone(
            db.engine,
            out,
            biofilter_version=bf_ver,
            schema_version=schema_version,
            fmt=fmt,
            chunksize=chunksize,
        )

        self.core.logger.log(f"✅ Bundle exported: {bundle_dir}", "INFO")
        return bundle_dir

    def import_(
        self,
        in_dir: str | Path,
        *,
        fmt: ExportFormat = "parquet",
        rebuild_indexes: bool = True,
        reset_postgres_sequences: bool = True,
    ) -> None:
        """
        Import a logical full-clone bundle into the current DB schema.

        Expectations:
          - Schema already exists (project create / migrations done)
          - This will truncate all tables and re-insert preserving PKs.
        """
        db = self.core.require_db()
        if not db.engine:
            raise RuntimeError("Database engine not initialized. Connect first.")

        inp = Path(in_dir).expanduser().resolve()
        self.core.logger.log(f"📥 Importing full clone bundle ← {inp} (fmt={fmt})", "WARNING")

        import_full_clone(
            db=db,
            in_dir=inp,
            fmt=fmt,
            reset_sequences=reset_postgres_sequences,
        )

        self.core.logger.log("✅ Bundle import completed.", "INFO")

        # Re-bootstrap models (safe) — especially useful if import touched metadata-heavy tables
        db.connect(check_exists=True)

        if rebuild_indexes:
            # Reuse your existing index rebuild flow if available
            try:
                self.core.logger.log("🧱 Rebuilding indexes after import...", "INFO")
                self.core.etl.rebuild_indexes(groups=None, drop_first=True)
                self.core.logger.log("✅ Index rebuild done.", "INFO")
            except Exception as e:
                self.core.logger.log(f"⚠️ Index rebuild failed: {e}", "WARNING")
