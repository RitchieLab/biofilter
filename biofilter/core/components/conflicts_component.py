from __future__ import annotations

from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.etl.conflict_manager import ConflictManager


class ConflictsComponent(BaseComponent):
    """
    Conflict management component (export/import, re-load, etc.).
    """

    def export_to_excel(self, output_path: str = "curation_conflicts.xlsx"):
        db = self.require_db()
        manager = ConflictManager(session=db.get_session(), logger=self.core.logger)
        return manager.export_conflicts_to_excel(output_path)

    def import_from_excel(self, input_path: str = "curation_conflicts_template.xlsx"):
        db = self.require_db()
        manager = ConflictManager(db.get_session(), self.core.logger)
        return manager.import_conflicts_from_excel(input_path)

    def reprocess_load(self, source_system: list | None = None) -> bool:
        """
        Convenience wrapper to run LOAD step using conflict CSVs (legacy behavior).
        """
        self.core.logger.log("🚧 Running conflict reprocess (load-only)...", "INFO")
        return self.core.etl.update(
            source_system=source_system,
            run_steps=["load"],
            force_steps=["load"],
            use_conflict_csv=True,
        )
  # noqa: E501
