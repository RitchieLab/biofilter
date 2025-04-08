from sqlalchemy.orm import Session
from sqlalchemy import select, update
from omics_modules.models import DataSource
import datetime


class UpdaterOperationsMixin:
    """
    Mixin responsible for handling database operations related to DataSources.
    """

    def sync_data_sources(self):
        """
        Synchronizes the data sources stored in the database with the available source system scripts.

        - Adds new data sources if they are not in the database.
        - Deactivates data sources that are in the database but no longer present in source system scripts.
        """
        self.logger.log("[INFO] Synchronizing data sources...")

        # Find all available source modules and update `_sourceSystems`
        self.findSourceModules()

        with Session(self._engine) as session:
            # Fetch all existing data sources from the database
            existing_sources = {
                ds.name: ds for ds in session.scalars(select(DataSource)).all()
            }
            detected_sources = set(self._sourceSystems.keys())

            # Insert new data sources in bulk
            new_sources = detected_sources - set(existing_sources.keys())
            new_entries = [
                DataSource(
                    name=source_name,
                    data_type="UNKNOWN",
                    format="UNKNOWN",
                    dtp_version="1.0",
                    last_status="pending",
                    active=True,
                )
                for source_name in new_sources
            ]

            if new_entries:
                session.add_all(new_entries)
                self.logger.log(f"[INFO] Added {len(new_entries)} new data sources.")

            # Deactivate missing sources in bulk
            missing_sources = set(existing_sources.keys()) - detected_sources
            if missing_sources:
                session.execute(
                    update(DataSource)
                    .where(DataSource.name.in_(missing_sources))
                    .values(active=False, updated_at=datetime.datetime.utcnow())
                )
                self.logger.log(
                    f"[WARNING] Deactivated {len(missing_sources)} missing data sources."
                )

            # Commit all changes in a single transaction
            session.commit()

        self.logger.log("[INFO] Data source synchronization complete.")

    def set_datasource_status(self, srcSet, source_name, new_status):
        """
        Updates the `last_status` of a single DataSource object.

        Args:
            srcSet (list): List of DataSource objects retrieved from the database.
            source_name (str): The name of the data source to update.
            new_status (str): The new status value to set.
        """
        with Session(self._engine) as session:
            result = session.execute(
                update(DataSource)
                .where(DataSource.name == source_name)
                .values(last_status=new_status, updated_at=datetime.datetime.utcnow())
            )

            if result.rowcount > 0:
                self.logger.log(
                    f"[INFO] Updated DataSource '{source_name}' to status '{new_status}'."
                )
                session.commit()
            else:
                self.logger.log(
                    f"[WARNING] DataSource '{source_name}' not found in database."
                )
