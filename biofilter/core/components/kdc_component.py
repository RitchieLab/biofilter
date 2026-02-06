from __future__ import annotations

from sqlalchemy.orm import Session

from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.kdc.rebuild import rebuild_kdc

from biofilter.modules.db.models.model_kdc import (
    KDCAsset,
    KDCAssetVersion,
    KDCSchemaField,
)


class KDCComponent(BaseComponent):
    """
    KDC entry point (lazy-loaded).
    """

    def _session(self) -> Session:
        db = self.require_db()
        return db.get_session()

    def rebuild(self, kds_root: str, dry_run: bool = False, strict: bool = False):
        session = self._session()
        result = rebuild_kdc(session, kds_root=kds_root, dry_run=dry_run, strict=strict)
        if not dry_run:
            session.commit()
        return result

    def list_assets(self):
        session = self._session()
        return (
            session.query(
                KDCAsset.source_system,
                KDCAsset.data_source,
                KDCAsset.asset,
            )
            .order_by(KDCAsset.source_system, KDCAsset.data_source, KDCAsset.asset)
            .all()
        )

    def list_asset_versions(self, *, source_system=None, data_source=None, asset=None):
        session = self._session()

        q = session.query(KDCAssetVersion).join(KDCAsset)

        if source_system:
            q = q.filter(KDCAsset.source_system == source_system)
        if data_source:
            q = q.filter(KDCAsset.data_source == data_source)
        if asset:
            q = q.filter(KDCAsset.asset == asset)

        return q.order_by(
            KDCAsset.source_system,
            KDCAsset.data_source,
            KDCAsset.asset,
            KDCAssetVersion.release,
            KDCAssetVersion.assembly,
        ).all()

    def describe_asset_version(self, asset_version_id: int):
        session = self._session()
        return (
            session.query(KDCSchemaField)
            .filter(KDCSchemaField.asset_version_id == asset_version_id)
            .order_by(KDCSchemaField.field_name)
            .all()
        )
