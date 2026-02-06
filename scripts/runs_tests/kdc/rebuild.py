from sqlalchemy.orm import Session
from biofilter import Biofilter
from biofilter.modules.kdc import rebuild_kdc

bf = Biofilter()
# session: Session = bf.db.get_session()

# result = rebuild_kdc(
#     session,
#     kds_root="biofilter_data/processed",
#     dry_run=False,
#     strict=False,
# )

# session.commit()

# print(result)
print(bf.kdc.list_assets())
res = bf.kdc.rebuild("biofilter_data/processed")
print(res)

# bf.kdc.list_assets()
# bf.kdc.list_asset_versions(asset="masterdata")
# bf.kdc.describe_asset_version(asset_version_id=1)