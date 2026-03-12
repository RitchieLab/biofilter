"""add schema_revision to biofilter_metadata

Revision ID: 35bc63e8d681
Revises: 94d71d25be0e
Create Date: 2026-01-22 05:11:06.173508

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "35bc63e8d681"
down_revision: Union[str, Sequence[str], None] = "94d71d25be0e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    cols = {c["name"] for c in insp.get_columns("biofilter_metadata")}
    if "schema_revision" not in cols:
        op.add_column(
            "biofilter_metadata",
            sa.Column("schema_revision", sa.String(length=50), nullable=True),
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    cols = {c["name"] for c in insp.get_columns("biofilter_metadata")}
    if "schema_revision" in cols:
        op.drop_column("biofilter_metadata", "schema_revision")
