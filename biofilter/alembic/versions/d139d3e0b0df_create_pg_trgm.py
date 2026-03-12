"""create pg_trgm

Revision ID: d139d3e0b0df
Revises: 35bc63e8d681
Create Date: 2026-01-28 17:48:54.329202

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d139d3e0b0df"
down_revision: Union[str, Sequence[str], None] = "35bc63e8d681"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_entity_aliases_alias_norm_trgm
        ON public.entity_aliases
        USING gin (alias_norm gin_trgm_ops);
    """
    )


def downgrade():
    op.execute("DROP INDEX IF EXISTS public.ix_entity_aliases_alias_norm_trgm;")
    # op.execute("DROP EXTENSION IF EXISTS pg_trgm;")  # opcional
