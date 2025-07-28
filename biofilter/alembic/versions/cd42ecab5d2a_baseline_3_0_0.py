"""baseline 3.0.0

Revision ID: cd42ecab5d2a
Revises: 43ced8ac4e12
Create Date: 2025-07-16 15:22:13.153045

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "cd42ecab5d2a"
down_revision: Union[str, Sequence[str], None] = "43ced8ac4e12"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
