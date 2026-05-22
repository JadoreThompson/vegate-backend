"""Converted payload from str to JSONB

Revision ID: 6c6003ecc6e1
Revises: 83ec7445d5f1
Create Date: 2026-05-22 08:29:46.064730

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "6c6003ecc6e1"
down_revision: Union[str, Sequence[str], None] = "83ec7445d5f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "event_outbox",
        sa.Column(
            "payload_jsonb",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )

    op.drop_column("event_outbox", "payload")

    op.alter_column(
        "event_outbox",
        "payload_jsonb",
        new_column_name="payload",
        server_default=None,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        "event_outbox",
        sa.Column(
            "payload_varchar",
            sa.VARCHAR(),
            nullable=False,
            server_default="{}",
        ),
    )

    op.drop_column("event_outbox", "payload")

    op.alter_column(
        "event_outbox",
        "payload_varchar",
        new_column_name="payload",
        server_default=None,
    )