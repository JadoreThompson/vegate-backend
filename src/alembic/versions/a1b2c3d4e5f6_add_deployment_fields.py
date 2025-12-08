"""add deployment fields

Revision ID: a1b2c3d4e5f6
Revises: 7588e14328cd
Create Date: 2025-12-08 01:07:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "7588e14328cd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add new columns to strategy_deployments table
    op.add_column(
        "strategy_deployments", sa.Column("ticker", sa.String(), nullable=False)
    )
    op.add_column(
        "strategy_deployments", sa.Column("timeframe", sa.String(), nullable=False)
    )
    op.add_column(
        "strategy_deployments",
        sa.Column("starting_balance", sa.Numeric(), nullable=False),
    )
    op.add_column(
        "strategy_deployments",
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "strategy_deployments",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.add_column(
        "strategy_deployments", sa.Column("error_message", sa.Text(), nullable=True)
    )
    op.add_column(
        "strategy_deployments",
        sa.Column("config", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.execute("alter table backtests rename column ticker to symbol")


def downgrade() -> None:
    """Downgrade schema."""
    # Remove columns from strategy_deployments table
    op.drop_column("strategy_deployments", "config")
    op.drop_column("strategy_deployments", "error_message")
    op.drop_column("strategy_deployments", "updated_at")
    op.drop_column("strategy_deployments", "stopped_at")
    op.drop_column("strategy_deployments", "starting_balance")
    op.drop_column("strategy_deployments", "timeframe")
    op.drop_column("strategy_deployments", "ticker")
