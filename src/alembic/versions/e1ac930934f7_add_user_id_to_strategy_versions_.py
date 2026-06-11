"""add_user_id_to_strategy_versions_deployments_backtests

Revision ID: e1ac930934f7
Revises: 9b191341ff7b
Create Date: 2026-06-10 13:34:48.336072

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "e1ac930934f7"
down_revision: Union[str, Sequence[str], None] = "9b191341ff7b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add columns as nullable first so existing rows are not rejected
    op.add_column("strategy_versions", sa.Column("user_id", sa.UUID(), nullable=True))
    op.add_column("strategy_deployments", sa.Column("user_id", sa.UUID(), nullable=True))
    op.add_column("strategy_deployments", sa.Column("strategy_id", sa.UUID(), nullable=True))
    op.add_column("backtests", sa.Column("user_id", sa.UUID(), nullable=True))
    op.add_column("backtests", sa.Column("strategy_id", sa.UUID(), nullable=True))

    # Populate strategy_versions.user_id from the owning strategy
    op.execute(
        """
        UPDATE strategy_versions
        SET user_id = strategy.user_id
        FROM strategy
        WHERE strategy_versions.strategy_id = strategy.strategy_id
        """
    )

    # Populate strategy_deployments.user_id and strategy_id
    # via version_id -> strategy_versions -> strategy
    op.execute(
        """
        UPDATE strategy_deployments
        SET user_id = strategy.user_id,
            strategy_id = strategy.strategy_id
        FROM strategy_versions, strategy
        WHERE strategy_deployments.version_id = strategy_versions.id
          AND strategy_versions.strategy_id = strategy.strategy_id
        """
    )

    # Populate backtests.user_id and strategy_id
    # via version_id -> strategy_versions -> strategy
    op.execute(
        """
        UPDATE backtests
        SET user_id = strategy.user_id,
            strategy_id = strategy.strategy_id
        FROM strategy_versions, strategy
        WHERE backtests.version_id = strategy_versions.id
          AND strategy_versions.strategy_id = strategy.strategy_id
        """
    )

    # Now that all rows are populated, enforce NOT NULL
    op.alter_column("strategy_versions", "user_id", nullable=False)
    op.alter_column("strategy_deployments", "user_id", nullable=False)
    op.alter_column("strategy_deployments", "strategy_id", nullable=False)
    op.alter_column("backtests", "user_id", nullable=False)
    op.alter_column("backtests", "strategy_id", nullable=False)

    # Add foreign key constraints
    op.create_foreign_key(
        "strategy_versions_user_id_fkey",
        "strategy_versions",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "strategy_deployments_user_id_fkey",
        "strategy_deployments",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "strategy_deployments_strategy_id_fkey",
        "strategy_deployments",
        "strategy",
        ["strategy_id"],
        ["strategy_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "backtests_user_id_fkey",
        "backtests",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "backtests_strategy_id_fkey",
        "backtests",
        "strategy",
        ["strategy_id"],
        ["strategy_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # Drop foreign keys first
    op.drop_constraint(
        "backtests_strategy_id_fkey", "backtests", type_="foreignkey"
    )
    op.drop_constraint(
        "backtests_user_id_fkey", "backtests", type_="foreignkey"
    )
    op.drop_constraint(
        "strategy_deployments_strategy_id_fkey",
        "strategy_deployments",
        type_="foreignkey",
    )
    op.drop_constraint(
        "strategy_deployments_user_id_fkey",
        "strategy_deployments",
        type_="foreignkey",
    )
    op.drop_constraint(
        "strategy_versions_user_id_fkey", "strategy_versions", type_="foreignkey"
    )

    # Drop columns (reverse order of addition)
    op.drop_column("backtests", "strategy_id")
    op.drop_column("backtests", "user_id")
    op.drop_column("strategy_deployments", "strategy_id")
    op.drop_column("strategy_deployments", "user_id")
    op.drop_column("strategy_versions", "user_id")
