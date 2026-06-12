"""Updated all primary keys to be id

Revision ID: a9711982901c
Revises: e1ac930934f7
Create Date: 2026-06-11 22:07:32.435765

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "a9711982901c"
down_revision: Union[str, Sequence[str], None] = "e1ac930934f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ----- users: user_id -> id -----
    op.add_column("users", sa.Column("id", sa.UUID(), nullable=True))
    op.execute("UPDATE users SET id = user_id")
    op.alter_column("users", "id", nullable=False)

    op.drop_constraint(op.f("backtests_user_id_fkey"), "backtests", type_="foreignkey")
    op.drop_constraint(
        op.f("broker_connections_user_id_fkey"),
        "broker_connections",
        type_="foreignkey",
    )
    op.drop_constraint(op.f("strategy_user_id_fkey"), "strategy", type_="foreignkey")
    op.drop_constraint(
        op.f("strategy_deployments_user_id_fkey"),
        "strategy_deployments",
        type_="foreignkey",
    )
    op.drop_constraint(
        op.f("strategy_versions_user_id_fkey"),
        "strategy_versions",
        type_="foreignkey",
    )

    op.drop_column("users", "user_id")
    op.create_primary_key("users_pkey", "users", ["id"])

    op.create_foreign_key(
        op.f("backtests_user_id_fkey"),
        "backtests",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("broker_connections_user_id_fkey"),
        "broker_connections",
        "users",
        ["user_id"],
        ["id"],
    )
    op.create_foreign_key(
        op.f("strategy_user_id_fkey"),
        "strategy",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("strategy_deployments_user_id_fkey"),
        "strategy_deployments",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_column("strategy_versions", "user_id")

    # ----- strategy: strategy_id -> id -----
    op.add_column("strategy", sa.Column("id", sa.UUID(), nullable=True))
    op.execute("UPDATE strategy SET id = strategy_id")
    op.alter_column("strategy", "id", nullable=False)

    op.drop_constraint(
        op.f("backtests_strategy_id_fkey"), "backtests", type_="foreignkey"
    )
    op.drop_constraint(
        op.f("strategy_deployments_strategy_id_fkey"),
        "strategy_deployments",
        type_="foreignkey",
    )
    op.drop_constraint(
        op.f("strategy_versions_strategy_id_fkey"),
        "strategy_versions",
        type_="foreignkey",
    )

    op.drop_column("strategy", "strategy_id")
    op.create_primary_key("strategy_pkey", "strategy", ["id"])

    op.create_foreign_key(
        op.f("backtests_strategy_id_fkey"),
        "backtests",
        "strategy",
        ["strategy_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("strategy_deployments_strategy_id_fkey"),
        "strategy_deployments",
        "strategy",
        ["strategy_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("strategy_versions_strategy_id_fkey"),
        "strategy_versions",
        "strategy",
        ["strategy_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # ----- ohlcs: ohlc_id -> id -----
    op.add_column("ohlcs", sa.Column("id", sa.UUID(), nullable=True))
    op.execute("UPDATE ohlcs SET id = ohlc_id")
    op.alter_column("ohlcs", "id", nullable=False)
    op.drop_column("ohlcs", "ohlc_id")
    op.create_primary_key("ohlcs_pkey", "ohlcs", ["id"])

    # ----- broker_connections: connection_id -> id -----
    op.add_column("broker_connections", sa.Column("id", sa.UUID(), nullable=True))
    op.execute("UPDATE broker_connections SET id = connection_id")
    op.alter_column("broker_connections", "id", nullable=False)

    op.drop_constraint(
        "strategy_deployments_broker_connection_id_fkey",
        "strategy_deployments",
        type_="foreignkey",
    )
    op.drop_column("broker_connections", "connection_id")
    op.create_primary_key("broker_connections_pkey", "broker_connections", ["id"])

    op.create_foreign_key(
        "strategy_deployments_broker_connection_id_fkey",
        "strategy_deployments",
        "broker_connections",
        ["broker_connection_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # ----- strategy_deployments: deployment_id -> id -----
    op.add_column("strategy_deployments", sa.Column("id", sa.UUID(), nullable=True))
    op.execute("UPDATE strategy_deployments SET id = deployment_id")
    op.alter_column("strategy_deployments", "id", nullable=False)

    op.drop_constraint(
        "strategy_deployment_metrics_deployment_id_fkey",
        "strategy_deployment_metrics",
        type_="foreignkey",
    )
    op.drop_constraint(
        "strategy_deployment_orders_deployment_id_fkey",
        "strategy_deployment_orders",
        type_="foreignkey",
    )
    op.drop_constraint(
        "deployment_events_deployment_id_fkey",
        "deployment_events",
        type_="foreignkey",
    )

    op.drop_column("strategy_deployments", "deployment_id")
    op.create_primary_key("strategy_deployments_pkey", "strategy_deployments", ["id"])

    op.create_foreign_key(
        "strategy_deployment_metrics_deployment_id_fkey",
        "strategy_deployment_metrics",
        "strategy_deployments",
        ["deployment_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "strategy_deployment_orders_deployment_id_fkey",
        "strategy_deployment_orders",
        "strategy_deployments",
        ["deployment_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "deployment_events_deployment_id_fkey",
        "deployment_events",
        "strategy_deployments",
        ["deployment_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # ----- strategy_deployments: id -> deployment_id -----
    op.add_column(
        "strategy_deployments", sa.Column("deployment_id", sa.UUID(), nullable=True)
    )
    op.execute("UPDATE strategy_deployments SET deployment_id = id")
    op.alter_column("strategy_deployments", "deployment_id", nullable=False)

    op.drop_constraint(
        "strategy_deployment_metrics_deployment_id_fkey",
        "strategy_deployment_metrics",
        type_="foreignkey",
    )
    op.drop_constraint(
        "strategy_deployment_orders_deployment_id_fkey",
        "strategy_deployment_orders",
        type_="foreignkey",
    )
    op.drop_constraint(
        "deployment_events_deployment_id_fkey", "deployment_events", type_="foreignkey"
    )

    op.drop_column("strategy_deployments", "id")
    op.create_primary_key(
        "strategy_deployments_pkey", "strategy_deployments", ["deployment_id"]
    )

    op.create_foreign_key(
        "strategy_deployment_metrics_deployment_id_fkey",
        "strategy_deployment_metrics",
        "strategy_deployments",
        ["deployment_id"],
        ["deployment_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "strategy_deployment_orders_deployment_id_fkey",
        "strategy_deployment_orders",
        "strategy_deployments",
        ["deployment_id"],
        ["deployment_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "deployment_events_deployment_id_fkey",
        "deployment_events",
        "strategy_deployments",
        ["deployment_id"],
        ["deployment_id"],
        ondelete="CASCADE",
    )

    # ----- broker_connections: id -> connection_id -----
    op.add_column(
        "broker_connections", sa.Column("connection_id", sa.UUID(), nullable=True)
    )
    op.execute("UPDATE broker_connections SET connection_id = id")
    op.alter_column("broker_connections", "connection_id", nullable=False)

    op.drop_constraint(
        "strategy_deployments_broker_connection_id_fkey",
        "strategy_deployments",
        type_="foreignkey",
    )

    op.drop_column("broker_connections", "id")
    op.create_primary_key(
        "broker_connections_pkey", "broker_connections", ["connection_id"]
    )

    op.create_foreign_key(
        "strategy_deployments_broker_connection_id_fkey",
        "strategy_deployments",
        "broker_connections",
        ["broker_connection_id"],
        ["connection_id"],
        ondelete="CASCADE",
    )

    # ----- ohlcs: id -> ohlc_id -----
    op.add_column("ohlcs", sa.Column("ohlc_id", sa.UUID(), nullable=True))
    op.execute("UPDATE ohlcs SET ohlc_id = id")
    op.alter_column("ohlcs", "ohlc_id", nullable=False)
    op.drop_constraint("ohlcs_pkey", "ohlcs", type_="primary")
    op.drop_column("ohlcs", "id")
    op.create_primary_key("ohlcs_pkey", "ohlcs", ["ohlc_id"])

    # ----- strategy: id -> strategy_id -----
    op.add_column("strategy", sa.Column("strategy_id", sa.UUID(), nullable=True))
    op.execute("UPDATE strategy SET strategy_id = id")
    op.alter_column("strategy", "strategy_id", nullable=False)

    op.drop_constraint(
        op.f("backtests_strategy_id_fkey"), "backtests", type_="foreignkey"
    )
    op.drop_constraint(
        op.f("strategy_deployments_strategy_id_fkey"),
        "strategy_deployments",
        type_="foreignkey",
    )
    op.drop_constraint(
        op.f("strategy_versions_strategy_id_fkey"),
        "strategy_versions",
        type_="foreignkey",
    )

    op.drop_column("strategy", "id")
    op.create_primary_key("strategy_pkey", "strategy", ["strategy_id"])

    op.create_foreign_key(
        op.f("backtests_strategy_id_fkey"),
        "backtests",
        "strategy",
        ["strategy_id"],
        ["strategy_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("strategy_deployments_strategy_id_fkey"),
        "strategy_deployments",
        "strategy",
        ["strategy_id"],
        ["strategy_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("strategy_versions_strategy_id_fkey"),
        "strategy_versions",
        "strategy",
        ["strategy_id"],
        ["strategy_id"],
        ondelete="CASCADE",
    )

    # ----- users: id -> user_id -----
    op.add_column("users", sa.Column("user_id", sa.UUID(), nullable=True))
    op.execute("UPDATE users SET user_id = id")
    op.alter_column("users", "user_id", nullable=False)

    op.drop_constraint(op.f("backtests_user_id_fkey"), "backtests", type_="foreignkey")
    op.drop_constraint(
        op.f("broker_connections_user_id_fkey"),
        "broker_connections",
        type_="foreignkey",
    )
    op.drop_constraint(op.f("strategy_user_id_fkey"), "strategy", type_="foreignkey")
    op.drop_constraint(
        op.f("strategy_deployments_user_id_fkey"),
        "strategy_deployments",
        type_="foreignkey",
    )

    op.drop_column("users", "id")
    op.create_primary_key("users_pkey", "users", ["user_id"])

    op.add_column("strategy_versions", sa.Column("user_id", sa.UUID(), nullable=True))

    op.create_foreign_key(
        op.f("backtests_user_id_fkey"),
        "backtests",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("broker_connections_user_id_fkey"),
        "broker_connections",
        "users",
        ["user_id"],
        ["user_id"],
    )
    op.create_foreign_key(
        op.f("strategy_user_id_fkey"),
        "strategy",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("strategy_deployments_user_id_fkey"),
        "strategy_deployments",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )
