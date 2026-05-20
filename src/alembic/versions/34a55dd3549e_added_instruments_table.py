"""Added instruments table

Revision ID: 34a55dd3549e
Revises: 26a470cde1db
Create Date: 2026-05-19 08:25:07.475718

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "34a55dd3549e"
down_revision: Union[str, Sequence[str], None] = "26a470cde1db"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        "instruments",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("native_symbol", sa.String(), nullable=False),
        sa.Column("broker_type", sa.String(), nullable=False),
        sa.Column("market_type", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_unique_constraint(
        "unq_symbol_market_type_broker_type",
        "instruments",
        ["symbol", "market_type", "broker_type"],
    )

    op.add_column(
        "ohlcs",
        sa.Column("instrument_id", sa.UUID(), nullable=True),
    )

    op.execute("""
        INSERT INTO instruments (
            id,
            symbol,
            native_symbol,
            broker_type,
            market_type
        )
        SELECT
            gen_random_uuid(),
            REPLACE(symbol, '/', '') AS symbol,
            symbol AS native_symbol,
            source AS broker_type,
            market_type
        FROM (
            SELECT DISTINCT
                symbol,
                source,
                market_type
            FROM ohlcs
        ) t
    """)

    op.execute("""
        UPDATE ohlcs o
        SET instrument_id = i.id
        FROM instruments i
        WHERE
            i.native_symbol = o.symbol
            AND i.broker_type = o.source
            AND i.market_type = o.market_type
    """)

    op.alter_column("ohlcs", "instrument_id", nullable=False)

    op.create_foreign_key(
        "fk_ohlcs_instrument_id",
        "ohlcs",
        "instruments",
        ["instrument_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.drop_index(op.f("idx_ohlc_source_symbol"), table_name="ohlcs")

    op.drop_column("ohlcs", "market_type")
    op.drop_column("ohlcs", "source")
    op.drop_column("ohlcs", "symbol")


def downgrade() -> None:
    """Downgrade schema."""

    op.add_column(
        "ohlcs",
        sa.Column("symbol", sa.String(), nullable=True),
    )

    op.add_column(
        "ohlcs",
        sa.Column("source", sa.String(), nullable=True),
    )

    op.add_column(
        "ohlcs",
        sa.Column("market_type", sa.String(), nullable=True),
    )

    op.execute("""
        UPDATE ohlcs o
        SET
            symbol = i.native_symbol,
            source = i.broker_type,
            market_type = i.market_type
        FROM instruments i
        WHERE o.instrument_id = i.id
    """)

    op.alter_column("ohlcs", "symbol", nullable=False)
    op.alter_column("ohlcs", "source", nullable=False)
    op.alter_column("ohlcs", "market_type", nullable=False)

    op.create_index(
        op.f("idx_ohlc_source_symbol"),
        "ohlcs",
        ["source", "symbol"],
        unique=False,
    )

    op.drop_constraint(
        "fk_ohlcs_instrument_id",
        "ohlcs",
        type_="foreignkey",
    )

    op.drop_column("ohlcs", "instrument_id")

    op.drop_table("instruments")
