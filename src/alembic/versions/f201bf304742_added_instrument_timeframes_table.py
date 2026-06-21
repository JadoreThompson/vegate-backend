"""Added instrument_timeframes table

Revision ID: f201bf304742
Revises: a9711982901c
Create Date: 2026-06-21 13:15:04.485625

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f201bf304742'
down_revision: Union[str, Sequence[str], None] = 'a9711982901c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('instrument_timeframes',
    sa.Column('instrument_id', sa.UUID(), nullable=False),
    sa.Column('timeframe', sa.String(), nullable=False),
    sa.Column('start_ts', sa.Integer(), nullable=False),
    sa.Column('end_ts', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['instrument_id'], ['instruments.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('instrument_id', 'timeframe')
    )

    op.execute("""
        INSERT INTO instrument_timeframes (instrument_id, timeframe, start_ts, end_ts)
        SELECT
            ohlc.instrument_id,
            ohlc.timeframe,
            MIN(ohlc.timestamp) AS start_ts,
            MAX(ohlc.timestamp) AS end_ts
        FROM ohlcs AS ohlc
        GROUP BY ohlc.instrument_id, ohlc.timeframe
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('instrument_timeframes')
