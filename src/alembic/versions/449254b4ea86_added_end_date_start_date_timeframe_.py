"""added end_date, start_date, timeframe fields to backtests

Revision ID: 449254b4ea86
Revises: 637030fea793
Create Date: 2025-12-08 15:24:31.565748

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "449254b4ea86"
down_revision: Union[str, Sequence[str], None] = "637030fea793"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():

    op.execute(
        """
        ALTER TABLE backtests 
        ADD COLUMN start_date DATE;
    """
    )

    op.execute(
        """
        ALTER TABLE backtests 
        ADD COLUMN end_date DATE;
    """
    )

    op.execute(
        """
        ALTER TABLE backtests 
        ADD COLUMN timeframe TEXT;
    """
    )

    op.execute(
        """
        UPDATE backtests
        SET 
            start_date = (NOW() - INTERVAL '5 years')::date,
            end_date = NOW()::date,
            timeframe = '15m'
    """
    )

    op.execute(
        """
        ALTER TABLE backtests
        ALTER COLUMN start_date SET NOT NULL;
    """
    )

    op.execute(
        """
        ALTER TABLE backtests
        ALTER COLUMN end_date SET NOT NULL;
    """
    )

    op.execute(
        """
        ALTER TABLE backtests
        ALTER COLUMN timeframe SET NOT NULL;
    """
    )


def downgrade():
    op.execute("ALTER TABLE backtests DROP COLUMN timeframe;")
    op.execute("ALTER TABLE backtests DROP COLUMN end_date;")
    op.execute("ALTER TABLE backtests DROP COLUMN start_date;")
