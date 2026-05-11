import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Date, ForeignKey, Integer, String, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from enums import BacktestStatus, BrokerType, Timeframe
from infra.db.model.backtest_order import BacktestOrder
from infra.db.model.base import Base, datetime_tz, uuid_pk

if TYPE_CHECKING:
    from infra.db.model.backtest_metric import BacktestMetric
    from infra.db.model.orders import Orders
    from infra.db.model.strategies import Strategies


class Backtest(Base):
    __tablename__ = "backtests"

    id: Mapped[uuid.UUID] = uuid_pk()
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    # Broker is needed to know which data source to use for backtesting
    broker: Mapped[BrokerType] = mapped_column(String, nullable=False)
    starting_balance: Mapped[int] = mapped_column(Integer, nullable=False)
    start_date: Mapped[date] = mapped_column(Date(), nullable=False)
    end_date: Mapped[date] = mapped_column(Date(), nullable=False)
    timeframe: Mapped[Timeframe] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=BacktestStatus.PENDING.value
    )
    created_at: Mapped[datetime] = datetime_tz()

    # Relationships
    strategy: Mapped["Strategies"] = relationship(
        back_populates="backtests", passive_deletes=True
    )
    orders: Mapped[list["BacktestOrder"]] = relationship(
        back_populates="backtest", cascade="all, delete-orphan", passive_deletes=True
    )
    metrics: Mapped["BacktestMetric"] = relationship(
        back_populates="backtest", uselist=False, cascade="all, delete-orphan"
    )
