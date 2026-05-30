# Vegate

## Core Concepts

### BaseStrategy

Every strategy extends `BaseStrategy`, which provides lifecycle hooks and client interfaces out of the box:

| Hook                | Description                                             |
| ------------------- | ------------------------------------------------------- |
| `startup()`         | Called once on initialisation - subscribe to feeds here |
| `on_candle(candle)` | Called on each new OHLC candle from subscribed feeds    |
| `shutdown()`        | Called on teardown - clean up open orders here          |

### Clients

Inside your strategy, two clients are available:

- **`self.ohlc_feed_client`** - subscribe to real-time OHLC data streams
- **`self.historical_data_client`** - Fetch past OHLC candles
- **`self.oms_client`** - place, cancel, and query orders and positions via the Order Management System

---

## Example Strategy

The strategy below demonstrates a simple alternating buy/sell pattern on `ETH/USD` using 1-minute candles via Alpaca's crypto feed.

```python
from vegate.oms.schema import OrderRequest, Order
from vegate.oms.enums import OrderType, OrderSide
from vegate.markets.enums import Timeframe
from vegate.strategy.base import BaseStrategy


class UserStrategy(BaseStrategy):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._order: Order = None

    def startup(self):
        self.ohlc_feed_client.subscribe(
            [
                {
                    "symbol": "ETH/USD",
                    "broker_type": "alpaca",
                    "market_type": "crypto",
                    "timeframe": [Timeframe.m1],
                }
            ]
        )

    def on_candle(self, candle):
        if self._order is None:
            # No open position - buy 1 ETH at market
            self._order = self.oms_client.place_order(
                OrderRequest(
                    symbol="ETH/USD",
                    side="buy",
                    order_type="market",
                    quantity=1,
                ),
                candle.timestamp
            )
        else:
            # Position is open - close it entirely
            position = self.oms_client.get_position("ETH/USD")
            self.oms_client.place_order(
                OrderRequest(
                    symbol="ETH/USD",
                    order_type="market",
                    side="sell",
                    quantity=position,
                ),
                candle.timestamp
            )
            self._order = None

    def shutdown(self):
        self.oms_client.cancel_all_orders()
```

### How it works

1. **`startup()`** - Subscribes to the `ETH/USD` 1-minute candle feed on Alpaca.
2. **`on_candle()`** - On each new candle, alternates between:
   - Placing a market **buy** for 1 ETH when no position is held
   - Querying the current position size and placing a market **sell** to close it fully
3. **`shutdown()`** - Cancels all outstanding orders on exit to avoid orphaned positions.

!!! tip "Extending this strategy"
    This example is intentionally minimal. From here you might add entry signals based on technical indicators, position sizing logic, or risk management filters before placing orders.
