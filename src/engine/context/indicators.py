import logging
from typing import Tuple
import numpy as np

logger = logging.getLogger(__name__)


class IndicatorMixin:
    """
    Mixin providing technical indicator calculations.

    This mixin can be added to StrategyContext to provide common technical
    indicators. All indicators fetch historical data via self.history() and
    perform calculations using numpy for efficiency.

    Example:
        class EnhancedContext(StrategyContext, IndicatorMixin):
            pass

        def strategy(ctx: EnhancedContext):
            sma_20 = ctx.sma('AAPL', period=20)
            rsi_14 = ctx.rsi('AAPL', period=14)

            if rsi_14 < 30 and ctx.close('AAPL') > sma_20:
                ctx.buy('AAPL', quantity=10)
    """

    def sma(self, symbol: str, period: int = 20) -> float:
        """
        Calculate Simple Moving Average.

        The SMA is the average of the closing prices over a specified period.
        It's a lagging indicator that smooths out price data.

        Args:
            symbol: Trading symbol
            period: Number of periods for the average (default: 20)

        Returns:
            Simple moving average value

        Raises:
            ValueError: If period <= 0 or insufficient data
            Exception: If historical data cannot be fetched

        Example:
            sma_50 = ctx.sma('AAPL', period=50)
            if ctx.close('AAPL') > sma_50:
                print("Price above 50-day SMA")
        """
        if period <= 0:
            raise ValueError("period must be positive")

        try:
            hist = self.history(symbol, bars=period)

            if len(hist) < period:
                raise ValueError(
                    f"Insufficient data: need {period} bars, got {len(hist)}"
                )

            closes = np.array(hist["close"])
            sma_value = float(np.mean(closes))

            logger.debug(f"SMA({period}) for {symbol}: {sma_value:.2f}")
            return sma_value

        except Exception as e:
            logger.error(f"Error calculating SMA for {symbol}: {e}", exc_info=True)
            raise

    def ema(self, symbol: str, period: int = 20) -> float:
        """
        Calculate Exponential Moving Average.

        The EMA gives more weight to recent prices, making it more responsive
        to price changes than the SMA. Uses standard EMA calculation with
        multiplier = 2 / (period + 1).

        Args:
            symbol: Trading symbol
            period: Number of periods for the average (default: 20)

        Returns:
            Exponential moving average value

        Raises:
            ValueError: If period <= 0 or insufficient data
            Exception: If historical data cannot be fetched

        Example:
            ema_12 = ctx.ema('AAPL', period=12)
            ema_26 = ctx.ema('AAPL', period=26)
            if ema_12 > ema_26:
                print("Bullish crossover")
        """
        if period <= 0:
            raise ValueError("period must be positive")

        try:
            # Fetch extra bars for EMA warmup
            hist = self.history(symbol, bars=period * 2)

            if len(hist) < period:
                raise ValueError(
                    f"Insufficient data: need at least {period} bars, got {len(hist)}"
                )

            closes = np.array(hist["close"])

            # EMA calculation
            multiplier = 2.0 / (period + 1)
            ema_value = closes[0]  # Start with first close

            for price in closes[1:]:
                ema_value = (price - ema_value) * multiplier + ema_value

            ema_value = float(ema_value)
            logger.debug(f"EMA({period}) for {symbol}: {ema_value:.2f}")
            return ema_value

        except Exception as e:
            logger.error(f"Error calculating EMA for {symbol}: {e}", exc_info=True)
            raise

    def rsi(self, symbol: str, period: int = 14) -> float:
        """
        Calculate Relative Strength Index.

        The RSI is a momentum oscillator that measures the speed and magnitude
        of price changes. Values range from 0 to 100, with readings above 70
        typically considered overbought and below 30 oversold.

        Args:
            symbol: Trading symbol
            period: Number of periods for RSI calculation (default: 14)

        Returns:
            RSI value between 0 and 100

        Raises:
            ValueError: If period <= 0 or insufficient data
            Exception: If historical data cannot be fetched

        Example:
            rsi = ctx.rsi('AAPL', period=14)
            if rsi < 30:
                print("Oversold condition")
            elif rsi > 70:
                print("Overbought condition")
        """
        if period <= 0:
            raise ValueError("period must be positive")

        try:
            # Need period + 1 bars to calculate period changes
            hist = self.history(symbol, bars=period + 1)

            if len(hist) < period + 1:
                raise ValueError(
                    f"Insufficient data: need {period + 1} bars, got {len(hist)}"
                )

            closes = np.array(hist["close"])

            # Calculate price changes
            deltas = np.diff(closes)

            # Separate gains and losses
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)

            # Calculate average gain and loss
            avg_gain = np.mean(gains)
            avg_loss = np.mean(losses)

            # Avoid division by zero
            if avg_loss == 0:
                rsi_value = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi_value = 100.0 - (100.0 / (1.0 + rs))

            logger.debug(f"RSI({period}) for {symbol}: {rsi_value:.2f}")
            return float(rsi_value)

        except Exception as e:
            logger.error(f"Error calculating RSI for {symbol}: {e}", exc_info=True)
            raise

    def macd(
        self, symbol: str, fast: int = 12, slow: int = 26, signal: int = 9
    ) -> Tuple[float, float, float]:
        """
        Calculate MACD (Moving Average Convergence Divergence).

        MACD is a trend-following momentum indicator that shows the relationship
        between two moving averages. Returns the MACD line, signal line, and
        histogram (MACD - signal).

        Args:
            symbol: Trading symbol
            fast: Fast EMA period (default: 12)
            slow: Slow EMA period (default: 26)
            signal: Signal line EMA period (default: 9)

        Returns:
            Tuple of (macd_line, signal_line, histogram)

        Raises:
            ValueError: If periods invalid or insufficient data
            Exception: If historical data cannot be fetched

        Example:
            macd_line, signal_line, histogram = ctx.macd('AAPL')
            if macd_line > signal_line and histogram > 0:
                print("Bullish MACD signal")
        """
        if fast <= 0 or slow <= 0 or signal <= 0:
            raise ValueError("All periods must be positive")

        if fast >= slow:
            raise ValueError("fast period must be less than slow period")

        try:
            # Need enough bars for slow EMA plus signal calculation
            bars_needed = slow * 2 + signal
            hist = self.history(symbol, bars=bars_needed)

            if len(hist) < bars_needed:
                raise ValueError(
                    f"Insufficient data: need {bars_needed} bars, got {len(hist)}"
                )

            closes = np.array(hist["close"])

            # Calculate fast EMA
            fast_multiplier = 2.0 / (fast + 1)
            fast_ema = closes[0]
            fast_emas = []
            for price in closes:
                fast_ema = (price - fast_ema) * fast_multiplier + fast_ema
                fast_emas.append(fast_ema)

            # Calculate slow EMA
            slow_multiplier = 2.0 / (slow + 1)
            slow_ema = closes[0]
            slow_emas = []
            for price in closes:
                slow_ema = (price - slow_ema) * slow_multiplier + slow_ema
                slow_emas.append(slow_ema)

            # Calculate MACD line
            macd_line_values = np.array(fast_emas) - np.array(slow_emas)

            # Calculate signal line (EMA of MACD line)
            signal_multiplier = 2.0 / (signal + 1)
            signal_line = macd_line_values[0]
            for macd_val in macd_line_values[1:]:
                signal_line = (macd_val - signal_line) * signal_multiplier + signal_line

            # Current values
            macd_current = float(macd_line_values[-1])
            signal_current = float(signal_line)
            histogram = macd_current - signal_current

            logger.debug(
                f"MACD for {symbol}: line={macd_current:.2f}, "
                f"signal={signal_current:.2f}, histogram={histogram:.2f}"
            )

            return macd_current, signal_current, histogram

        except Exception as e:
            logger.error(f"Error calculating MACD for {symbol}: {e}", exc_info=True)
            raise

    def bollinger_bands(
        self, symbol: str, period: int = 20, std_dev: float = 2.0
    ) -> Tuple[float, float, float]:
        """
        Calculate Bollinger Bands.

        Bollinger Bands consist of a middle band (SMA) and upper/lower bands
        that are standard deviations away from the middle band. They help
        identify volatility and potential overbought/oversold conditions.

        Args:
            symbol: Trading symbol
            period: Number of periods for SMA (default: 20)
            std_dev: Number of standard deviations (default: 2.0)

        Returns:
            Tuple of (upper_band, middle_band, lower_band)

        Raises:
            ValueError: If period <= 0 or std_dev <= 0 or insufficient data
            Exception: If historical data cannot be fetched

        Example:
            upper, middle, lower = ctx.bollinger_bands('AAPL', period=20)
            current_price = ctx.close('AAPL')

            if current_price < lower:
                print("Price below lower Bollinger Band - potential buy signal")
            elif current_price > upper:
                print("Price above upper Bollinger Band - potential sell signal")
        """
        if period <= 0:
            raise ValueError("period must be positive")

        if std_dev <= 0:
            raise ValueError("std_dev must be positive")

        try:
            hist = self.history(symbol, bars=period)

            if len(hist) < period:
                raise ValueError(
                    f"Insufficient data: need {period} bars, got {len(hist)}"
                )

            closes = np.array(hist["close"])

            # Calculate middle band (SMA)
            middle_band = float(np.mean(closes))

            # Calculate standard deviation
            std = float(np.std(closes, ddof=0))

            # Calculate upper and lower bands
            upper_band = middle_band + (std_dev * std)
            lower_band = middle_band - (std_dev * std)

            logger.debug(
                f"Bollinger Bands for {symbol}: "
                f"upper={upper_band:.2f}, middle={middle_band:.2f}, "
                f"lower={lower_band:.2f}"
            )

            return upper_band, middle_band, lower_band

        except Exception as e:
            logger.error(
                f"Error calculating Bollinger Bands for {symbol}: {e}", exc_info=True
            )
            raise
