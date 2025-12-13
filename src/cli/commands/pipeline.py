"""Pipeline commands for tick data ingestion."""

import sys
import asyncio
import logging

import click

from engine.enums import BrokerType, MarketType
from pipelines import AlpacaPipeline

logger = logging.getLogger("commands.pipeline")


@click.group()
def pipeline():
    """Manage tick data ingestion pipelines."""
    pass


@pipeline.command(name="run")
@click.option(
    "--broker",
    type=click.Choice([b.value for b in BrokerType], case_sensitive=True),
    required=True,
    help="Broker to ingest ticks from",
)
@click.option(
    "--market",
    type=click.Choice([m.value for m in MarketType], case_sensitive=True),
    required=True,
    help="Market type the symbol is in",
)
@click.option(
    "--symbol",
    type=str,
    required=True,
    help="Symbol to ingest (e.g., BTC/USD, AAPL)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging output",
)
def pipeline_run(broker, market, symbol, verbose):
    """
    Run a tick data ingestion pipeline for a specific symbol.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    broker_enum = BrokerType(broker)
    market_enum = MarketType(market)

    click.echo(f"Starting {market} pipeline for {symbol} via {broker}")

    try:
        if broker_enum == BrokerType.ALPACA:
            inst = AlpacaPipeline()
        else:
            raise ValueError(f"Unsupported broker type: {broker}")

        async def run_pipeline():
            async with inst:
                if market_enum == MarketType.CRYPTO:
                    await inst.run_crypto_pipeline(symbol)
                elif market_enum == MarketType.STOCKS:
                    await inst.run_stocks_pipeline(symbol)
                else:
                    raise ValueError(f"Unsupported market type: {market}")

        asyncio.run(run_pipeline())

    except KeyboardInterrupt:
        click.echo("\nPipeline stopped by user")
    except Exception as e:
        click.echo(f"Error running pipeline: {e}", err=True)
        logger.exception("Pipeline failed")
        sys.exit(1)


@pipeline.command(name="list")
def pipeline_list():
    """
    List available brokers and market types.

    Shows all supported broker and market combinations that can be used
    with the 'pipeline run' command.
    """
    click.echo("Available Brokers:")
    for broker in BrokerType:
        click.echo(f"  • {broker.value}")

    click.echo("\nAvailable Market Types:")
    for market in MarketType:
        click.echo(f"  • {market.value}")

    click.echo("\nExample usage:")
    click.echo("  vegate pipeline run --broker alpaca --market crypto --symbol BTC/USD")
    click.echo("  vegate pipeline run --broker alpaca --market stocks --symbol AAPL")
