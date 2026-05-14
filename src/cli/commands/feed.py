import sys
import logging

import click

from runners import BacktestRunner
from runners.market_feed_runner import MarketFeedRunner 
from runners.runner_config import RunnerConfig
from runners.utils import run_runner

logger = logging.getLogger("commands.backtest")


@click.group()
def feed():
    """Manage backtests."""
    pass


@feed.command(name="run")
@click.option("--host", type=str, required=True, help="Server host")
@click.option("--port", type=int, required=True, help="Server port")
def run(host, port):
    runner_config = RunnerConfig(cls=MarketFeedRunner, args=(host, port))
    run_runner(runner_config)
