from .runner_config import RunnerConfig


def run_runner(config: RunnerConfig) -> None:
    """Instantiate and run a runner based on the provided configuration.

    Args:
        config: RunnerConfig instance containing runner class, args, and kwargs
    """
    runner = config.cls(*config.args, **config.kwargs)
    runner.run()
