from abc import ABC, abstractmethod


class BaseRunner(ABC):
    """
    Abstract base class for a long-running process.
    Each runner should be designed to be the target of a multiprocessing.Process.
    """

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def run(self) -> None:
        """The main entry point for the process."""
        ...
