"""Server package for AutoForexV2."""

from importlib.metadata import version

from auto_forex_server.events import BrokerEventHandler, EventBus, RecordingEventHandler
from auto_forex_server.tasks import InMemoryTaskRepository, TaskManager

__all__ = [
    "BrokerEventHandler",
    "EventBus",
    "InMemoryTaskRepository",
    "RecordingEventHandler",
    "TaskManager",
    "__version__",
]

__version__ = version("auto-forex-server")
