"""Server package for AutoForexV2."""

from importlib.metadata import version

from auto_forex_server.events import BrokerEventHandler, EventBus, RecordingEventHandler
from auto_forex_server.providers import (
    ProviderFactory,
    ProviderName,
    create_provider,
)
from auto_forex_server.tasks import InMemoryTaskRepository, TaskManager

__all__ = [
    "BrokerEventHandler",
    "EventBus",
    "InMemoryTaskRepository",
    "ProviderFactory",
    "ProviderName",
    "RecordingEventHandler",
    "TaskManager",
    "__version__",
    "create_provider",
]

__version__ = version("auto-forex-server")
