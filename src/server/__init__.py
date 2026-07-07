"""Server package for AutoForexV2."""

from importlib.metadata import version

from server.events import EventBus, RecordingEventHandler
from server.providers import (
    ProviderFactory,
    ProviderName,
    create_provider,
)
from server.tasks import InMemoryTaskRepository, TaskManager

__all__ = [
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
