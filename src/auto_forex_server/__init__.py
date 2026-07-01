"""Server package for AutoForexV2."""

from importlib.metadata import version

from auto_forex_server.events import BrokerEventHandler, EventBus, RecordingEventHandler
from auto_forex_server.providers import ProviderServices, create_provider, create_provider_services
from auto_forex_server.tasks import InMemoryTaskRepository, TaskManager

__all__ = [
    "BrokerEventHandler",
    "EventBus",
    "InMemoryTaskRepository",
    "ProviderServices",
    "RecordingEventHandler",
    "TaskManager",
    "__version__",
    "create_provider",
    "create_provider_services",
]

__version__ = version("auto-forex-server")
