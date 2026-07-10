"""Server package for AutoForexV2."""

from importlib.metadata import version

from server.providers import (
    ProviderFactory,
    ProviderName,
    create_provider,
)

__all__ = [
    "ProviderFactory",
    "ProviderName",
    "__version__",
    "create_provider",
]

__version__ = version("auto-forex-server")
