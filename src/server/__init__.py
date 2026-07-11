"""Server package for AutoForexV2."""

from importlib.metadata import version

from server.providers import (
    ProviderFactory,
    ProviderName,
)

__all__ = [
    "ProviderFactory",
    "ProviderName",
    "__version__",
]

__version__ = version("auto-forex-server")
