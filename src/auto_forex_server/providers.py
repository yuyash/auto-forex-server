"""Provider service factory for server runtime wiring."""

from __future__ import annotations

from typing import Any, cast

from core import AccountProvider, TradingProvider
from oanda import OandaProvider, OandaSettings

ProviderServices = TradingProvider


def create_provider(
    provider: AccountProvider,
    *,
    settings: object | None = None,
) -> TradingProvider:
    """Create provider-specific service implementations from a Core provider enum."""
    if provider == AccountProvider.OANDA:
        return _create_oanda_provider(settings)

    msg = f"unsupported account provider: {provider}"
    raise ValueError(msg)


def create_provider_services(
    provider: AccountProvider,
    *,
    settings: object | None = None,
) -> TradingProvider:
    """Create provider-specific services.

    Use :func:`create_provider` for new code.
    """
    return create_provider(provider, settings=settings)


def _create_oanda_provider(settings: object | None) -> TradingProvider:
    if settings is None:
        settings = cast(Any, OandaSettings)()
    if not isinstance(settings, OandaSettings):
        msg = "OANDA provider requires OandaSettings"
        raise TypeError(msg)

    return OandaProvider.from_settings(settings)
