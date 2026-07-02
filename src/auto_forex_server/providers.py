"""Provider service factory for server runtime wiring."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, cast

from core import TradingProvider
from oanda import OandaProvider, OandaSettings


class ProviderName(StrEnum):
    """Provider names supported by the server runtime."""

    OANDA = "oanda"


class ProviderFactory:
    """Create provider-specific service bundles for server runtime wiring."""

    def create(
        self,
        provider: ProviderName,
        *,
        settings: object | None = None,
    ) -> TradingProvider:
        """Create provider-specific service implementations."""
        if provider == ProviderName.OANDA:
            return self._create_oanda_provider(settings)

        msg = f"unsupported account provider: {provider.value}"
        raise ValueError(msg)

    def _create_oanda_provider(self, settings: object | None) -> TradingProvider:
        if settings is None:
            settings = cast(Any, OandaSettings)()
        if not isinstance(settings, OandaSettings):
            msg = "OANDA provider requires OandaSettings"
            raise TypeError(msg)

        return OandaProvider.from_settings(settings)


def create_provider(
    provider: ProviderName,
    *,
    settings: object | None = None,
) -> TradingProvider:
    """Create provider-specific service implementations."""
    return ProviderFactory().create(provider, settings=settings)
