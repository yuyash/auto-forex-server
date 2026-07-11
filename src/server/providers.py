"""Provider service factory for server runtime wiring."""

from __future__ import annotations

from enum import StrEnum

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
        settings: OandaSettings,
    ) -> TradingProvider:
        """Create provider-specific service implementations."""
        if provider == ProviderName.OANDA:
            return self._create_oanda_provider(settings)

        msg = f"unsupported account provider: {provider.value}"
        raise ValueError(msg)

    def _create_oanda_provider(self, settings: OandaSettings) -> TradingProvider:
        return OandaProvider.from_settings(settings)
