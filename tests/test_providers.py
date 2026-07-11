from __future__ import annotations

from typing import cast

from core import TradingProvider
from oanda import (
    OandaAccountManager,
    OandaBroker,
    OandaDataSource,
    OandaGateway,
    OandaProvider,
    OandaSettings,
)
from pydantic import SecretStr

from server.providers import (
    ProviderFactory,
    ProviderName,
)


class TestProviders:
    def test_create_oanda_provider_from_provider_enum(self) -> None:
        settings = OandaSettings(account_id="001-001-1234567-001", access_token=SecretStr("token"))

        provider = ProviderFactory().create(ProviderName.OANDA, settings=settings)
        account_manager = cast(OandaAccountManager, provider.account_manager)
        broker = cast(OandaBroker, provider.broker)
        data_source = cast(OandaDataSource, provider.data)

        assert isinstance(provider, TradingProvider)
        assert isinstance(provider, OandaProvider)
        assert isinstance(account_manager, OandaAccountManager)
        assert isinstance(broker, OandaBroker)
        assert isinstance(data_source, OandaDataSource)
        assert isinstance(broker.gateway, OandaGateway)
        assert account_manager.gateway is broker.gateway
        assert data_source.gateway is broker.gateway

    def test_provider_factory_creates_oanda_provider(self) -> None:
        settings = OandaSettings(account_id="001-001-1234567-001", access_token=SecretStr("token"))

        provider = ProviderFactory().create(ProviderName.OANDA, settings=settings)

        assert isinstance(provider, OandaProvider)
