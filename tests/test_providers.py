from __future__ import annotations

from typing import cast

import pytest
from core import AccountProvider, TradingProvider
from oanda import (
    OandaAccountManager,
    OandaBroker,
    OandaDataSource,
    OandaGateway,
    OandaProvider,
    OandaSettings,
)
from pydantic import SecretStr

from auto_forex_server.providers import create_provider, create_provider_services


def test_create_oanda_provider_from_provider_enum() -> None:
    settings = OandaSettings(account_id="001-001-1234567-001", access_token=SecretStr("token"))

    provider = create_provider(AccountProvider.OANDA, settings=settings)
    account_manager = cast(OandaAccountManager, provider.account_manager)
    broker = cast(OandaBroker, provider.broker)
    data_source = cast(OandaDataSource, provider.data_source)

    assert isinstance(provider, TradingProvider)
    assert isinstance(provider, OandaProvider)
    assert isinstance(account_manager, OandaAccountManager)
    assert isinstance(broker, OandaBroker)
    assert isinstance(data_source, OandaDataSource)
    assert isinstance(broker.gateway, OandaGateway)
    assert account_manager.gateway is broker.gateway
    assert data_source.gateway is broker.gateway


def test_create_provider_services_delegates_to_create_provider() -> None:
    settings = OandaSettings(account_id="001-001-1234567-001", access_token=SecretStr("token"))

    provider = create_provider_services(AccountProvider.OANDA, settings=settings)

    assert isinstance(provider, OandaProvider)


def test_create_oanda_provider_services_rejects_wrong_settings_type() -> None:
    with pytest.raises(TypeError, match="OANDA provider requires OandaSettings"):
        create_provider(AccountProvider.OANDA, settings=object())
