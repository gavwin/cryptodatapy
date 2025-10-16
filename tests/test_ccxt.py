from unittest.mock import AsyncMock

import ccxt
import ccxt.async_support as ccxt_async
import pandas as pd
import pytest

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.ccxt_api import CCXT
from cryptodatapy.transform import ConvertParams


class TestCCXT:
    """
    Test CCXT class.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.ccxt_instance = CCXT(
            exchanges=["bitget", "bitmex", "okx", "coinbase", "kraken"],
        )
        # self.ccxt_instance.exchange = Mock()
        self.ccxt_instance.exchange = AsyncMock()
        self.ccxt_instance.exchange.id = "bitget"
        self.ccxt_instance.exchange.has = {
            "fetchOHLCV": True,
            "fetchFundingRateHistory": True,
            "fetchOpenInterestHistory": True,
        }
        self.ccxt_instance.exchange.rateLimit = 1000
        self.ccxt_instance.markets = ["BTC/USDT", "BTC/USDT:USDT", "BTC/USDC"]
        self.data_req = DataRequest(
            source="ccxt",
            exch="bitget",
            tickers=["btc"],
            fields=["open", "high", "low", "close", "volume", "funding_rate", "oi"],
            freq="1h",
            mkt_type="perpetual_future",
            start_date=pd.Timestamp.utcnow() - pd.Timedelta(days=1),
        )
        self.data_req = ConvertParams(self.data_req).to_ccxt()
        self.ccxt_instance.exchange.fetch_ohlcv.return_value = [
            [1625097600000, 35045.0, 35057.57, 34966.09, 34980.47, 100.833562],
            [1625097660000, 34975.37, 35022.4, 34974.08, 34974.51, 53.304605],
        ]
        self.ccxt_instance.exchange.fetch_funding_rate_history.return_value = [
            {
                "info": {
                    "symbol": "BTCUSDT",
                    "fundingTime": "1625097600000",
                    "fundingRate": "0.00010000",
                    "markPrice": "",
                },
                "symbol": "BTC/USDT:USDT",
                "fundingRate": 0.0001,
                "timestamp": 1625097600000,
                "datetime": "2021-07-01T00:00:00.000Z",
            }
        ]
        self.ccxt_instance.exchange.fetch_open_interest_history.return_value = [
            {
                "symbol": "BTC/USDT:USDT",
                "baseVolume": 85765.538,
                "quoteVolume": 9063264651.5962,
                "openInterestAmount": 85765.538,
                "openInterestValue": 9063264651.5962,
                "timestamp": 1737525600000,
                "datetime": "2025-01-22T06:00:00.000Z",
                "info": {
                    "symbol": "BTCUSDT",
                    "sumOpenInterest": "85765.53800000",
                    "sumOpenInterestValue": "9063264651.59620000",
                    "timestamp": "1737525600000",
                },
            }
        ]

    def test_initialization(self):
        """
        Test initialization of CCXT class.
        """
        # types
        assert isinstance(self.ccxt_instance, CCXT)
        assert isinstance(self.ccxt_instance.categories, list)
        assert isinstance(self.ccxt_instance.market_types, list)
        assert isinstance(self.ccxt_instance.max_obs_per_call, int)
        # values
        assert self.ccxt_instance.categories == ["crypto"]
        assert self.ccxt_instance.market_types == [
            "spot",
            "future",
            "perpetual_future",
            "option",
        ]
        assert self.ccxt_instance.max_obs_per_call == 1000

    def test_get_exchanges_info(self):
        """
        Test get exchanges method.
        """
        exchanges = self.ccxt_instance.get_exchanges_info()
        # types
        assert isinstance(exchanges, list), "Exchanges should be a list."
        # values
        assert "bitget" in exchanges

    @pytest.mark.asyncio
    async def test_fetch_ohlcv(self):
        data = await self.ccxt_instance._fetch_ohlcv_async(
            ticker="BTC/USDT",
            freq="1m",
            start_date=1625097600000,
            end_date=1625097660000,
            exch="bitget",
        )

        assert isinstance(data, list), "Data should be a list"
        assert len(data) == 1
        assert data[0][0] == 1625097600000
        assert data[0][4] == 34980.38

    @pytest.mark.asyncio
    async def test_fetch_funding_rate(self):
        data = await self.ccxt_instance._fetch_funding_rates_async(
            ticker="BTC/USDT:USDT",
            start_date=1625097600000,
            end_date=1625097660000,
            exch="okx",
        )

        assert len(data) == 283
        assert data[0]["symbol"] == "BTC/USDT:USDT"
        assert data[0]["fundingRate"] == 0.00010000
        assert data[0]["timestamp"] == 1752480000000
        assert data[0]["datetime"] == "2025-07-14T08:00:00.000Z"

    @pytest.mark.asyncio
    async def test_fetch_open_interest(self):
        data = await self.ccxt_instance._fetch_open_interest_async(
            ticker="BTC/USDT:USDT",
            freq="1h",
            start_date=176062700000,
            end_date=1760627347783,
            exch="okx",
        )
        assert len(data) == 720
        assert data[0]["openInterestValue"] == 4389156920.734
        assert data[0]["timestamp"] == 1758038400000
        assert data[0]["datetime"] == "2025-09-16T16:00:00.000Z"

    @pytest.mark.asyncio
    async def test_fetch_tidy_ohlcv(self, exch="okx"):
        """
        Test fetch_tidy_ohlcv method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()

        df = await self.ccxt_instance.fetch_tidy_ohlcv_async(self.data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), (
            "Index is not DatetimeIndex."
        )
        assert list(df.columns) == ["open", "high", "low", "close", "volume"], (
            "Fields are missing from dataframe."
        )
        assert (df.dtypes == "Float64").all(), "Data types are not float64."

    @pytest.mark.asyncio
    async def test_fetch_tidy_funding_rates(self, exch="okx"):
        """
        Test fetch_tidy_funding_rates method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()

        df = await self.ccxt_instance.fetch_tidy_funding_rates_async(self.data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), (
            "Index is not DatetimeIndex."
        )
        assert list(df.columns) == ["funding_rate"], (
            "Fields are missing from dataframe."
        )
        assert (df.dtypes == "Float64").all(), "Data types are not float64."

    @pytest.mark.asyncio
    async def test_fetch_tidy_open_interest(self, exch="okx"):
        """
        Test fetch_tidy_open_interest method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()
        data_req = DataRequest(
            source="ccxt",
            exch="okx",
            tickers=["btc"],
            fields=["open", "high", "low", "close", "volume", "oi"],
            freq="1h",
            mkt_type="perpetual_future",
            start_date=pd.Timestamp.utcnow() - pd.Timedelta(days=1),
        )

        df = await self.ccxt_instance.fetch_tidy_open_interest_async(data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), (
            "Index is not DatetimeIndex."
        )
        print('\n')
        print(df.head())
        assert list(df.columns) == ["oi"], "Fields are missing from dataframe."
        assert (df.dtypes == "Float64").all(), "Data types are not float64."

    @pytest.mark.asyncio
    async def test_get_data(self, exch="okx"):
        """
        Test get data method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()

        df = await self.ccxt_instance.get_data_async(self.data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), (
            "Index is not DatetimeIndex."
        )
        assert list(df.index.get_level_values(1).unique()) == ["BTC/USDT:USDT"], (
            "Tickers are missing from dataframe."
        )
        assert list(df.columns) == [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "funding_rate",
            "oi",
        ], "Fields are missing from dataframe."
        assert (df.dtypes == "Float64").all(), "Data types are not float64."


if __name__ == "__main__":
    pytest.main()
