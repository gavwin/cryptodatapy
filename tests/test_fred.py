import numpy as np
import pandas as pd
import pytest

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.pandasdr_api import PandasDataReader


@pytest.fixture
def fred():
    """Create PandasDataReader instance for FRED tests."""
    return PandasDataReader()


@pytest.fixture
def data_req():
    """Create basic DataRequest instance."""
    return DataRequest()


@pytest.fixture
def fred_data_resp():
    """Load FRED test data response."""
    return pd.read_csv('data/fred_df.csv', index_col=0, parse_dates=True)


def test_wrangle_data_resp(fred, data_req, fred_data_resp) -> None:
    """
    Test wrangling of FRED data response into tidy data format.
    """
    # Setup data request for macro category
    data_req.source = 'fred'
    data_req.cat = 'macro'
    data_req.tickers = ['US_MB', 'US_UE_Rate']
    data_req.fields = ['actual']
    data_req.freq = 'm'

    # Wrangle data response
    df = fred.wrangle_data_resp(data_req, fred_data_resp)

    # Assertions
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
    assert df.index.names == ['date', 'ticker'], "Index names incorrect."
    assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), "Date index is not DatetimeIndex."
    assert list(df.columns) == ['actual'], "Column name should be 'actual' for macro data."
    assert set(df.index.get_level_values(1).unique()) == {'US_MB', 'US_UE_Rate'}, "Tickers missing from dataframe."
    assert df['actual'].dtype == 'Float64', "Actual values should be Float64 type."
    # Check no entire rows of NaNs
    assert not df.isna().all(axis=1).any(), "Found rows with all NaN values."


def test_wrangle_data_resp_rates(fred, data_req, fred_data_resp) -> None:
    """
    Test wrangling of FRED data response for rates category (should use 'close' field).
    """
    # Setup data request for rates category
    data_req.source = 'fred'
    data_req.cat = 'rates'
    data_req.tickers = ['US_MB', 'US_UE_Rate']
    data_req.fields = ['close']
    data_req.freq = 'm'

    # Wrangle data response
    df = fred.wrangle_data_resp(data_req, fred_data_resp)

    # Assertions
    assert not df.empty, "Dataframe was returned empty."
    assert list(df.columns) == ['close'], "Column name should be 'close' for non-macro data."


def test_convert_params(fred, data_req) -> None:
    """
    Test parameter conversion for FRED.
    """
    # Setup data request
    data_req.source = 'fred'
    data_req.cat = 'macro'
    data_req.tickers = ['US_MB', 'US_UE_Rate']
    data_req.fields = ['actual']
    data_req.freq = 'd'
    data_req.start_date = pd.Timestamp('2020-01-01')
    data_req.end_date = pd.Timestamp('2022-12-31')

    # Convert params
    converted_req = fred.convert_params(data_req)

    # Assertions
    assert converted_req.source_tickers is not None, "Source tickers should be set."
    assert len(converted_req.source_tickers) == 2, "Should have 2 source tickers."
    assert converted_req.source_freq == 'd', "Frequency should be preserved."
    assert converted_req.source_start_date == pd.Timestamp('2020-01-01'), "Start date should match."
    assert converted_req.source_end_date == pd.Timestamp('2022-12-31'), "End date should match."
    assert converted_req.tz == "America/New_York", "Timezone should be America/New_York."


def test_convert_params_defaults(fred, data_req) -> None:
    """
    Test parameter conversion with default values.
    """
    # Setup minimal data request
    data_req.source = 'fred'
    data_req.cat = 'macro'
    data_req.tickers = ['US_MB']
    data_req.fields = ['actual']

    # Convert params
    converted_req = fred.convert_params(data_req)

    # Assertions
    assert converted_req.source_start_date == pd.Timestamp('1920-01-01'), "Default start date should be 1920-01-01."
    assert converted_req.source_end_date is not None, "End date should be set to current time."
    assert isinstance(converted_req.source_end_date, pd.Timestamp), "End date should be Timestamp."


def test_check_params_invalid_category(fred) -> None:
    """
    Test parameter validation with invalid category.
    """
    data_req = DataRequest(source='fred', cat='crypto', tickers=['US_MB'])
    with pytest.raises(ValueError, match="Select a valid category"):
        fred.convert_params(data_req)


def test_check_params_no_tickers(fred) -> None:
    """
    Test parameter validation with no tickers.
    """
    data_req = DataRequest(source='fred', cat='macro', tickers=[])
    data_req.source_tickers = []
    with pytest.raises(ValueError, match="No tickers provided"):
        fred.convert_params(data_req)


def test_check_params_invalid_freq(fred) -> None:
    """
    Test parameter validation with invalid frequency.
    """
    data_req = DataRequest(source='fred', cat='macro', tickers=['US_MB'], freq='tick')
    data_req.source_freq = 'tick'
    with pytest.raises(ValueError, match="frequency is not available"):
        fred.convert_params(data_req)


def test_check_params_invalid_mkt_type(fred) -> None:
    """
    Test parameter validation with invalid market type.
    """
    data_req = DataRequest(source='fred', cat='macro', tickers=['US_MB'], mkt_type='perpetual_future')
    with pytest.raises(ValueError, match="is not available"):
        fred.convert_params(data_req)


def test_check_params_invalid_fields(fred) -> None:
    """
    Test parameter validation with invalid fields for category.
    """
    data_req = DataRequest(source='fred', cat='macro', tickers=['US_MB'], fields=['open', 'high', 'low'])
    with pytest.raises(ValueError, match="fields are not available"):
        fred.convert_params(data_req)


def test_integration_get_data(fred) -> None:
    """
    Test integration of get_data method with real FRED API call.
    Note: This test makes actual API calls to FRED.
    """
    data_req = DataRequest(
        source='fred',
        tickers=['US_MB', 'US_UE_Rate'],
        fields='actual',
        cat='macro',
        freq='m',
        start_date='2020-01-01',
        end_date='2022-12-31'
    )

    df = fred.get_data(data_req)

    # Assertions
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert df.index.names == ['date', 'ticker'], "Index names should be ['date', 'ticker']."
    assert isinstance(
        df.index.get_level_values(0), pd.DatetimeIndex
    ), "Date index is not DatetimeIndex."
    assert set(df.index.get_level_values(1).unique()) == {
        'US_MB', 'US_UE_Rate'
    }, "Tickers are missing from dataframe."
    assert list(df.columns) == ['actual'], "Fields are missing from dataframe."
    # Check date range
    assert df.index.get_level_values(0).min() >= pd.Timestamp('2020-01-01'), "Start date incorrect."
    assert df.index.get_level_values(0).max() <= pd.Timestamp('2023-01-01'), "End date incorrect."
    # Check data types
    assert df['actual'].dtype == 'Float64', "Actual values should be Float64 type."


def test_integration_get_data_rates(fred) -> None:
    """
    Test integration of get_data method for rates category.
    Note: This test makes actual API calls to FRED.
    """
    data_req = DataRequest(
        source='fred',
        tickers=['US_Rates_10Y'],
        fields='close',
        cat='rates',
        freq='m',
        start_date='2020-01-01',
        end_date='2022-12-31'
    )

    df = fred.get_data(data_req)

    # Assertions
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['close'], "Column should be 'close' for rates data."
    assert 'US_Rates_10Y' in df.index.get_level_values(1).unique(), "Ticker missing from dataframe."


if __name__ == "__main__":
    pytest.main()
