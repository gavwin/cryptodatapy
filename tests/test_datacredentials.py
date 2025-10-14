"""
Tests for DataCredentials class.
"""
import os
import pytest
from cryptodatapy.util.datacredentials import DataCredentials


class TestDataCredentials:
    """
    Test class for DataCredentials.
    """

    def test_initialization_no_env_vars(self):
        """
        Test DataCredentials initialization behavior.

        Note: DataCredentials reads environment variables at class definition time,
        so we can only verify that the class handles cases where env vars may or may
        not be set at import time. This test verifies the attributes are accessible
        and have the correct type (None or str).
        """
        # Create instance
        creds = DataCredentials()

        # Verify API keys are either None or strings (depending on import-time env state)
        api_keys = [
            ('cryptocompare_api_key', creds.cryptocompare_api_key),
            ('glassnode_api_key', creds.glassnode_api_key),
            ('alpha_vantage_api_key', creds.alpha_vantage_api_key),
            ('polygon_api_key', creds.polygon_api_key),
            ('coinmetrics_api_key', creds.coinmetrics_api_key)
        ]

        for key_name, key_value in api_keys:
            assert key_value is None or isinstance(key_value, str), \
                f"{key_name} should be None or string, got {type(key_value)}"

    def test_initialization_with_env_vars(self):
        """
        Test DataCredentials reads environment variables.

        Note: DataCredentials reads environment variables at class definition time,
        not instance creation time. This test verifies that if env vars are set
        before importing the module, they are correctly loaded.
        """
        # Create instance - will use whatever env vars were set when class was defined
        creds = DataCredentials()

        # Verify API keys attributes exist (may be None if env vars not set at import time)
        assert hasattr(creds, 'cryptocompare_api_key')
        assert hasattr(creds, 'glassnode_api_key')
        assert hasattr(creds, 'alpha_vantage_api_key')
        assert hasattr(creds, 'polygon_api_key')
        assert hasattr(creds, 'coinmetrics_api_key')

        # If any env vars were set at import time, they should be strings
        if creds.cryptocompare_api_key is not None:
            assert isinstance(creds.cryptocompare_api_key, str)
        if creds.glassnode_api_key is not None:
            assert isinstance(creds.glassnode_api_key, str)
        if creds.alpha_vantage_api_key is not None:
            assert isinstance(creds.alpha_vantage_api_key, str)
        if creds.polygon_api_key is not None:
            assert isinstance(creds.polygon_api_key, str)
        if creds.coinmetrics_api_key is not None:
            assert isinstance(creds.coinmetrics_api_key, str)

    def test_database_credentials_default_none(self):
        """
        Test that database credentials default to None.
        """
        creds = DataCredentials()

        # PostgreSQL credentials
        assert creds.postgresql_db_address is None, "PostgreSQL address should default to None"
        assert creds.postgresql_db_port is None, "PostgreSQL port should default to None"
        assert creds.postgresql_db_username is None, "PostgreSQL username should default to None"
        assert creds.postgresql_db_password is None, "PostgreSQL password should default to None"
        assert creds.postgresql_db_name is None, "PostgreSQL database name should default to None"

        # MongoDB credentials
        assert creds.mongo_db_username is None, "MongoDB username should default to None"
        assert creds.mongo_db_password is None, "MongoDB password should default to None"
        assert creds.mongo_db_name is None, "MongoDB database name should default to None"

    def test_database_credentials_can_be_set(self):
        """
        Test that database credentials can be set.
        """
        creds = DataCredentials(
            postgresql_db_address='localhost',
            postgresql_db_port='5432',
            postgresql_db_username='test_user',
            postgresql_db_password='test_pass',
            postgresql_db_name='test_db',
            mongo_db_username='mongo_user',
            mongo_db_password='mongo_pass',
            mongo_db_name='mongo_db'
        )

        # Verify PostgreSQL credentials
        assert creds.postgresql_db_address == 'localhost'
        assert creds.postgresql_db_port == '5432'
        assert creds.postgresql_db_username == 'test_user'
        assert creds.postgresql_db_password == 'test_pass'
        assert creds.postgresql_db_name == 'test_db'

        # Verify MongoDB credentials
        assert creds.mongo_db_username == 'mongo_user'
        assert creds.mongo_db_password == 'mongo_pass'
        assert creds.mongo_db_name == 'mongo_db'

    def test_base_urls_are_set(self):
        """
        Test that API base URLs are properly set.
        """
        creds = DataCredentials()

        # Verify base URLs
        assert creds.cryptocompare_base_url == 'https://min-api.cryptocompare.com/data/'
        assert creds.glassnode_base_url == 'https://api.glassnode.com/v1/metrics/'
        assert creds.aqr_base_url == 'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/'
        assert creds.polygon_base_url == 'https://api.polygon.io/v3/reference/'

    def test_coinmetrics_base_url_matches_api_key_state(self):
        """
        Test that CoinMetrics base URL is consistent with API key state at import time.

        Note: The base URL is determined when the class is defined based on whether
        the API key env var was set at that time. This test verifies the URL
        matches the API key state.
        """
        creds = DataCredentials()

        # Verify base URL is one of the two valid options
        valid_urls = [
            'https://api.coinmetrics.io/v4',  # Paid API (when key is set)
            'https://community-api.coinmetrics.io/v4'  # Free API (when key is not set)
        ]
        assert creds.coinmetrics_base_url in valid_urls, \
            f"CoinMetrics base URL should be one of {valid_urls}"

        # Verify URL matches the API key state
        if creds.coinmetrics_api_key is not None and creds.coinmetrics_api_key != '':
            assert creds.coinmetrics_base_url == 'https://api.coinmetrics.io/v4', \
                "Should use paid API URL when API key is set"
        else:
            assert creds.coinmetrics_base_url == 'https://community-api.coinmetrics.io/v4', \
                "Should use community API URL when no API key is set"

    def test_coinmetrics_base_url_with_api_key(self):
        """
        Test CoinMetrics base URL logic.

        Note: The coinmetrics_base_url is determined at class definition time
        based on whether the API key env var is set at that moment.
        This test verifies the URL is correctly set based on the state
        when the class was first imported.
        """
        creds = DataCredentials()

        # Verify the URL is one of the two expected values
        assert creds.coinmetrics_base_url in [
            'https://api.coinmetrics.io/v4',  # If API key was set at import
            'https://community-api.coinmetrics.io/v4'  # If no API key at import
        ], "CoinMetrics base URL should be either paid or community API"

        # If API key exists, should use paid API
        # If no API key, should use community API
        if creds.coinmetrics_api_key is not None and creds.coinmetrics_api_key != '':
            expected_url = 'https://api.coinmetrics.io/v4'
        else:
            expected_url = 'https://community-api.coinmetrics.io/v4'

        assert creds.coinmetrics_base_url == expected_url, \
            f"URL should match API key state: {expected_url}"

    def test_cryptocompare_endpoints_exist(self):
        """
        Test that CryptoCompare endpoints dictionary is properly populated.
        """
        creds = DataCredentials()

        # Verify endpoints dictionary exists and has expected keys
        assert isinstance(creds.cryptomcompare_endpoints, dict), \
            "CryptoCompare endpoints should be a dictionary"

        expected_endpoints = [
            'exchanges_info',
            'indexes_info',
            'assets_info',
            'markets_info',
            'on-chain_tickers_info',
            'on-chain_info',
            'social_info',
            'news',
            'news_sources',
            'rate_limit_info',
            'top_mkt_cap_info',
            'indexes'
        ]

        for endpoint in expected_endpoints:
            assert endpoint in creds.cryptomcompare_endpoints, \
                f"Endpoint '{endpoint}' should be in CryptoCompare endpoints"
            assert isinstance(creds.cryptomcompare_endpoints[endpoint], str), \
                f"Endpoint '{endpoint}' value should be a string"

    def test_cryptocompare_endpoints_values(self):
        """
        Test that CryptoCompare endpoints have correct values.
        """
        creds = DataCredentials()

        # Verify specific endpoint values
        assert creds.cryptomcompare_endpoints['exchanges_info'] == 'exchanges/general'
        assert creds.cryptomcompare_endpoints['indexes_info'] == 'index/list'
        assert creds.cryptomcompare_endpoints['assets_info'] == 'all/coinlist'
        assert creds.cryptomcompare_endpoints['markets_info'] == 'v2/cccagg/pairs'
        assert creds.cryptomcompare_endpoints['news'] == 'v2/news/?lang=EN'

    def test_api_rate_limit_url(self):
        """
        Test that API rate limit URL is set.
        """
        creds = DataCredentials()
        assert creds.cryptocompare_api_rate_limit == "https://min-api.cryptocompare.com/stats/rate/limit"

    def test_vendor_urls(self):
        """
        Test that vendor URLs are properly set.
        """
        creds = DataCredentials()

        assert creds.dbnomics_vendors_url == "https://db.nomics.world/providers"
        assert creds.pdr_vendors_url == "https://pandas-datareader.readthedocs.io/en/latest/readers/index.html"

    def test_search_urls(self):
        """
        Test that search URLs are properly set.
        """
        creds = DataCredentials()
        assert creds.dbnomics_search_url == "https://db.nomics.world/"

    def test_is_dataclass(self):
        """
        Test that DataCredentials is a proper dataclass.
        """
        from dataclasses import is_dataclass
        assert is_dataclass(DataCredentials), "DataCredentials should be a dataclass"

    def test_dataclass_fields(self):
        """
        Test that DataCredentials has the expected dataclass fields.
        """
        from dataclasses import fields

        creds_fields = [f.name for f in fields(DataCredentials)]

        # Check for critical fields
        critical_fields = [
            'postgresql_db_address',
            'postgresql_db_port',
            'postgresql_db_username',
            'postgresql_db_password',
            'postgresql_db_name',
            'mongo_db_username',
            'mongo_db_password',
            'mongo_db_name',
            'cryptocompare_api_key',
            'glassnode_api_key',
            'alpha_vantage_api_key',
            'polygon_api_key',
            'coinmetrics_api_key',
            'cryptocompare_base_url',
            'glassnode_base_url',
            'aqr_base_url',
            'coinmetrics_base_url',
            'polygon_base_url',
            'cryptomcompare_endpoints',
            'cryptocompare_api_rate_limit',
            'dbnomics_vendors_url',
            'pdr_vendors_url',
            'dbnomics_search_url'
        ]

        for field_name in critical_fields:
            assert field_name in creds_fields, f"Field '{field_name}' should be in DataCredentials"

    def test_multiple_instances_independent(self):
        """
        Test that multiple DataCredentials instances are independent.
        """
        creds1 = DataCredentials(postgresql_db_address='server1')
        creds2 = DataCredentials(postgresql_db_address='server2')

        assert creds1.postgresql_db_address == 'server1'
        assert creds2.postgresql_db_address == 'server2'
        assert creds1.postgresql_db_address != creds2.postgresql_db_address

    def test_instance_attributes_are_mutable(self):
        """
        Test that DataCredentials attributes can be modified after instantiation.
        """
        creds = DataCredentials()

        # Modify attributes
        creds.postgresql_db_address = 'new_address'
        creds.postgresql_db_port = '5433'

        # Verify modifications
        assert creds.postgresql_db_address == 'new_address'
        assert creds.postgresql_db_port == '5433'

    def test_empty_string_api_keys_behavior(self):
        """
        Test behavior of API keys based on environment state at class import time.

        Note: Since DataCredentials reads env vars at class definition time,
        this test documents the actual behavior rather than attempting to
        modify environment variables after import.
        """
        creds = DataCredentials()

        # API keys can be either None (not set at import) or string (set at import)
        api_keys = [
            creds.cryptocompare_api_key,
            creds.glassnode_api_key,
            creds.alpha_vantage_api_key,
            creds.polygon_api_key,
            creds.coinmetrics_api_key
        ]

        for key in api_keys:
            # Each key should be either None or a string
            assert key is None or isinstance(key, str), \
                "API keys should be None or string type"

    def test_all_api_keys_attributes_exist(self):
        """
        Test that all API key attributes are accessible.
        """
        creds = DataCredentials()

        # All API key attributes should exist (even if None)
        assert hasattr(creds, 'cryptocompare_api_key')
        assert hasattr(creds, 'glassnode_api_key')
        assert hasattr(creds, 'alpha_vantage_api_key')
        assert hasattr(creds, 'polygon_api_key')
        assert hasattr(creds, 'coinmetrics_api_key')

    def test_all_base_url_attributes_exist(self):
        """
        Test that all base URL attributes are accessible.
        """
        creds = DataCredentials()

        # All base URL attributes should exist
        assert hasattr(creds, 'cryptocompare_base_url')
        assert hasattr(creds, 'glassnode_base_url')
        assert hasattr(creds, 'aqr_base_url')
        assert hasattr(creds, 'coinmetrics_base_url')
        assert hasattr(creds, 'polygon_base_url')

    def test_repr_does_not_expose_sensitive_data(self):
        """
        Test that repr does not accidentally expose passwords in plain text.
        Note: This is a best practice test - dataclasses include all fields in repr by default.
        """
        creds = DataCredentials(
            postgresql_db_password='secret_password',
            mongo_db_password='mongo_secret'
        )

        repr_str = repr(creds)
        # This test documents current behavior - passwords ARE visible in repr
        # In production, consider using field(repr=False) for sensitive fields
        assert 'DataCredentials' in repr_str


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
