import os
from dataclasses import dataclass, field


@dataclass
class DataCredentials:
    """
    Stores data credentials used by the CryptoDataPy project for data extraction, storage, etc.
    """

    # SQL db for structured data
    # postgresql db credentials
    postgresql_db_address: str = None
    postgresql_db_port: str = None
    postgresql_db_username: str = None
    postgresql_db_password: str = None
    postgresql_db_name: str = None

    # NoSQL DB for tick/unstructured data
    # Arctic/mongodb credentials
    mongo_db_username: str = None
    mongo_db_password: str = None
    mongo_db_name: str = None

    # API keys - will be populated at instance initialization
    cryptocompare_api_key: str = None
    glassnode_api_key: str = None
    tiingo_api_key: str = None
    alpha_vantage_api_key: str = None
    polygon_api_key: str = None
    coinmetrics_api_key: str = None

    # API base URLs
    cryptocompare_base_url: str = 'https://min-api.cryptocompare.com/data/'
    glassnode_base_url: str = 'https://api.glassnode.com/v1/metrics/'
    tiingo_base_url: str = 'https://api.tiingo.com/tiingo/'
    aqr_base_url: str = 'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/'
    coinmetrics_base_url: str = None  # Will be set in __post_init__ based on API key
    polygon_base_url: str = 'https://api.polygon.io/v3/reference/'

    # API endpoints
    cryptomcompare_endpoints: dict = field(default_factory=lambda: {
        'exchanges_info': 'exchanges/general',
        'indexes_info': 'index/list',
        'assets_info': 'all/coinlist',
        'markets_info': 'v2/cccagg/pairs',
        'on-chain_tickers_info': 'blockchain/list',
        'on-chain_info': 'blockchain/latest?fsym=BTC',
        'social_info': 'social/coin/histo/day',
        'news': 'v2/news/?lang=EN',
        'news_sources': 'news/feeds',
        'rate_limit_info': 'rate/limit',
        'top_mkt_cap_info': 'top/mktcapfull?',
        'indexes': 'index/'
    })

    # api limit URLs
    cryptocompare_api_rate_limit: str = "https://min-api.cryptocompare.com/stats/rate/limit"

    # vendors URLs
    dbnomics_vendors_url: str = "https://db.nomics.world/providers"
    pdr_vendors_url: str = "https://pandas-datareader.readthedocs.io/en/latest/readers/index.html"

    # search URLs
    dbnomics_search_url: str = "https://db.nomics.world/"

    def __post_init__(self):
        """
        Read API keys from environment variables at instance initialization.
        This ensures keys are loaded when the instance is created, not when the class is defined.
        """
        # Read API keys from environment variables if not already set
        if self.cryptocompare_api_key is None:
            self.cryptocompare_api_key = os.environ.get('CRYPTOCOMPARE_API_KEY')

        if self.glassnode_api_key is None:
            self.glassnode_api_key = os.environ.get('GLASSNODE_API_KEY')

        if self.tiingo_api_key is None:
            self.tiingo_api_key = os.environ.get('TIINGO_API_KEY')

        if self.alpha_vantage_api_key is None:
            self.alpha_vantage_api_key = os.environ.get('ALPHAVANTAGE_API_KEY')

        if self.polygon_api_key is None:
            self.polygon_api_key = os.environ.get('POLYGON_API_KEY')

        if self.coinmetrics_api_key is None:
            self.coinmetrics_api_key = os.environ.get('COINMETRICS_API_KEY')

        # Set CoinMetrics base URL based on whether API key is present
        if self.coinmetrics_base_url is None:
            if self.coinmetrics_api_key is not None and self.coinmetrics_api_key != '':
                self.coinmetrics_base_url = 'https://api.coinmetrics.io/v4'
            else:
                self.coinmetrics_base_url = 'https://community-api.coinmetrics.io/v4'
