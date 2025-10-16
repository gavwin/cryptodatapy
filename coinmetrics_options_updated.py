from datetime import datetime
import polars as pl
import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.data_vendors.coinmetrics_api import CoinMetrics
from cryptodatapy.util.datacredentials import DataCredentials


class CoinMetricsOptionsData:
    """Class to fetch CoinMetrics options data using cryptodatapy and save to Parquet files."""

    def __init__(self, api_key: str = None):
        """
        Initialize the CoinMetrics client via cryptodatapy.

        Args:
            api_key: CoinMetrics API key. If None, will try to get from environment.
        """
        # Set API key if provided
        if api_key:
            import os
            os.environ['COINMETRICS_API_KEY'] = api_key

        self.client = CoinMetrics()

    def fetch_options_markets(self, output_path: str = None) -> list:
        """
        Get all Deribit BTC options markets. Saves to Parquet file if output_path is given.

        Args:
            output_path (str, optional): Path to output file. Defaults to None.

        Returns:
            list: List of market identifiers
        """
        print("Fetching options markets catalog...")

        # Get all markets and filter for Deribit BTC options
        markets_df = self.client.get_markets_info()

        # Filter for Deribit exchange, BTC base, and option type
        btc_options = markets_df[
            (markets_df['exchange'] == 'deribit') &
            (markets_df['base'] == 'btc') &
            (markets_df['type'] == 'option')
        ]

        markets = btc_options['market'].tolist()
        print(f"Retrieved {len(markets)} BTC options markets from Deribit")

        if output_path is not None:
            markets_pl = pl.DataFrame({"market": markets})
            markets_pl.write_parquet(output_path)
            print(f"Markets list saved to {output_path}")

        return markets

    def fetch_options_data(
        self,
        start_date: str = "2010-01-01",
        end_date: str = None,
        output_path: str = "coinmetrics_options_data.parquet",
        exch: str = "deribit",
        base: str = "btc",
    ) -> None:
        """
        Fetch options data and save to Parquet file using cryptodatapy.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (defaults to today)
            output_path: Path to save the Parquet file
            exch: Exchange name (default: 'deribit')
            base: Base asset (default: 'btc')
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        print(f"Starting data collection from {start_date} to {end_date}")

        # Get all relevant markets
        markets = self.fetch_options_markets()

        if not markets:
            print("No markets found! Cannot fetch data.")
            return

        print(f"Fetching data for {len(markets)} markets from CoinMetrics API...")

        # Create DataRequest for options market data
        # Note: cryptodatapy uses market identifiers directly from CoinMetrics
        data_req = DataRequest(
            source='coinmetrics',
            mkt_type='option',
            freq='d',  # daily frequency
            start_date=start_date,
            end_date=end_date,
            exch=exch,
            tickers=[base],  # base asset
        )

        # Fetch OHLCV data for options
        print("Fetching OHLCV data...")
        try:
            df_ohlcv = self.client.get_ohlcv(data_req)
            print(f"Retrieved {len(df_ohlcv)} OHLCV records")
        except Exception as e:
            print(f"Error fetching OHLCV: {e}")
            df_ohlcv = pd.DataFrame()

        # Fetch open interest data for options
        print("Fetching open interest data...")
        try:
            df_oi = self.client.get_open_interest(data_req)
            print(f"Retrieved {len(df_oi)} open interest records")
        except Exception as e:
            print(f"Error fetching open interest: {e}")
            df_oi = pd.DataFrame()

        # Combine dataframes if both exist
        if not df_ohlcv.empty and not df_oi.empty:
            df_combined = df_ohlcv.join(df_oi, how='outer')
            df_final = df_combined.reset_index()
        elif not df_ohlcv.empty:
            df_final = df_ohlcv.reset_index()
        elif not df_oi.empty:
            df_final = df_oi.reset_index()
        else:
            print("No data retrieved!")
            return

        # Convert to Polars and save to Parquet
        print(f"Converting {len(df_final)} records to Polars DataFrame...")
        df_polars = pl.from_pandas(df_final)

        print(f"Saving data to {output_path}...")
        df_polars.write_parquet(output_path)

        print(f"Completed! Saved {len(df_polars)} records to {output_path}")

        # Display basic info
        print(f"Data shape: {df_polars.shape}")
        print(f"Columns: {list(df_polars.columns)}")
        print("\nFirst few rows:")
        print(df_polars.head())


if __name__ == "__main__":
    print("Starting CoinMetrics Options Data Collection")
    print("=" * 60)

    # Initialize the data fetcher
    # API key will be read from environment variable COINMETRICS_API_KEY
    # or you can pass it directly: fetcher = CoinMetricsOptionsData(api_key='your_key')
    fetcher = CoinMetricsOptionsData()

    # Fetch data for specified date range
    fetcher.fetch_options_data(
        start_date="2025-09-10",
        end_date="2025-10-16",
        output_path="updated.parquet",
    )

    # Also save the markets list
    markets = fetcher.fetch_options_markets("btc_options_markets.parquet")

    print("=" * 60)
    print("All data collection completed!")
