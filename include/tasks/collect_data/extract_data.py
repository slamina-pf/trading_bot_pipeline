import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from include.connections import BinanceConnection, PostgresConnection
from include.constants import BINANCE_API_KEY, BINANCE_SECRET, TEMP_DATA_GENERAL_PATH, POSTGRES_ETL_USER, POSTGRES_ETL_PASSWORD, POSTGRES_ETL_DB, POSTGRES_ETL_DB_HOST, POSTGRES_ETL_DB_PORT
import os

# Module-level: utilities to extract OHLCV data from Binance via ccxt.
# The main entrypoint in this module is the ExtractData class below
class ExtractData:
    """
        ExtractData handles downloading historical OHLCV candle data from Binance.

        Attributes
        - symbol: market symbol to request (e.g. "BTC/USDT")
        - timeframe: candle timeframe accepted by ccxt (e.g. "1h", "5m")
        - days: number of days before now to start downloading
        - limit: max candles per request (ccxt/exchange limit)
        - data_storage_name: filename (without extension) used when saving parquet
        - start_date / end_date: computed UTC datetime window for extraction
        - connection: ccxt exchange instance returned by [`BinanceConnection`](include/connections.py)
    """
    def __init__(
            self, 
            symbol: str = "BTC/USDT", 
            timeframe: str = "1h", 
            days: int = 365,
            limit: int = 1000, 
            data_storage_name: str = "btc_usdt_5m",
        ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.days = days
        self.limit = limit
        self.data_storage_name = data_storage_name
        self.end_date = datetime.now(timezone.utc)
        self.start_date = datetime.now(timezone.utc) - timedelta(days=self.days)
        self.exchange = BinanceConnection(BINANCE_API_KEY, BINANCE_SECRET).create_client()
        self.etl_connection = PostgresConnection(POSTGRES_ETL_USER, POSTGRES_ETL_PASSWORD, POSTGRES_ETL_DB_HOST, int(POSTGRES_ETL_DB_PORT), POSTGRES_ETL_DB)


    def get_start_date(self):
        con = self.etl_connection.get_connection_string()
        cur = con.cursor()
        cur.execute("SELECT name, duration FROM timeframe ORDER BY name;")
        rows = cur.fetchall()

    def save_to_parquet(self, df):
        """
            Persist a pandas DataFrame to parquet under TEMP_DATA_GENERAL_PATH.

            The directory is created if it doesn't exist.
        """
        os.makedirs(f'{TEMP_DATA_GENERAL_PATH}', exist_ok=True)
        df.to_parquet(f'{TEMP_DATA_GENERAL_PATH}/{self.data_storage_name}.parquet', index=False)


    def to_milliseconds(self, dt):
        """
            Convert a timezone-aware datetime to epoch milliseconds (int).
            Used to pass since/end timestamps to ccxt fetch_ohlcv.
        """
        return int(dt.timestamp() * 1000)
    

    def get_data(self, start_date: datetime = None) -> list:
        """
            Retrieve OHLCV data in a loop, paging through results until the end timestamp.

            If no start_date is provided, start from the earliest available candle.

            Returns:
                list: concatenated raw candle lists from ccxt (each candle is [ts, open, high, low, close, volume]).
        """
        # If start_date is None, fetch the first available candle
        if start_date is None:
            try:
                # Fetch one candle without specifying 'since' → earliest available
                first_candle = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, limit=1)
                if not first_candle:
                    raise ValueError("No OHLCV data available for this symbol/timeframe.")
                since = first_candle[0][0]  # Timestamp of the first candle
            except Exception as e:
                raise RuntimeError(f"Failed to fetch initial OHLCV data: {e}")
        else:
            since = self.to_milliseconds(start_date)

        end_timestamp = self.to_milliseconds(self.end_date)
        all_candles = []

        while since < end_timestamp:
            candles = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, since, self.limit)

            if not candles:
                break

            all_candles.extend(candles)

            # Move to next batch
            since = candles[-1][0] + 1

            # Respect rate limits
            time.sleep(self.exchange.rateLimit / 1000)
        
        return all_candles
    

    def extract(self):
        """
            High-level method that performs extraction and saves the result as parquet.

            Steps:
            1. Download raw candles via get_data()
            2. Convert to pandas.DataFrame with proper column names
            3. Convert timestamp column from ms to pandas datetime (UTC)
            4. Persist DataFrame using save_to_parquet()
        """
        data = self.get_data()

        df = pd.DataFrame(
            data,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        self.save_to_parquet(df)