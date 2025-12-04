import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from include.connections import PostgresConnection, BinanceBasicConnection
from include.constants import TEMP_DATA_GENERAL_PATH
import os
from sqlalchemy import text

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
        - connection: ccxt exchange instance returned by [`BinanceBasicConnection`](include/connections.py)
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
        self.exchange = BinanceBasicConnection().create_client()
        self.etl_connection = PostgresConnection().get_engine()


    def to_milliseconds(self, dt):
        """
            Convert a timezone-aware datetime to epoch milliseconds (int).
            Used to pass since/end timestamps to ccxt fetch_ohlcv.
        """
        return int(dt.timestamp() * 1000)
    

    """ def get_start_date(self):
        con = self.etl_connection.get_connection_string()
        cur = con.cursor()
        cur.execute("SELECT name, duration FROM timeframe ORDER BY name;")
        rows = cur.fetchall() """

    def add_symbol_and_timeframe(self, df: pd.DataFrame) -> pd.DataFrame:
        engine = self.etl_connection   

        with engine.connect() as conn:
            symbol_id = conn.execute(
                text("SELECT id FROM symbol WHERE symbol = :symbol"),
                {"symbol": self.symbol}
            ).scalar()

            timeframe_id = conn.execute(
                text("SELECT id FROM timeframe WHERE name = :timeframe"),
                {"timeframe": self.timeframe}
            ).scalar()
        print("Symbol ID: ", symbol_id)
        print("Timeframe ID: ", timeframe_id)
        df["symbol_id"] = symbol_id
        df["timeframe_id"] = timeframe_id
        return df

    def load_data(self, df):
        os.makedirs(TEMP_DATA_GENERAL_PATH, exist_ok=True)
        temp_path = os.path.join(TEMP_DATA_GENERAL_PATH, f"{self.data_storage_name}.parquet")

        os.makedirs(TEMP_DATA_GENERAL_PATH, exist_ok=True)

        df.to_parquet(temp_path, index=False)
        print(f"Data saved to {temp_path}")

    def get_data(self, start_date: datetime = None) -> list:
        """
            Retrieve OHLCV data in a loop, paging through results until the end timestamp.

            If no start_date is provided, start from the earliest available candle.

            Returns:
                list: concatenated raw candle lists from ccxt (each candle is [ts, open, high, low, close, volume]).
        """
        
        since = self.exchange.parse8601('2017-01-01T00:00:00Z')
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
        print("Total candles fetched: ", len(all_candles))
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
        print("Starting data extraction fuck you")
        data = self.get_data()
        print("final data: ", len(data))
        df = pd.DataFrame(data,columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'] )
        df = self.add_symbol_and_timeframe(df)
        self.load_data(df)
