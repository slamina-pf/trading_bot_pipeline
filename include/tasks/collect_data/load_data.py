import os
from include.connections import PostgresConnection
from include.constants import TEMP_DATA_GENERAL_PATH
import pandas as pd
import pandas.io.sql as psql
from sqlalchemy import text
from sqlalchemy.orm import Session
from include.models import Symbol, Timeframe, MarketOCHLV
from datetime import datetime

class LoadData:
    def __init__(self, data_storage_name: str = "btc_usdt_5m"):
        self.data_storage_name = data_storage_name
        self.engine = PostgresConnection().get_engine()

    def save(self):
        print("Loading data into PostgreSQL")
        temp_path = os.path.join(TEMP_DATA_GENERAL_PATH, f"{self.data_storage_name}.parquet")
        df = pd.read_parquet(temp_path)
        print(df.head())
        unix_ts = 1733260000
        dt = datetime.fromtimestamp(unix_ts)
        # Get the underlying DBAPI connection
        with Session(self.engine) as session:
            candle = MarketOCHLV(
                timestamp=dt,
                open=0,
                close=0,
                high=0,
                low=0,
                volume=0,
                symbol_id=1,
                timeframe_id=1
            )

            session.add(candle)
            session.commit()
        print("Loaded into PostgreSQL successfully!")
