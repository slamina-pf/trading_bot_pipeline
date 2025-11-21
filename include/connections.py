import ccxt
import psycopg2
from include.constants import POSTGRES_ETL_USER, POSTGRES_ETL_PASSWORD, POSTGRES_ETL_DB, POSTGRES_ETL_DB_HOST, POSTGRES_ETL_DB_PORT


print("Postgres ETL User: ", POSTGRES_ETL_USER)
print("Postgres ETL Password: ", POSTGRES_ETL_PASSWORD)
print("Postgres ETL DB Host: ", POSTGRES_ETL_DB_HOST)
print("Postgres ETL DB Port: ", POSTGRES_ETL_DB_PORT)
print("Postgres ETL DB: ", POSTGRES_ETL_DB)
class BinanceConnection:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    def create_client(self):
        EXCHANGE = ccxt.binance({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'},
        })
        EXCHANGE.set_sandbox_mode(True)
        EXCHANGE.enableRateLimit = True

        return EXCHANGE
    
class BinanceBasicConnection:
    
    def create_client(self):
        
        BASIC_EXCHANGE = ccxt.binance()

        return BASIC_EXCHANGE
class PostgresConnection:
    def __init__(
            self, 
            user: str = POSTGRES_ETL_USER , 
            password: str = POSTGRES_ETL_PASSWORD, 
            host: str = POSTGRES_ETL_DB_HOST, 
            port: int = int(POSTGRES_ETL_DB_PORT), 
            database: str = POSTGRES_ETL_DB
        ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def get_connection_string(self) -> str:
        con = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password
        )
        return con