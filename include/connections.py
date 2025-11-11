import ccxt
import psycopg2
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
    

class PostgresConnection:
    def __init__(self, user: str, password: str, host: str, port: int, database: str):
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