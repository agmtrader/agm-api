from enum import Enum

class Tickers(Enum):
    AAPL = 0
    NVDA = 1
    TSLA = 2
    SPY = 3

class MarketDataType(Enum):
    LIVE = 1
    DELAYED = 3