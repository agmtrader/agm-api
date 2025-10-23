from enum import IntEnum


class MarketDataField(IntEnum):
    """Enumeration of market data snapshot field identifiers from IBKR API."""

    # Price and Trading Information
    LAST_PRICE = 31  # Last traded price
    SYMBOL = 55  # Ticker symbol
    TEXT = 58  # Free-form text field
    HIGH = 70  # Current day high price
    LOW = 71  # Current day low price
    MARKET_VALUE = 73  # Current market value of the position
    AVG_PRICE = 74  # Average price of the position
    UNREALIZED_PNL = 75  # Unrealized profit or loss
    FORMATTED_POSITION = 76
    FORMATTED_UNREALIZED_PNL = 77
    DAILY_PNL = 78  # Profit or loss for the current day
    REALIZED_PNL = 79  # Realized profit or loss
    UNREALIZED_PNL_PERCENT = 80  # Unrealized PnL as a percentage
    CHANGE = 82  # Change from previous close
    CHANGE_PERCENT = 83  # Change percentage from previous close
    BID_PRICE = 84  # Highest bid price
    ASK_SIZE = 85  # Ask size (shares/contracts)
    ASK_PRICE = 86  # Lowest ask price
    VOLUME = 87  # Trading volume for the day
    BID_SIZE = 88  # Bid size (shares/contracts)

    # Contract & Exchange Information
    EXCHANGE = 6004
    CONID = 6008  # IBKR contract identifier
    SECTYPE = 6070  # Security type / asset class
    MONTHS = 6072
    REGULAR_EXPIRY = 6073
    MARKET_DATA_DELIVERY_METHOD = 6119
    UNDERLYING_CONID = 6457
    SERVICE_PARAMS = 6508
    MARKET_DATA_AVAILABILITY = 6509  # Real-time / delayed / frozen availability flags

    # Company & Quote Venue Details
    COMPANY_NAME = 7051
    ASK_EXCH = 7057
    LAST_EXCH = 7058
    LAST_SIZE = 7059
    BID_EXCH = 7068

    # Volatility & Option Metrics
    IMPLIED_VOL_HIST_VOL_PERCENT = 7084
    PUT_CALL_INTEREST = 7085
    PUT_CALL_VOLUME = 7086
    HIST_VOL_PERCENT = 7087
    HIST_VOL_CLOSE_PERCENT = 7088
    OPTION_VOLUME = 7089
    CONID_EXCHANGE = 7094

    # Tradability & Descriptions
    CAN_BE_TRADED = 7184
    CONTRACT_DESCRIPTION_1 = 7219
    CONTRACT_DESCRIPTION_2 = 7220
    LISTING_EXCHANGE = 7221

    # Fundamentals & Stats
    INDUSTRY = 7280
    CATEGORY = 7281
    AVERAGE_VOLUME = 7282
    OPTION_IMPLIED_VOL_PERCENT = 7283
    HISTORICAL_VOL_PERCENT_DEPRECATED = 7284
    PUT_CALL_RATIO = 7285
    DIVIDEND_AMOUNT = 7286
    DIVIDEND_YIELD_PERCENT = 7287
    DIVIDEND_EX_DATE = 7288
    MARKET_CAP = 7289
    PE_RATIO = 7290
    EPS = 7291
    COST_BASIS = 7292
    FIFTY_TWO_WEEK_HIGH = 7293
    FIFTY_TWO_WEEK_LOW = 7294
    OPEN = 7295
    CLOSE = 7296

    # Greeks
    DELTA = 7308
    GAMMA = 7309
    THETA = 7310
    VEGA = 7311

    # Option Activity & Ratios
    OPTION_VOLUME_CHANGE_PERCENT = 7607
    IMPLIED_VOL_PERCENT_STRIKE = 7633  # Implied vol for a specific strike
    MARK = 7635
    SHORTABLE_SHARES = 7636
    FEE_RATE = 7637
    OPTION_OPEN_INTEREST = 7638
    PERCENT_OF_MARK_VALUE = 7639
    SHORTABLE_LEVEL = 7644
    MORNINGSTAR_RATING = 7655

    # Dividends & Moving Averages
    DIVIDENDS = 7671
    DIVIDENDS_TTM = 7672
    EMA_200 = 7674
    EMA_100 = 7675
    EMA_50 = 7676
    EMA_20 = 7677
    PRICE_OVER_EMA_200 = 7678
    PRICE_OVER_EMA_100 = 7679
    PRICE_OVER_EMA_50 = 7724
    PRICE_OVER_EMA_20 = 7681

    # Intraday & Event Information
    CHANGE_SINCE_OPEN = 7682
    UPCOMING_EVENT = 7683
    UPCOMING_EVENT_DATE = 7684
    UPCOMING_ANALYST_MEETING = 7685
    UPCOMING_EARNINGS = 7686
    UPCOMING_MISC_EVENT = 7687
    RECENT_ANALYST_MEETING = 7688
    RECENT_EARNINGS = 7689
    RECENT_MISC_EVENT = 7690

    # Probability Metrics
    PROBABILITY_OF_MAX_RETURN = 7694
    BREAK_EVEN = 7695
    SPX_DELTA = 7696
    FUTURES_OPEN_INTEREST = 7697
    LAST_YIELD = 7698
    BID_YIELD = 7699
    PROBABILITY_OF_MAX_RETURN_ALT = 7700  # Duplicate identifier with alt name
    PROBABILITY_OF_MAX_LOSS = 7702
    PROFIT_PROBABILITY = 7703

    # Bond & Organization Details
    ORGANIZATION_TYPE = 7704
    DEBT_CLASS = 7705
    RATINGS = 7706
    BOND_STATE_CODE = 7707
    BOND_TYPE = 7708

    # Dates & Beta
    LAST_TRADING_DATE = 7714
    ISSUE_DATE = 7715
    BETA = 7718

    ASK_YIELD = 7720

    # Historical Prices
    PRIOR_CLOSE = 7741

    # Volume Precision
    VOLUME_LONG = 7762

    # Permissions
    HAS_TRADING_PERMISSIONS = 7768

    # Raw Values
    DAILY_PNL_RAW = 7920
    COST_BASIS_RAW = 7921

    # (No convenience aliases inside enum to avoid non-int values)

# Convenience tuple of commonly requested minimal fields
MIN_DEFAULT_FIELDS = (
    MarketDataField.SYMBOL,
    MarketDataField.LAST_PRICE,
    MarketDataField.AVG_PRICE,
    MarketDataField.BID_PRICE,
    MarketDataField.ASK_PRICE,
    MarketDataField.BID_SIZE,
    MarketDataField.ASK_SIZE,
)
