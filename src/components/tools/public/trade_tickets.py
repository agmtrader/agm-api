import pandas as pd
import numpy as np
import xlwt
from io import BytesIO
from src.utils.logger import logger
from src.utils.exception import handle_exception
import re
from datetime import datetime
from src.utils.connectors.supabase import db
from src.utils.connectors.flex_query_api import getFlexQuery
from src.utils.connectors.gmail import GmailConnector

logger.announcement('Initializing Trade Tickets Service', type='info')
agmToken = "t=419584539155539272816800"
logger.announcement('Initialized Trade Tickets Service', type='success')

@handle_exception
def send_trade_ticket_email(content, client_email):
    gmail = GmailConnector()
    return gmail.send_email(content, client_email, 'Confirmación de Transacción', 'trade_ticket')

@handle_exception
def list_trade_tickets(query: dict):
    logger.info(f"Listing trade tickets")
    trade_tickets = db.read('trade_ticket', query=query)
    return trade_tickets

@handle_exception
def read(query_id):
    trades = getFlexQuery(query_id)
    return trades


IMPROSA_HEADERS = {
    1: 'NUMERO DE CONTRATO',
    2: 'N/A',
    3: 'FECHA OPERACIÓN ',
    5: 'FECHA DE LIQUIDACION ',
    6: 'EMISION',
    7: 'SERIE',
    8: 'FACIAL',
    10: 'PRECIO BRUTO (PRECIO)',
    11: 'REND BRUTO (YIELD/ N.A. EN AGM)',
    12: 'INTERESES ACUMULADOS(ACCRUED / N.A. EN AGM)',
    13: 'VALOR TRANSADO\n(NET - TOTAL / PRODUCTO) ',
    14: 'N/A',
    15: 'N/A',
    16: 'N/A',
    17: 'N/A',
    18: 'MONEDA',
    19: 'DIAS AL VENCIMIENTO',
    20: 'FECHA DE VENCIMIENTO',
    21: 'N/A',
    22: 'N/A',
    23: 'N/A',
    24: 'BOLSA',
    25: 'MERCADO',
    26: 'TIPO DE TRANSACION',
    27: 'COMISION DE BOLSA EN CASO DE QUE APLIQUE',
}


def _first_value(row, *keys):
    for key in keys:
        value = row.get(key)
        if value not in (None, ''):
            return value
    return ''


def _number(value):
    if value in (None, ''):
        return ''
    try:
        return float(value)
    except (TypeError, ValueError):
        return ''


def _date(value):
    if value in (None, ''):
        return None
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _improsa_row(row):
    operation_date = _date(_first_value(row, 'TradeDate', 'Date/Time', 'OrigTradeDate'))
    maturity_date = _date(_first_value(row, 'Maturity', 'Expiry', 'MaturityDate'))
    if maturity_date is None:
        details = extract_bond_details(str(row.get('Description', '')))
        maturity_date = _date(details.get('maturity'))
    days_to_maturity = ''
    if operation_date and maturity_date:
        days_to_maturity = (maturity_date.date() - operation_date.date()).days

    commission = 0.0
    commission_found = False
    for key in (
        'Commission',
        'BrokerClearingCommission',
        'BrokerExecutionCommission',
        'ThirdPartyClearingCommission',
        'ThirdPartyExecutionCommission',
        'ThirdPartyRegulatoryCommission',
        'OtherCommission',
    ):
        value = _number(row.get(key))
        if value != '':
            commission += abs(value)
            commission_found = True

    return {
        0: _first_value(row, 'ClientAccountID', 'AccountAlias'),
        3: operation_date,
        5: _date(_first_value(row, 'SettleDate', 'SettlementDate')),
        6: _first_value(row, 'ISIN', 'SecurityID'),
        7: _first_value(row, 'ISIN', 'SecurityID'),
        8: _number(_first_value(row, 'Quantity', 'FaceValue')),
        10: _number(_first_value(row, 'Price')),
        11: _number(_first_value(row, 'Yield', 'Yield (Price + Interest)', 'YTM')),
        12: _number(_first_value(row, 'AccruedInterest')),
        13: _number(_first_value(row, 'NetCash', 'Amount')),
        18: _first_value(row, 'CurrencyPrimary', 'Currency'),
        19: days_to_maturity,
        20: maturity_date,
        24: _first_value(row, 'Exchange', 'ListingExchange'),
        25: _first_value(row, 'Market', 'ListingExchange'),
        26: _first_value(row, 'TransactionType', 'Buy/Sell'),
        27: commission if commission_found else '',
    }


def generate_improsa_xls(flex_query_dict, indices):
    """Build the legacy Excel workbook required by the Improsa export."""
    rows = pd.DataFrame(flex_query_dict).iloc[indices].to_dict(orient='records')
    if not rows:
        raise ValueError('At least one execution must be selected.')

    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('ExportacionDatos_0')
    header_style = xlwt.easyxf('align: wrap on, vert centre;')
    na_header_style = xlwt.easyxf('align: wrap on, vert centre; pattern: pattern solid, fore_colour yellow;')
    price_header_style = xlwt.easyxf('align: wrap on, vert centre; pattern: pattern solid, fore_colour gray25;')
    text_style = xlwt.easyxf('align: wrap on, horiz centre, vert centre;')
    date_style = xlwt.easyxf(num_format_str='DD/MM/YYYY')
    number_style = xlwt.easyxf(num_format_str='#,##0.00')
    integer_style = xlwt.easyxf(num_format_str='#,##0')

    widths = {0: 1800, 1: 1800, 2: 1400, 3: 2600, 5: 3300, 6: 2200, 7: 2200, 8: 1700, 10: 1900, 11: 1900, 12: 3600, 13: 2800, 18: 1800, 19: 3000, 20: 3300, 24: 1500, 25: 1900, 26: 3000, 27: 6000}
    for column, width in widths.items():
        worksheet.col(column).width = width

    worksheet.row(0).height = 750
    for column, label in IMPROSA_HEADERS.items():
        style = na_header_style if column == 2 else price_header_style if column == 10 else header_style
        worksheet.write(0, column, label, style)

    for row_number, source_row in enumerate(rows, start=1):
        values = _improsa_row(source_row)
        required_columns = {
            0: 'contract number',
            3: 'operation date',
            5: 'settlement date',
            6: 'issue',
            8: 'face value',
            10: 'gross price',
            13: 'transaction value',
            18: 'currency',
        }
        missing = [label for column, label in required_columns.items() if values[column] in (None, '')]
        if missing:
            raise ValueError(f'Missing required Improsa fields: {", ".join(missing)}')
        worksheet.write_merge(row_number, row_number, 0, 1, values[0], text_style)
        for column, value in values.items():
            if column in (0, 1):
                continue
            style = date_style if isinstance(value, datetime) else number_style if column in (8, 10, 11, 12, 13, 27) else integer_style if column == 19 else xlwt.Style.default_style
            worksheet.write(row_number, column, value, style)
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return output

@handle_exception
def generate(flex_query_dict, indices):
    generated = generate_trade_confirmation_message(flex_query_dict=flex_query_dict, indices=indices)
    return generated

def generate_trade_confirmation_message(flex_query_dict, indices):

    logger.info('Generating trade ticket. Processing data...')

    # Create dataframe with indexed rows only
    flex_query_df = pd.DataFrame(flex_query_dict)
    df_indexed = flex_query_df.iloc[indices].copy()

    # Check if all rows in the Description column have the same value
    if df_indexed['Description'].nunique() != 1:
        logger.error('Not all rows in the Description column have the same value.')
        raise Exception('Not all rows in the Description column have the same value.')

    bond_details = extract_bond_details(df_indexed['Description'].iloc[0])

    # Use extracted bond details
    coupon = bond_details.get('coupon')
    maturity = bond_details.get('maturity')

    if (df_indexed.loc[:,'AccruedInterest'] == 0).any():
        logger.error('At least one row has AccruedInterest value of 0.')
        raise Exception('At least one row has AccruedInterest value of 0.')

    df_indexed['Coupon'] = coupon
    df_indexed['Maturity'] = maturity

    df_indexed.loc[:,'Quantity'] = df_indexed['Quantity'].astype(float).abs()
    df_indexed.loc[:,'AccruedInterest'] = df_indexed['AccruedInterest'].astype(float).abs()
    df_indexed.loc[:,'NetCash'] = df_indexed['NetCash'].astype(float).abs()
    df_indexed.loc[:,'Amount'] = df_indexed['NetCash'].astype(float).abs()

    try:
        df_indexed.loc[:,'Accrued (Days)'] = round((df_indexed['AccruedInterest'].astype(float)) / (df_indexed['Coupon'].astype(float)/100 * df_indexed['Quantity'].astype(float)) * 360).astype(float)
    except:
        df_indexed.loc[:,'Accrued (Days)'] = 0

    df_indexed.loc[:,'TotalAmount'] = round(df_indexed['AccruedInterest'] + df_indexed['NetCash'], 2).astype(float)

    df_indexed.loc[:,'Price (including Commissions)'] = round((df_indexed['NetCash']/df_indexed['Quantity']) * 100, 4).astype(float)
    
    df_indexed['Price'] = df_indexed['Price'].astype(float)
    df_consolidated = df_indexed.iloc[0:1].copy()

    if (len(df_indexed) > 1 and len(df_indexed) != 0):

        logger.info('Detected consolidated ticket. Processing data...')

        # Replace info with new info
        df_consolidated.loc[:, 'Quantity'] = df_indexed['Quantity'].sum()
        df_consolidated.loc[:, 'AccruedInterest'] = df_indexed['AccruedInterest'].sum()
        df_consolidated.loc[:, 'NetCash'] = df_indexed['NetCash'].sum()
        df_consolidated.loc[:, 'Amount'] = df_indexed['NetCash'].sum()
        df_consolidated.loc[:, 'Price'] = df_indexed['Price'].sum()/len(df_indexed)
        df_consolidated.loc[:, 'Exchange'] = ''
        df_consolidated.loc[:,'Accrued (Days)'] = round((df_consolidated['AccruedInterest'].astype(float)) / (df_consolidated['Coupon'].astype(float)/100 * df_consolidated['Quantity'].astype(float)) * 360).astype(float)
        df_consolidated.loc[:,'TotalAmount'] = round(df_consolidated['AccruedInterest'] + df_consolidated['NetCash'], 2).astype(float)
        df_consolidated.loc[:,'Price (including Commissions)'] = round((df_consolidated['NetCash']/df_consolidated['Quantity']) * 100, 4).astype(float)

    if (len(df_consolidated) != 1):
        raise Exception('Consolidated trade ticket must be one row.')

    df_consolidated = df_consolidated.replace([np.inf, -np.inf], np.nan)
    df_consolidated = df_consolidated.fillna('')
    consolidated_dict = df_consolidated.to_dict(orient='records')[0]
    consolidated_dict['type'] = 'single' if len(df_indexed) == 1 else 'consolidated'
    
    logger.info('Generating client confirmation message...')
    df_consolidated = pd.DataFrame([consolidated_dict])

    # Generate email message
    trade_confirmation_columns = [
        "ClientAccountID",
        "AccountAlias",
        "CurrencyPrimary",

        "AssetClass",
        "Symbol",
        "Description",
        "Conid",
        "SecurityID",
        "SecurityIDType",
        "CUSIP",
        "ISIN",
        "FIGI",
        "Issuer",
        "Maturity",

        "Buy/Sell",
        "SettleDate",
        "TradeDate",
        "Exchange",
        "Quantity",
        "AccruedInterest",
        "Accrued (Days)",
        "Price",
        "Price (including Commissions)",
        "Amount",
        "TotalAmount"
    ]

    # Fill dictionary with trade data
    tradeData = {}

    for key in trade_confirmation_columns:
        try:
            tradeData[key] = df_consolidated.iloc[0][key]
        except:
            raise Exception(f'Column {key} not found in dataframe.')

    # Create message from dictionary
    message = ''
    skips = ['FIGI', 'CurrencyPrimary',' Maturity']

    for key, value in tradeData.items():
        message += str(str(key) + ': ' + str(value) + '\n')
        if (key in skips):
            message += '\n'

    logger.success(f'Client confirmation message generated.')
    return {'data': message}

def extract_bond_details(description: str):
    """Extract symbol, coupon, maturity, ISIN and ratings from a bond description.

    Expected examples:
        "GM CORP 5 Oct01'28 37045VAS9 BAA2/BBB"
        "US TREASURY 3.25 12/31/2032 US91282CHF10 AA+/AA+"

    The function is designed to be resilient to minor format variations.
    """
    # -----------------------------
    # Symbol (take everything before the coupon figure)
    # -----------------------------
    # IBKR descriptions commonly contain the issue date before the coupon,
    # e.g. ``... D08/26/13 05.950% MS43``. Prefer the number explicitly
    # marked as a percentage so the month/day/year is never mistaken for the
    # coupon. The optional trailing digit handles descriptions such as
    # ``8.5%7``.
    percentage_pattern = re.compile(
        r"(?<![\d.])(?P<coupon>\d+(?:\.\d+)?|\d+\s+\d+/\d+)\s*%",
        re.IGNORECASE,
    )
    coupon_match = percentage_pattern.search(description)
    if coupon_match is None:
        # Preserve support for descriptions without a percent sign, while
        # avoiding the numeric components of slash-separated dates.
        coupon_pattern = re.compile(r"(?<![/\d])\d+(?:\s+\d+/\d+|\.\d+)?(?![/\d])")
        coupon_match = coupon_pattern.search(description)
    if coupon_match:
        symbol_part = description[:coupon_match.start()].strip()
    else:
        # Fallback: up to first date or identifier
        symbol_part = description.split()[0]

    symbol = symbol_part

    # -----------------------------
    # Coupon
    # -----------------------------
    coupon = None
    if coupon_match:
        coupon_str = coupon_match.group('coupon') if 'coupon' in coupon_match.groupdict() else coupon_match.group(0)
        if ' ' in coupon_str:  # mixed number e.g. "5 1/2"
            whole, fraction = coupon_str.split(' ')
            num, den = fraction.split('/')
            coupon = float(whole) + float(num) / float(den)
        else:
            coupon = float(coupon_str)

    # -----------------------------
    # Maturity (support formats like Oct01'28 or 12/31/2032)
    # -----------------------------
    maturity = None

    # Format 1: MMMDD'YY e.g. Oct01'28
    mat1 = re.search(r"([A-Za-z]{3})(\d{2})'?(\d{2})", description)
    if mat1:
        mon_str, day_str, yr_str = mat1.groups()
        try:
            date_obj = datetime.strptime(f"{mon_str}{day_str}{yr_str}", "%b%d%y")
            maturity = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Format 2: MM/DD/YY(YY) e.g. 12/31/2032. IBKR prefixes issue dates
    # with ``D`` (D08/26/13), so do not treat those as maturities.
    if maturity is None:
        mat2 = re.search(r"(?<![Dd])(\d{2})/(\d{2})/(\d{2,4})", description)
        if mat2:
            m, d, y = mat2.groups()
            fmt = "%y" if len(y) == 2 else "%Y"
            date_obj = datetime.strptime(f"{m}/{d}/{y}", f"%m/%d/{fmt}")
            maturity = date_obj.strftime("%Y-%m-%d")

    # -----------------------------
    # ISIN (two letters + 10 alphanumerics)
    # -----------------------------
    isin_match = re.search(r"\b[A-Z]{2}[A-Z0-9]{10}\b", description)
    isin = isin_match.group(0) if isin_match else None

    # -----------------------------
    # Ratings (capture Moody's/S&P pair e.g. BAA2/BBB, Aa1/AA-, etc.)
    # -----------------------------
    ratings_match = re.search(r"\b([A-Z]{1,4}[+-]?\d?/[A-Z]{1,4}[+-]?\d?)\b", description)
    ratings = ratings_match.group(1) if ratings_match else None
    
    return {
        'symbol': symbol,
        'coupon': coupon,
        'maturity': maturity,
        'isin': isin,
        'ratings': ratings,
    }
