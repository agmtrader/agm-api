import pandas as pd
import numpy as np
from src.utils.logger import logger
from src.utils.exception import handle_exception
import re
from datetime import datetime
from src.utils.connectors.supabase import db
from src.utils.connectors.flex_query_api import getFlexQuery

logger.announcement('Initializing Trade Tickets Service', type='info')
agmToken = "t=419584539155539272816800"
logger.announcement('Initialized Trade Tickets Service', type='success')


@handle_exception
def list_trade_tickets(query: dict):
    logger.info(f"Listing trade tickets")
    trade_tickets = db.read('trade_ticket', query=query)
    return trade_tickets

@handle_exception
def read(query_id):
    trades = getFlexQuery(query_id)
    return trades

@handle_exception
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

    # TODO fix this
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

    logger.info(f'Extracting bond details from description: {description}')

    # -----------------------------
    # Symbol (take everything before the coupon figure)
    # -----------------------------
    coupon_pattern = re.compile(r"\b\d+(?:\s+\d+/\d+|\.\d+)?\b")  # e.g. 5 or 5 1/2 or 5.25
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
        coupon_str = coupon_match.group(0)
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

    # Format 2: MM/DD/YY(YY) e.g. 12/31/2032
    if maturity is None:
        mat2 = re.search(r"(\d{2})/(\d{2})/(\d{2,4})", description)
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

    logger.success(
        f"Extracted bond details: symbol={symbol}, coupon={coupon}, maturity={maturity}, isin={isin}, ratings={ratings}"
    )

    return {
        'symbol': symbol,
        'coupon': coupon,
        'maturity': maturity,
        'isin': isin,
        'ratings': ratings,
    }
@handle_exception
def generate_excel_file(flex_query_dict, indices):
    logger.info('Generating trade ticket. Processing data...')

    # Create dataframe with indexed rows only
    flex_query_df = pd.DataFrame(flex_query_dict)
    df_indexed = flex_query_df.iloc[indices].copy()

    bond_details = extract_bond_details(df_indexed['Description'].iloc[0])

    if (df_indexed.loc[:,'AccruedInterest'] == 0).any():
        logger.error('At least one row has AccruedInterest value of 0.')
        raise Exception('At least one row has AccruedInterest value of 0.')

    df_indexed['Coupon'] = bond_details['coupon']
    df_indexed['Maturity'] = bond_details['maturity']

    df_indexed.loc[:,'Quantity'] = df_indexed['Quantity'].astype(float).abs()
    df_indexed.loc[:,'AccruedInterest'] = df_indexed['AccruedInterest'].astype(float).abs()
    df_indexed.loc[:,'NetCash'] = df_indexed['NetCash'].astype(float).abs()
    df_indexed.loc[:,'Amount'] = df_indexed['NetCash'].astype(float).abs()

    try:
        df_indexed.loc[:,'Accrued (Days)'] = round((df_indexed['AccruedInterest'].astype(float)) / (df_indexed['Coupon'].astype(float)/100 * df_indexed['Quantity'].astype(float)) * 360).astype(float)
    except:
        df_indexed.loc[:,'Accrued (Days)'] = 0

    df_indexed.loc[:,'TotalAmount'] = round(df_indexed['AccruedInterest'] + df_indexed['NetCash'], 2).astype(float)

    # TODO fix this
    df_indexed.loc[:,'Price (including Commissions)'] = round((df_indexed['NetCash']/df_indexed['Quantity']) * 100, 4).astype(float)
    
    df_indexed['Price'] = df_indexed['Price'].astype(float)
    df_indexed = df_indexed.fillna('')

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

    df_indexed = df_indexed[trade_confirmation_columns]

    # Translate the columns to Spanish
    df_indexed = df_indexed.rename(columns={
        'ClientAccountID': 'Cuenta de Cliente',
        'AccountAlias': 'Alias de Cuenta',
        'CurrencyPrimary': 'Moneda Principal',
        'AssetClass': 'Clase de Activo',
        'Symbol': 'Símbolo',
        'Description': 'Descripción',
        'Conid': 'Conid',
        'SecurityID': 'ID de Seguridad',
        'SecurityIDType': 'Tipo de ID de Seguridad',
        'CUSIP': 'CUSIP',
        'ISIN': 'ISIN',
        'FIGI': 'FIGI',
        'Issuer': 'Emisor',
        'Maturity': 'Vencimiento',
        'Buy/Sell': 'Compra/Venta',
        'SettleDate': 'Fecha de Liquidación',
        'TradeDate': 'Fecha de Operación',
        'Exchange': 'Bolsa',
        'Quantity': 'Cantidad',
        'AccruedInterest': 'Interés Acumulado',
        'Accrued (Days)': 'Días de Interés Acumulado',
        'Price': 'Precio',
        'Price (including Commissions)': 'Precio (incluyendo Comisiones)',
        'Amount': 'Cantidad',
        'TotalAmount': 'Cantidad Total'
    })
    indexed_dict = df_indexed.to_dict(orient='records')
    return {'data': indexed_dict}

query_function_map = {
    '986431': generate_trade_confirmation_message,
    '1321545': generate_excel_file,
}

@handle_exception
def generate(query_id, flex_query_dict, indices):
    generated = query_function_map[query_id](flex_query_dict=flex_query_dict, indices=indices)
    return generated