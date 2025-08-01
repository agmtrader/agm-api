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
def list_trade_tickets(query = None):
    logger.info(f"Getting trade tickets for {query}")

    # Get all trade tickets
    trade_tickets = db.read('trade_ticket', query={})

    return trade_tickets

@handle_exception
def fetch_trade_ticket(query_id):
    trades = getFlexQuery(query_id)
    return trades

@handle_exception
def generate_trade_ticket(flex_query_dict, indices):

    logger.info('Generating trade ticket. Processing data...')

    # Create dataframe with indexed rows only
    flex_query_df = pd.DataFrame(flex_query_dict)
    df_indexed = flex_query_df.iloc[indices].copy()

    # Check if all rows in the Description column have the same value
    if df_indexed['Description'].nunique() != 1:
        logger.error('Not all rows in the Description column have the same value.')
        raise Exception('Not all rows in the Description column have the same value.')

    symbol, coupon, maturity = extract_bond_details(df_indexed['Description'].iloc[0])

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
    return consolidated_dict

@handle_exception
def generate_client_confirmation_message(consolidated_dict):

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
        "SettleDate",
        "TradeDate",
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
    return {'message': message}

def extract_bond_details(description):
    
    # Extract symbol (assuming it's always at the beginning)
    logger.info(f'Extracting bond details from description: {description}')
    symbol = description.split()[0]
    
    # Extract coupon (handling both mixed number and decimal formats)
    coupon_match = re.search(r'(\d+(?:\s+\d+/\d+|\.\d+)|\d+)', description)
    if coupon_match:
        coupon_str = coupon_match.group(1)
        if '/' in coupon_str:
            whole, fraction = coupon_str.split(' ')
            numerator, denominator = fraction.split('/')
            coupon = float(whole) + (float(numerator) / float(denominator))
        else:
            coupon = float(coupon_str)
    else:
        coupon = None
    
    # Extract date (assuming it's always at the end)
    date_match = re.search(r'(\d{2}/\d{2}/\d{2,4})$', description)
    if date_match:
        date_str = date_match.group(1)
        date_obj = datetime.strptime(date_str, '%m/%d/%y' if len(date_str) == 8 else '%m/%d/%Y')
        maturity = date_obj.strftime('%Y-%m-%d')
    else:
        maturity = None
    
    logger.success(f'Extracted bond details: symbol={symbol}, coupon={coupon}, maturity={maturity}')
    return symbol, coupon, maturity