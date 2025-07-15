import pandas as pd
import numpy as np
from src.utils.logger import logger
from src.utils.exception import handle_exception
import re
from datetime import datetime
from src.utils.connectors.supabase import db

logger.announcement('Initializing Trade Tickets Service', type='info')
logger.announcement('Initialized Trade Tickets Service', type='success')

@handle_exception
def list_trade_tickets(query = None):
    logger.info(f"Getting trade tickets for {query}")

    # Get all trade tickets
    trade_tickets = db.read('trade_ticket', query={})

    return trade_tickets

@handle_exception
def fetch_trade_ticket(query_id):
    trades = fetchFlexQueries([query_id])
    return trades[query_id]

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

import requests as rq
import xml.etree.ElementTree as ET
import time
import pandas as pd
import csv

from src.utils.logger import logger
from src.utils.exception import handle_exception

logger.announcement('Initializing Flex Query Service', type='info')
version='&v=3'
url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?"
logger.announcement('Initialized Flex Query Service', type='success')

@handle_exception
def fetchFlexQueries(queryIds):
    agmToken = "t=419584539155539272816800"
    flex_queries = {}

    for _, queryId in enumerate(queryIds):

        flex_query_df = getFlexQuery(agmToken, queryId)
        flex_query_df['file_name'] = queryId

        try:
            flex_query_dict = flex_query_df.to_dict(orient='records')
            flex_queries[queryId] = flex_query_dict
        except Exception as e:
            logger.error(f'Flex Query Empty for queryId {queryId}: {str(e)}')
            raise Exception(f'Flex Query Empty for queryId {queryId}: {str(e)}')
        
    return flex_queries

def getFlexQuery(token, queryId):

    logger.info(f'Getting Flex Query for queryId: {queryId}')

    xml_data = None

    retry_count = 0
    max_retries = 5
    retry_delay = 1

    try:
        
        # Create url for GET request to API for generating a report
        logger.info('Requesting Flex Query Template...')
        generatedTemplateURL = "".join([url, token, '&q=' + queryId, version])
        generatedTemplateResponse = rq.get(url=generatedTemplateURL)
        while generatedTemplateResponse.status_code != 200 and retry_count < max_retries:
            logger.warning(f'Flex Query Template Generation Failed. Preview: {generatedTemplateResponse.text[0:100]}')
            logger.info(f'Retrying... Attempt {retry_count} of {max_retries}')
            time.sleep(retry_delay)
            generatedTemplateResponse = rq.get(url=generatedTemplateURL)
            retry_count += 1
    
    except Exception as e:
        logger.error(f'Error requesting Flex Query Template: {str(e)}')
        raise Exception(f'Error requesting Flex Query Template: {str(e)}')

    logger.success('Flex Query Template Generated')

    # Populate ET element with generated report template
    tree = ET.ElementTree(ET.fromstring(generatedTemplateResponse.content))
    root = tree.getroot()
    refCode = "&q=%s" % root.find('ReferenceCode').text

    try:

        # Create url for GET request to API to fetch generated report
        logger.info("Generating Flex Query...")
        generatedReportURL = root.find('Url').text
        generatedReportURL = "".join([generatedReportURL, "?",token, refCode, version])

        generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
        while 'ErrorCode' in generatedReportResponse.text and retry_count < max_retries:
            logger.warning(f'Flex Query Generation Failed. Preview: {generatedReportResponse.text[0:100]}')
            logger.info(f'Retrying... Attempt {retry_count} of {max_retries}')
            time.sleep(retry_delay)
            generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
            retry_count += 1

        if 'ErrorCode' in generatedReportResponse.text and 'Fail' in generatedReportResponse.text:
            logger.error(f'Flex Query Generation Failed. Error Code: {generatedReportResponse.text[0:100]}')
            raise Exception(f'Flex Query Generation Failed. Error Code: {generatedReportResponse.text[0:100]}')

    except Exception as e:
        logger.error(f'Error generating Flex Query: {str(e)}')
        raise Exception(f'Error generating Flex Query: {str(e)}')
    
    xml_data = generatedReportResponse.content
    df = binaryXMLtoDF(xml_data)
    logger.success(f"Flex Query generated")
    return df

def binaryXMLtoDF(binaryXMLData):

    logger.info(f'Converting binary XML to DF')
    xml_data = binaryXMLData.decode('utf-8')
    logger.info(f'Data Preview: {xml_data[0:500]}')
    if 'ErrorCode' in xml_data and 'ErrorMessage' in xml_data:
        raise Exception(f'Flex Query Generation Failed. Error in IBKR.')

    reader = csv.reader(xml_data.splitlines(), skipinitialspace=True)

    rows = []

    for row in reader:
        if ('BOA' not in row) and ('BOF' not in row) and ('BOS' not in row) and ('EOS' not in row) and ('EOA' not in row) and ('EOF' not in row) and ('MSG' not in row):
            rows.append(row)
    
    df = pd.DataFrame(rows[1:], columns=rows[0])
    logger.success(f'Successfully converted binary XML to DF')
    return df