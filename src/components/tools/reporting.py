from pandas.tseries.offsets import BDay
from datetime import datetime
from src.utils.logger import logger
import pandas as pd
from io import BytesIO
import base64
import yfinance as yf
import time
from src.components.accounts import read_accounts
from src.utils.connectors.drive import GoogleDrive
import pytz
from src.utils.connectors.flex_query_api import getFlexQuery
import pandas as pd
import os
import sys

logger.announcement('Initializing Reporting Service', type='info')
ratings = {
    # S&P Ratings
    "AAA": {"Short-term": "A-1+", "NAIC": 1, "Class1": "Prime", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 1, "S&P Equivalent": "AAA", "Source": "S&P"},
    "AA+": {"Short-term": "A-1+", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 2, "S&P Equivalent": "AA+", "Source": "S&P"},
    "AA": {"Short-term": "A-1+", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 3, "S&P Equivalent": "AA", "Source": "S&P"},
    "AA-": {"Short-term": "A-1+", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 4, "S&P Equivalent": "AA-", "Source": "S&P"},
    "A+": {"Short-term": "A-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 5, "S&P Equivalent": "A+", "Source": "S&P"},
    "A": {"Short-term": "A-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 6, "S&P Equivalent": "A", "Source": "S&P"},
    "A-": {"Short-term": "A-2", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 7, "S&P Equivalent": "A-", "Source": "S&P"},
    "BBB+": {"Short-term": "A-2", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 8, "S&P Equivalent": "BBB+", "Source": "S&P"},
    "BBB": {"Short-term": "A-3", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 9, "S&P Equivalent": "BBB", "Source": "S&P"},
    "BBB-": {"Short-term": "B", "NAIC": 3, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 10, "S&P Equivalent": "BBB-", "Source": "S&P"},
    "BB+": {"Short-term": "B", "NAIC": 3, "Class1": "Non-investment grade", "Class2": "Non-investment grade", "Class3": "Non-investment grade", "Level": 11, "S&P Equivalent": "BB+", "Source": "S&P"},
    "BB": {"Short-term": "B", "NAIC": 3, "Class1": "Speculative", "Class2": "AKA high-yield bonds", "Class3": "Non-investment grade", "Level": 12, "S&P Equivalent": "BB", "Source": "S&P"},
    "BB-": {"Short-term": "B", "NAIC": 3, "Class1": "Speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 13, "S&P Equivalent": "BB-", "Source": "S&P"},
    "B+": {"Short-term": "B", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 14, "S&P Equivalent": "B+", "Source": "S&P"},
    "B": {"Short-term": "B", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 15, "S&P Equivalent": "B", "Source": "S&P"},
    "B-": {"Short-term": "B", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 16, "S&P Equivalent": "B-", "Source": "S&P"},
    "CCC+": {"Short-term": "C", "NAIC": 5, "Class1": "Substantial risks", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 17, "S&P Equivalent": "CCC+", "Source": "S&P"},
    "CCC": {"Short-term": "C", "NAIC": 5, "Class1": "Extremely speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 18, "S&P Equivalent": "CCC", "Source": "S&P"},
    "CCC-": {"Short-term": "C", "NAIC": 5, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 19, "S&P Equivalent": "CCC-", "Source": "S&P"},
    "CC": {"Short-term": "C", "NAIC": 6, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 20, "S&P Equivalent": "CC", "Source": "S&P"},
    "C": {"Short-term": "C", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 21, "S&P Equivalent": "C", "Source": "S&P"},
    "D": {"Short-term": "/", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 22, "S&P Equivalent": "D", "Source": "S&P"},

    # Moody's Ratings
    "Aaa": {"Short-term": "P-1", "NAIC": 1, "Class1": "Prime", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 1, "S&P Equivalent": "AAA", "Source": "Moody's"},
    "Aa1": {"Short-term": "P-1", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 2, "S&P Equivalent": "AA+", "Source": "Moody's"},
    "Aa2": {"Short-term": "P-1", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 3, "S&P Equivalent": "AA", "Source": "Moody's"},
    "Aa3": {"Short-term": "P-1", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 4, "S&P Equivalent": "AA-", "Source": "Moody's"},
    "A1": {"Short-term": "P-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 5, "S&P Equivalent": "A+", "Source": "Moody's"},
    "A2": {"Short-term": "P-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 6, "S&P Equivalent": "A", "Source": "Moody's"},
    "A3": {"Short-term": "P-2", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 7, "S&P Equivalent": "A-", "Source": "Moody's"},
    "Baa1": {"Short-term": "P-2", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 8, "S&P Equivalent": "BBB+", "Source": "Moody's"},
    "Baa2": {"Short-term": "P-3", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 9, "S&P Equivalent": "BBB", "Source": "Moody's"},
    "Baa3": {"Short-term": "P-3", "NAIC": 3, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 10, "S&P Equivalent": "BBB-", "Source": "Moody's"},
    "Ba1": {"Short-term": "Not prime", "NAIC": 3, "Class1": "Non-investment grade", "Class2": "Non-investment grade", "Class3": "Non-investment grade", "Level": 11, "S&P Equivalent": "BB+", "Source": "Moody's"},
    "Ba2": {"Short-term": "Not prime", "NAIC": 3, "Class1": "Speculative", "Class2": "AKA high-yield bonds", "Class3": "Non-investment grade", "Level": 12, "S&P Equivalent": "BB", "Source": "Moody's"},
    "Ba3": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 13, "S&P Equivalent": "BB-", "Source": "Moody's"},
    "B1": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 14, "S&P Equivalent": "B+", "Source": "Moody's"},
    "B2": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 15, "S&P Equivalent": "B", "Source": "Moody's"},
    "B3": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 16, "S&P Equivalent": "B-", "Source": "Moody's"},
    "Caa1": {"Short-term": "Not prime", "NAIC": 5, "Class1": "Substantial risks", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 17, "S&P Equivalent": "CCC+", "Source": "Moody's"},
    "Caa2": {"Short-term": "Not prime", "NAIC": 5, "Class1": "Extremely speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 18, "S&P Equivalent": "CCC", "Source": "Moody's"},
    "Caa3": {"Short-term": "Not prime", "NAIC": 5, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 19, "S&P Equivalent": "CCC-", "Source": "Moody's"},
    "Ca": {"Short-term": "Not prime", "NAIC": 6, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 20, "S&P Equivalent": "CC", "Source": "Moody's"},
    "C": {"Short-term": "Not prime", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 21, "S&P Equivalent": "C", "Source": "Moody's"},
    "D": {"Short-term": "Not prime", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 22, "S&P Equivalent": "D", "Source": "Moody's"},
}
Drive = GoogleDrive()
cst = pytz.timezone('America/Costa_Rica')
cst_time = datetime.now(cst)
batch_folder_id = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'
backups_folder_id = '1d9RShyGidP04XdnH87pUHsADghgOiWj3'
resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
logger.announcement('Initialized Reporting Service', type='success')

"""
TODO:
- Fetch and upload the other four sources:
    - Clients List
    - Contact List Summary
    - RTD
    - Tasks for Subaccounts
"""

def get_clients_report():
    """
    Get the clients list.
    
    :return: Response object with clients list or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    clients_file = [client for client in files_in_resources_folder if 'ibkr_clients' in client['name']]
    if len(clients_file) != 1:
        logger.error('Clients file not found or multiple files found')
        raise Exception('Clients file not found or multiple files found')
    clients = Drive.download_file(file_id=clients_file[0]['id'], parse=True)
    return clients

def get_client_fees():
    """
    Get the client fees.
    
    :return: Response object with client fees or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    client_fees_file = [client_fees for client_fees in files_in_resources_folder if 'ibkr_client_fees' in client_fees['name']]
    logger.info(f'Client fees file: {client_fees_file}')
    if len(client_fees_file) != 1:
        logger.error('Client fees file not found or multiple files found')
        raise Exception('Client fees file not found or multiple files found')
    client_fees = Drive.download_file(file_id=client_fees_file[0]['id'], parse=True)
    return client_fees

def get_nav_report():
    """
    Get the NAV report.
    
    :return: Response object with NAV report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    nav_file = [nav for nav in files_in_resources_folder if 'ibkr_nav' in nav['name']]
    if len(nav_file) != 1:
        logger.error('Nav file not found or multiple files found')
        raise Exception('Nav file not found or multiple files found')
    nav = Drive.download_file(file_id=nav_file[0]['id'], parse=True)
    return nav

def get_rtd_report():
    """
    Get the RTD report.
    
    :return: Response object with RTD report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    rtd_file = [rtd for rtd in files_in_resources_folder if 'ibkr_rtd' in rtd['name']]
    if len(rtd_file) != 1:
        logger.error('RTD file not found or multiple files found')
        raise Exception('RTD file not found or multiple files found')
    rtd = Drive.download_file(file_id=rtd_file[0]['id'], parse=True)
    return rtd  

def get_open_positions_report():
    """
    Get the open positions report.
    
    :return: Response object with open positions report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    open_positions_file = [open_positions for open_positions in files_in_resources_folder if 'ibkr_open_positions_all' in open_positions['name']]
    if len(open_positions_file) != 1:
        logger.error('Open positions file not found or multiple files found')
        raise Exception('Open positions file not found or multiple files found')
    open_positions = Drive.download_file(file_id=open_positions_file[0]['id'], parse=True)
    return open_positions

def get_proposals_equity_report():
    """
    Get the proposals equity report.
    
    :return: Response object with proposals equity report or error message
    """
    proposals_equity = Drive.export_file(file_id='1AqpIE7LRV40J-Aew5fA-P6gEfji3Yb-Rp5DohI9BQFY', mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', parse=True)
    return proposals_equity

def get_securities_bond_dictionary():
    """
    Get the securities bond dictionary.
    
    :return: Response object with securities bond dictionary or error message
    """
    securities_bond_dictionary = Drive.download_file(file_id='1hNhc35aug_smWefPkT8mDLil97_i8_E9', parse=True)
    return securities_bond_dictionary

"""
ETL PIPELINE
"""

def run():
    """
    Run the ETL pipeline.
    
    :return: Response object with success message or error message
    """
    try:
        extract()
        transform()
    except:
        logger.error(f'Error running ETL pipeline')
        raise Exception(f'Error running ETL pipeline')
    return {'status': 'success'}

def extract() -> dict:
    logger.announcement('Generating Reports.', type='info')

    # Fetch Flex Queries
    logger.announcement('Fetching Flex Queries.', type='info')
    flex_query_ids = ['732383', '734782', '742588']
    flex_queries = {}
    for query_id in flex_query_ids:
        try:
            flex_queries[query_id] = getFlexQuery(query_id)
        except:
            logger.error(f'Error fetching Flex Query for {query_id}')
            raise Exception(f'Error fetching Flex Query for {query_id}')
    logger.announcement('Flex Queries fetched.', type='success')

    # Upload Flex Queries to batch folder
    logger.announcement('Uploading Flex Queries to batch folder.', type='info')
    for key, value in flex_queries.items():
        try:
            Drive.upload_file(file_name=key, mime_type='text/csv', file_data=value, parent_folder_id=batch_folder_id)
        except:
            logger.error(f'Error uploading Flex Query for {key}')
            raise Exception(f'Error uploading Flex Query for {key}')
        
    logger.announcement('Flex Queries uploaded to batch folder.', type='success')
    time.sleep(2)

    # Rename files in batch folder
    logger.announcement('Renaming files in batch folder.', type='info')
    number_renamed = rename_files_in_batch()
    logger.announcement(f'{number_renamed} files renamed successfully.', type='success')
    
    # Sort files to respective backup folders
    logger.announcement('Sorting files to backup folders.', type='info')
    number_sorted = sort_batch_files_to_backup_folders()
    logger.announcement(f'{number_sorted} files sorted to backup folders.', type='success')

    logger.announcement('Reports successfully extracted.', type='success')
    return {'status': 'success'}

def transform() -> dict:
    logger.announcement('Transforming reports.', type='info')

    # Clear resources folder
    logger.announcement('Clearing resources folder.', type='info')
    Drive.clear_folder(folder_id=resources_folder_id)
    logger.announcement('Resources folder cleared.', type='success')

    # Process files in each backup folder
    logger.announcement('Processing files.', type='info')
    for report_type, config in report_configs.items():
        logger.announcement(f'Processing {report_type.capitalize()} file.', type='info')
        process_report(config)
    logger.announcement('Files processed.', type='success')

    logger.announcement('Reports successfully transformed.', type='success')
    return {'status': 'success'}

"""
EXTRACT HELPERS
"""
def rename_files_in_batch():
    """
    Rename files in the batch folder based on specific naming conventions.
    :return: Response object with updated batch files or error message
    """
    # Get the current time in CST
    today_date = cst_time.strftime('%Y%m%d%H%M')
    yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
    first_date = cst_time.replace(day=1).strftime('%Y%m%d')

    # Get all files in batch
    batch_files = Drive.get_files_in_folder(batch_folder_id)

    count = 0

    # Rename files based on specific patterns
    for f in batch_files:
        if ('742588' in f['name']):
            new_name = ('742588_' + yesterday_date + '.csv')
        elif ('734782' in f['name']):
            new_name = ('734782_' + yesterday_date + '.csv')
        elif ('732383' in f['name']):
            new_name = ('732383_' + first_date + '_' + yesterday_date + '.csv')
        elif ('clients' in f['name']):
            new_name = ('clients ' + today_date + ' agmtech212' + '.xls')
        elif ('tasks_for_subaccounts' in f['name']):
            new_name = ('tasks_for_subaccounts ' + today_date + ' agmtech212' + '.csv')
        elif ('ContactListSummary' in f['name']):
            new_name = ('ContactListSummary ' + today_date + ' agmtech212' + '.csv')
        elif ('RTD' in f['name']):
            new_name = ('RTD ' + today_date + '.csv')
        else:
            logger.error(f'File {f["name"]} did not match any pattern in when renaming files in batch. Defaulting to yesterday date.')
            new_name = f['name'] + yesterday_date + '.csv'

        try:
            Drive.rename_file(file_id=f['id'], new_name=new_name)
            count += 1
        except:
            logger.error(f'Error renaming file.')
            raise Exception(f'Error renaming file.')
        
    logger.announcement(f'{count} files renamed.', type='success')
    return count

def sort_batch_files_to_backup_folders():
    """
    Sort files from the batch folder into their respective backup folders.
    
    :param batch_files: List of files in the batch folder
    :return: Response object with success message or error
    """
    folder_names = ['TasksForSubaccounts', 'ContactListSummary', 'RTD', 'Clients', '742588', '734782', '732383']
    folders_info = {}

    # Get info for all backup folders
    for folder_name in folder_names:
        folder_info = Drive.get_folder_info(backups_folder_id, folder_name)
        folders_info[folder_name] = folder_info
    
    # Assign folder info to variables
    subaccounts_folder_info = folders_info['TasksForSubaccounts']
    contacts_folder_info = folders_info['ContactListSummary']
    rtd_folder_info = folders_info['RTD']
    clients_folder_info = folders_info['Clients']
    open_positions_folder_info = folders_info['742588']
    nav_folder_info = folders_info['734782']
    client_fees_folder_info = folders_info['732383']

    # Get all files in batch
    batch_files = Drive.get_files_in_folder(batch_folder_id)

    count = 0

    # Sort files to their respective folders
    for f in batch_files:
        # Determine the destination folder based on file name
        if 'clients' in f['name']:
            new_parent_id = clients_folder_info['id']
        elif 'ContactListSummary' in f['name']:
            new_parent_id = contacts_folder_info['id']
        elif 'tasks_for_subaccounts' in f['name']:
            new_parent_id = subaccounts_folder_info['id']
        elif 'RTD' in f['name']:
            new_parent_id = rtd_folder_info['id']
        elif '742588' in f['name']:
            new_parent_id = open_positions_folder_info['id']
        elif '734782' in f['name']:
            new_parent_id = nav_folder_info['id']
        elif '732383' in f['name']:
            new_parent_id = client_fees_folder_info['id']
        else:
            new_parent_id = 'root'

        # Move file to destination
        try:
            Drive.move_file(f=f, newParentId=new_parent_id)
            count += 1
        except:
            logger.error(f'Error moving file to destination')
            raise Exception(f'Error moving file to destination')

    logger.announcement(f'{count} files moved to backup folders.', type='success')
    return count

"""
TRANSFORM HELPERS
"""
def process_report(config):
    """
    Process a single report according to its configuration.
    
    :param config: Dictionary containing report configuration
    :return: Response object with success message or error
    """
    folder_id = config['folder_id']
    output_filename = config['output_filename']
    
    # Get files in the report's backup folder
    files = Drive.get_files_in_folder(folder_id)

    if len(files) == 0:
        logger.error(f'No files found in backup folder.')
        raise Exception('No files found in backup folder.')

    # Get most recent file
    most_recent_file = get_most_recent_file(files)

    # Download file and read into dataframe
    f = Drive.download_file(file_id=most_recent_file['id'], parse=True)
    file_df = pd.DataFrame(f)
    file_df = file_df.fillna('')

    # Apply custom processing if specified
    if 'process_func' in config:
        file_df = config['process_func'](file_df)

    # Upload processed file to Resources folder
    file_dict = file_df.to_dict(orient='records')
    Drive.upload_file(file_name=output_filename, mime_type='text/csv', file_data=file_dict, parent_folder_id=resources_folder_id)
    return

def process_rtd(df):
    """
    Process the RTD file.
    
    :param df: Input dataframe
    :return: Processed dataframe
    """
    
    df = df[['Symbol',
            'Financial Instrument',
            'Company Name',
            'Bid Size',
            'Bid',
            'Bid Yield',
            'Ask Size',
            'Ask',
            'Ask Yield',
            'Industry',
            'Sector',
            #'Duration %  ',
            'Current Yield',
            'Maturity',
            'Next Option Date',
            'Coupon',
            'Last',
            'Ratings',
            'Payment Frequency',
            'Trading Currency',
            'Issue Date',
            'Last Trading Date',
            #'Face Value',
            #"Moody's",
            #'S&P',
            #'Bond Features',
            #'Time-To-Maturity (TTM)'
            ]]
    
    numeric_columns = ['Bid',
            'Ask',
            'Bid Yield',
            'Ask Yield',
            'Bid Size',
            'Ask Size',
            'Current Yield',
            #'Duration %  ',
            'Coupon',
            'Last',
            #'Face Value',
            #'Time-To-Maturity (TTM)'
            ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    date_columns = [
        'Next Option Date',
        'Last Trading Date',
        'Maturity',
        'Issue Date'
        ]
    
    for column in date_columns:
        try:
            df[column] = pd.to_datetime(df[column], errors='coerce')
        except:
            df[column] = df[column]

    df['CY'] = df.apply(lambda row: get_current_yield(row['Coupon'], row['Last'], row['Ask'], row['Bid']), axis=1)
    df['Price'] = df.apply(lambda row: get_first_valid_price(row['Ask'], row['Bid'], row['Last']), axis=1)
    df['Frequency'] = df['Payment Frequency'].apply(get_payment_frequency_from_text)
    df['Price Cluster'] = df['Price'].apply(get_bond_price_cluster)
    df['Size Preasure'] = df.apply(lambda row: get_size_preasure(row['Ask Size'], row['Bid Size']), axis=1)
    df['Duration'] = df.apply(lambda row: get_bond_duration(row['Maturity'], 
        row['Coupon']/100, # Convert coupon percentage to decimal
        row['Price'],
        row['Frequency']), 
        axis=1)
    
    # Create new columns for Moody's and S&P ratings
    df['Moodys'], df['SP'] = zip(*df['Ratings'].apply(extract_rating_from_text))
    df['Moodys'] = df['Moodys'].apply(clean_rating_text)
    df['SP'] = df['SP'].apply(clean_rating_text)

    # Extract first chars before space from Financial Instrument column
    df['Issuer'] = df['Financial Instrument'].str.split().str[0]

    #Calculate years to maturity
    today = pd.Timestamp.today()
    df['Years to Maturity'] = df.apply(lambda row: get_years_to_date(today, row['Maturity']), axis=1)
    df['YTM'] = df.apply(lambda row: get_bond_ytm(row['Price'], row['Coupon'], row['Years to Maturity']), axis=1)

    def get_rating_level(rating):
        """
        Get the level for a single rating from the ratings dictionary.
        Returns None if rating is not found.
        """
        if pd.isna(rating) or rating not in ratings:
            return None
        return ratings[rating]["Level"]

    # Get levels for both SP and Moodys ratings
    df['SP_Level'] = df['SP'].apply(get_rating_level)
    df['Moodys_Level'] = df['Moodys'].apply(get_rating_level)

    # Get the lowest (highest number) level between SP and Moodys
    df['Rating Level'] = df[['SP_Level', 'Moodys_Level']].max(axis=1)

    # Drop the temporary columns
    df = df.drop(['SP_Level', 'Moodys_Level'], axis=1)


    # Create a new dictionary with only S&P ratings
    sp_dict = {rating: info for rating, info in ratings.items() if info["Source"] == "S&P"}

    # Create S&P Equivalent column by mapping Rating Level to S&P rating
    def get_sp_equivalent(level):
        """
        Get the S&P equivalent rating for a given level from the ratings dictionary.
        Returns None if level is not found.
        """
        if pd.isna(level):
            return None
        
        # Find the first rating entry that matches the level
        for rating, info in ratings.items():
            if info["Level"] == level:# and info["Source"] == "S&P":
                return rating
        return None

    df['S&P Equivalent'] = df['Rating Level'].apply(get_sp_equivalent)

    # Filter df to only include rows where Symbol contains 'IBCID'
    df_ibcid = df[df['Symbol'].str.contains('IBCID', na=False)]

    return df_ibcid

def process_clients_file(sheets_df):
    """
    Process the clients file by concatenating all sheets into a single dataframe.
    
    :param df: Input dataframe from Excel file
    :return: Processed dataframe
    """
    return sheets_df

def process_open_positions_template(df):
    """
    Process the open positions template file.
    
    This function performs the following steps:
    1. Upload the full file to the resources folder
    2. Filter the dataframe for BOND asset class and LOT level of detail
    3. Reorder columns according to the specified order
    
    :param df: Input dataframe
    :return: Processed dataframe
    """
    # Upload the full file
    full_dict = df.to_dict(orient='records')
    Drive.upload_file(file_name='ibkr_open_positions_all.csv', mime_type='text/csv', file_data=full_dict, parent_folder_id=resources_folder_id)
    
    # Generate template (extract bonds and details)
    df = df[(df['AssetClass'] == 'BOND') & (df['LevelOfDetail'] == 'LOT')]

    # Extract only the columns that are needed
    file_columns = [
        'ClientAccountID',
        'AccountAlias',
        'Model',
        'CurrencyPrimary',
        'FXRateToBase',
        'AssetClass',
        'Symbol',
        'Description',
        'Conid',
        'SecurityID',
        'SecurityIDType',
        'CUSIP',
        'ISIN',
        'ListingExchange',
        'UnderlyingConid',
        'UnderlyingSymbol',
        'UnderlyingSecurityID',
        'UnderlyingListingExchange',
        'Issuer',
        'Multiplier',
        'Strike',
        'Expiry',
        'Put/Call',
        'PrincipalAdjustFactor',
        'ReportDate',
        'Quantity',
        'MarkPrice',
        'PositionValue',
        'PositionValueInBase',
        'OpenPrice',
        'CostBasisPrice',
        'CostBasisMoney',
        'PercentOfNAV',
        'FifoPnlUnrealized',
        'UnrealizedCapitalGainsPnl',
        'UnrealizedFxPnl',
        'Side',
        'LevelOfDetail',
        'OpenDateTime',
        'HoldingPeriodDateTime',
        'Code',
        'OriginatingOrderID',
        'OriginatingTransactionID',
        'AccruedInterest',
        'VestingDate',
        'SerialNumber',
        'DeliveryType',
        'CommodityType',
        'Fineness',
        'Weight'
    ]
    df = df[file_columns]

    # Create formula columns
    formula_columns = [
        'KEY',
        'securities_bond row',
        'Maturity',
        'Coupon',
        'Sector',
        'Frequency',
        'Open Date',
        'Column5',
        'Column6',
        'Tasa',
        'Column7',
        'Meses en Cartera',
        'Rendimiento Acumulado x Cupón',
        'Current Price - 100',
        'Duraciones',
        'MDURATION',
        'DURATION',
        'Column8',
        'Market Price',
        'Yield',
        'Rate',
        'RTD MATCH CONID',
        'RTD Duration',
        'RTD Bid',
        'RTD Ask',
        'RTD Credit Rating',
        'RTD Credit Rating Level',
        'RTD Ask Value',
        'a',
        'a2',
        'Change in Price + Accrued Interest Received',
        'Year on Portfolio',
        'Yield (Price + Interest)',
        'a3',
        'Duration FX',
        'Coupon * Quantity',
        'Credit Rating Main Class',
        'Investment Grade Amt',
        'Column1',
        'Issuer FX'
    ]
    formula_df = pd.DataFrame(columns=formula_columns)

    # Concatenate the two dataframes horizontally
    concatenated_df = pd.concat([df, formula_df], axis=1)
    concatenated_df = concatenated_df.fillna('')

    return concatenated_df

def get_finance_data():
    """
    Get finance data from the finance folder.
    
    :return: Response object with finance data or error message
    """

    proposals_equity_list_id = '1AqpIE7LRV40J-Aew5fA-P6gEfji3Yb-Rp5DohI9BQFY'
    
    raw_file = Drive.export_file(file_id=proposals_equity_list_id, mime_type='text/csv', parse=False)

    try:
        # Convert binary response to DataFrame
        data = pd.read_csv(BytesIO(raw_file))
    except:
        raise Exception('Error processing file.')
    
    # Rest of your code using 'data' DataFrame instead of previous undefined 'data' variable
    ticker_list = data['Ticker'].tolist()

    df = yf.download(tickers=ticker_list, period= 'max', interval= '1d')
    logger.info(f'Downloaded data from Yahoo Finance')
    df = df.sort_index(ascending=False)
    logger.info(f'Sorted data from Yahoo Finance')
    df2 = df.iloc[[0,251,503,755,1007,1259]]
    logger.info(f'Filtered data from Yahoo Finance')

    # EXPORT DATASET
    filename =  'dataset_PX_5Y.csv'

    # Instead of writing to file, write to memory
    csv_buffer = BytesIO()
    df2.to_csv(csv_buffer, index=True)
    csv_buffer.seek(0)
    logger.info(f'Exported data to memory')
    base64_data = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')

    # Upload the CSV data from memory
    Drive.upload_file(file_name=filename, mime_type='text/csv', file_data=base64_data, parent_folder_id=resources_folder_id)
    return {'status': 'success'}

report_configs = {
    'clients': {
        'folder_id': '1FNcbWNptK-A5IhmLws-R2Htl85OSFrIn',
        'output_filename': 'ibkr_clients.csv',
        'process_func': process_clients_file,
    },
    'rtd': {
        'folder_id': '12L3NKflYtMiisnZOpU9aa1syx2ZJA6JC',
        'output_filename': 'ibkr_rtd.csv',
        'process_func': process_rtd,
    },
    'open_positions': {
        'folder_id': '1JL4__mr1XgOtnesYihHo-netWKMIGMet',
        'output_filename': 'ibkr_open_positions_template.csv',
        'process_func': process_open_positions_template,
    },
    'client_fees': {
        'folder_id': '1OnSEo8B2VUF5u-VkhtzZVIzx6ABe_YB7',
        'output_filename': 'ibkr_client_fees.csv',
    },
    'nav': {
        'folder_id': '1WgYA-Q9mnPYrbbLfYLuJZwUIWBYjiD4c',
        'output_filename': 'ibkr_nav_in_base.csv',
    },
}

"""
HELPER FUNCTIONS
"""

def cluster_bonds_by_price(DataFrame, PriceColumn):
    """
    Adds a cluster column to a bond DataFrame based on price ranges.
    
    Args:
        df (pd.DataFrame): DataFrame containing bond data with 'Price' column
        
    Returns:
        pd.DataFrame: DataFrame with added 'Cluster' column
    """
    # Define price bins and labels
    bins = [0, 80, 95, 105, 120, float('inf')]
    labels = ['Deep Discount', 'Discount', 'Par', 'Premium', 'High Premium']
    
    # Add cluster column based on price ranges
    DataFrame['Price Cluster'] = pd.cut(DataFrame[PriceColumn], bins=bins, labels=labels, include_lowest=True)
    
    return DataFrame

def get_bond_price_cluster(price):
    """
    Returns the price cluster label for a given bond price.
    
    Args:
        price (float): Bond price
        
    Returns:
        str: Price cluster label ('Deep Discount', 'Discount', 'Par', 'Premium', or 'High Premium')
    """
    # Define same bins and labels as cluster_bonds_by_price
    bins = [0, 70, 95, 105, 120, float('inf')]
    labels = ['Deep Discount', 'Discount', 'Par', 'Premium', 'High Premium']
    
    # Find which bin the price falls into
    for i in range(len(bins)-1):
        if bins[i] <= price < bins[i+1]:
            return labels[i]
            
    return None  # Return None if price doesn't fall in any bin

def get_first_valid_price(ask, bid, last):
    """
    Returns the first valid price from bid, ask, or last price in that order.
    Returns None if no valid price is found.
    """
    
    if pd.notnull(ask) and ask != 0:
        return ask
    elif pd.notnull(bid) and bid != 0:
        return bid
    elif pd.notnull(last) and last != 0:
        return last
    else:
        return None

def get_size_preasure(BID_size, ASK_size):

    try:
        size_preasure = BID_size - ASK_size
        return size_preasure
    except Exception as e:
        return None

def get_current_yield(coupon, last, ask, bid):
    """
    Calculate current yield using first available price (Last, Ask, or Bid)
    
    Args:
        coupon (float): Bond coupon rate
        last (float): Last traded price
        ask (float): Ask price
        bid (float): Bid price
        
    Returns:
        float: Current yield or None if no valid price available
    """

    if coupon is None or coupon == 0 or not isinstance(coupon, (int, float)):
        return None
    else:
        if pd.notnull(last) and last != 0:
            return coupon / last
        elif pd.notnull(ask) and ask != 0:
            return coupon / ask
        elif pd.notnull(bid) and bid != 0:
            return coupon / bid
        else:
            return None

def get_bond_duration(maturity_date, coupon_rate, price, frequency):
    
    """
    Calculate the Macaulay Duration for a bond
    
    Args:
        maturity_date: The maturity date of the bond (datetime.date)
        coupon_rate: Annual coupon rate as decimal (e.g. 0.05 for 5%)
        price: Clean price of the bond as percentage of par (e.g. 100 for par)
        frequency: Number of coupon payments per year (default=2 for semi-annual)
        
    Returns:
        duration: Macaulay Duration in years
    """

    # Default to semi-annual frequency if not provided because it's the most common frequency
    if frequency is None:
        frequency = 2


    try:
        # Convert maturity_date to datetime.date if it's datetime
        if isinstance(maturity_date, datetime.datetime):
            maturity_date = maturity_date.date()

        today = datetime.date.today()
        
        # Time to maturity in years
        t = (maturity_date - today).days / 360.0

        if t <= 0:
            return 0

        # Convert annual rates to per-period rates
        period_coupon = coupon_rate / frequency

        periods = max(1, int(t * frequency))  # Ensure at least 1 period
        r = ( ( coupon_rate * 100 ) / price) / frequency

        # Calculate present value of each cash flow
        pv_factors = [(1 + r) ** (-i) for i in range(1, periods + 1)]
        cash_flows = [period_coupon * 100] * periods
        cash_flows[-1] += 100  # Add principal repayment at maturity
        
        # Calculate weighted present values
        weighted_pvs = [cf * pvf * (i/frequency) for i, (cf, pvf) in enumerate(zip(cash_flows, pv_factors), 1)]
        
        # Macaulay Duration formula
        duration = sum(weighted_pvs) / price

    except Exception as e:
        return 0

    return duration

def get_payment_frequency_from_text(frequency_text):
    """
    Convert payment frequency text to number of payments per year.
    
    Args:
        frequency_text (str): Payment frequency text (e.g., 'Semi-Annual', 'Quarterly', 'Annual')
        
    Returns:
        int: Number of payments per year (e.g., 2 for Semi-Annual, 4 for Quarterly, 1 for Annual)
    """
    if pd.isna(frequency_text):
        return None
        
    frequency_text = str(frequency_text).lower().strip()
    
    # Common frequency mappings
    frequency_map = {
        'annual': 1,
        'semi-annual': 2,
        'semi annual': 2,
        'quarterly': 4,
        'monthly': 12,
        'zero': 0,
        'zero coupon': 0
    }
    
    # Try exact match first
    if frequency_text in frequency_map:
        return frequency_map[frequency_text]
        
    # Try partial matches
    for key, value in frequency_map.items():
        if key in frequency_text:
            return value
            
    # Default to semi-annual if no match found (most common for bonds)
    return 2

def get_bond_ytm(price, coupon, years_to_maturity):
    try:
        # Use the first available price (Last, Ask, or Bid)
        price = price if pd.notnull(price) else None
                
        if price is None or years_to_maturity == 0:
            return None
            
        numerator = coupon + ((100 - price) / years_to_maturity)
        denominator = (100 + price) / 2
        
        return numerator / denominator
    except:
        return None

def get_years_to_date(start_date, end_date):
    """
    Calculate years to maturity from today to a given maturity date.
    
    Args:
        maturity_date (datetime): The maturity date of the bond
        
    Returns:
        float: Number of years to maturity, or None if invalid input
    """
    try:
        if pd.isna(end_date):
            return None
            
        years = (end_date - start_date).total_seconds() / (365.25 * 24 * 60 * 60)
        return years if years > 0 else None
        
    except:
        return None

def get_fraction_as_float(fraction_text):
    try:
        # Check if there's a space indicating a whole number and a fraction
        if ' ' in fraction_text:
            whole_number, fraction = fraction_text.split(' ')
            # Split the fraction into numerator and denominator
            numerator, denominator = fraction.split('/')
            # Convert to float: whole number + fraction part
            return float(whole_number) + float(numerator) / float(denominator)
        else:
            # If no fraction, convert the text directly to float
            return float(fraction_text)
    except Exception as e:
        return None  # Return None or some default value in case of conversion error

def get_coupon_from_ibkr_description(text):

    text = text.replace("  ", " ").strip()
    text = text.split(' ')
    
    elements = len(text)

    if elements == 3:
        coupon = text[1]

    elif elements == 4:
        coupon = f'{text[1]} {text[2]}'

    else:
        None

    coupon = get_fraction_as_float(coupon)
    return coupon

def get_maturity_from_ibkr_description(text):

    text = text.replace("  ", " ").strip()
    text = text.split(' ')
    
    elements = len(text)

    if elements == 3:
        maturity = text[2]

    elif elements == 4:
        maturity = text[3]

    else:
        None

    # Convert maturity string (MM/DD/YY) to datetime
    try:
        if maturity and '/' in maturity:
            month, day, year = maturity.split('/')
            # Add '20' prefix to year if it's 2 digits
            if len(year) == 2:
                year = '20' + year
            maturity = datetime.datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d').date()
    except Exception as e:
        maturity = None
    return maturity

def extract_rating_from_text(rating_text):
    """
    Extract rating information from text in formats like:
    - 'CAA3/CCC+ (MOODY/SP)' for dual ratings
    - 'CAA3 (MOODY)' for single rating
    Returns a tuple of (moody_rating, sp_rating)
    """
    rating_text = str(rating_text)
    
    # Find position of '('
    open_paren_pos = rating_text.find('(')
    if open_paren_pos == -1:
        return None, None
        
    # Get the rating and agency parts
    rating_part = rating_text[:open_paren_pos].strip()
    agency_part = rating_text[open_paren_pos+1:-1].strip()
    
    # Initialize ratings
    moody_rating = None
    sp_rating = None
    
    # Check if it's a dual rating format
    if '/' in rating_part and '/' in agency_part:
        ratings = rating_part.split('/')
        agencies = agency_part.split('/')
        
        if len(ratings) == 2 and len(agencies) == 2:
            for rating, agency in zip(ratings, agencies):
                if 'MOODY' in agency.upper():
                    moody_rating = rating.strip()
                elif 'SP' in agency.upper():
                    sp_rating = rating.strip()
    else:
        # Single rating format
        if 'MOODY' in agency_part.upper():
            moody_rating = rating_part.strip()
        elif 'SP' in agency_part.upper():
            sp_rating = rating_part.strip()
            
    return moody_rating, sp_rating

def clean_rating_text(text):
    # Handle None or non-string inputs
    if text is None or not isinstance(text, str):
        return None
        
    # Create new text keeping only characters from chars list
    chars = ['A','a','B','b','C','1','2','3','/','-','+']

    new_text = ''.join(c for c in text if c in chars)
    new_text = new_text.strip()
    return new_text

def list_files_recursively(root_folder):
    """
    Creates a list of full paths for all files in a folder and its subfolders.
    
    Args:
        root_folder (str): Path to the root folder to search
        
    Returns:
        list: List of full file paths found recursively
    """
    file_paths = []
    
    # Walk through directory tree
    for dirpath, dirnames, filenames in os.walk(root_folder):
        for filename in filenames:
            # Create full file path by joining directory path and filename
            full_path = os.path.join(dirpath, filename)
            file_paths.append(full_path)
            
    return file_paths

def get_file_created_date(file_path):
    """Gets the creation date of a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Datetime object of when file was created
    """
    try:
        # Get file creation timestamp
        # On macOS, st_birthtime gives actual creation time
        if sys.platform == 'darwin':
            creation_time = os.stat(file_path).st_birthtime
        else:
            # On other platforms, use earliest of ctime/mtime 
            stats = os.stat(file_path)
            creation_time = min(stats.st_ctime, stats.st_mtime)
        
        # Convert timestamp to datetime object
        creation_datetime = datetime.datetime.fromtimestamp(creation_time)
        
        return creation_datetime
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise Exception(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Error getting file created date: {e}")
        raise Exception(f"Error getting file created date: {e}")

def get_latest_ibkr_query(folder_path: str, query_number: str, daily: bool):

    # PARAMS

    if daily == True: date_length = 8
    if query_number == 'RTD': date_length = 12
    else: date_length = 17

    if query_number == 'RTD': delimiter = ' '
    else: delimiter = '_'

    query_length = len(query_number) + 1 + date_length + 4


    # FUNCTION

    all_paths = list_files_recursively(folder_path)

    selected_files = []
    for file in all_paths:
        
        filename = os.path.basename(file)

        if filename.startswith(f'{query_number}{delimiter}') and filename.endswith('.csv') and len(filename) == query_length:
            selected_files.append(filename)
        else:
            filename = None
    
    selected_files = sorted(selected_files, reverse=True) # Sort files in descending order
            
    latest_filename = selected_files[0] # Get the latest uploaded file

    # Find the full path of latest uploaded file
    latest_filepath = None
    for path in all_paths:
        if os.path.basename(path) == latest_filename:
            latest_filepath = path
            break


    return latest_filepath

def rename_files_for_daily_reports(folder_path):


    def rename_file_with_date(file_path):
        """Renames a file by adding date in YYYYMMDD format and 'agmtech212'.
        
        Args:
            file_path: Path to the file to rename
            
        Returns:
            Path to the renamed file
        """
        try:
            # Get directory and filename
            directory = os.path.dirname(file_path)
            filename = os.path.basename(file_path)
            
            # Get file extension
            name, ext = os.path.splitext(filename)
            
            # Get file creation date
            creation_date = get_file_created_date(file_path)

            # Convert creation date to YYYYMMDD format
            creation_date_str = creation_date.strftime('%Y%m%d%H%M')

            # Create new filename
            if filename == 'RTD.csv':
                new_filename = f"{name} {creation_date_str}{ext}"
            else:
                new_filename = f"{name} {creation_date_str} agmtech212{ext}"
            
            new_filepath = os.path.join(directory, new_filename)
            
            # Rename the file
            os.rename(file_path, new_filepath)
            
            return new_filepath
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise Exception(f"File not found: {file_path}")
        except Exception as e:
            logger.error(f"Error renaming file: {e}")
            raise Exception(f"Error renaming file: {e}")


    FilesToRename = ['clients.xls',
                    'tasks_for_subaccounts.csv',
                    'ContactListSummary.csv',
                    'RTD.csv']


    def list_files_in_folder(folder_path):
        """Lists all files in the specified folder.
        
        Args:
            folder_path: Path to the folder to list files from
            
        Returns:
            List of filenames in the folder, or empty list if error
        """
        try:
            # Get list of files in the folder
            files = os.listdir(folder_path)
            
            return files
            
        except FileNotFoundError:
            return []
        except Exception as e:
            return []


    folder_path = '/Users/agm_crf/Downloads'
    list_files_in_folder(folder_path)

    ListOfFiles = list_files_in_folder(folder_path)

    for file in FilesToRename:
        if file in ListOfFiles:
            rename_file_with_date(f'{folder_path}/{file}')

def rename_ibkr_batch_files(folder_path):

    all_files = os.listdir(folder_path)

    queries = ['732383', '732385', '734782', '742588']

    for file in all_files:
        # Skip files that don't match our criteria
        if not (file.endswith('.csv') and ('_' in file or any(query in file for query in queries))):
            continue
            
        
        # Only try to split if there are underscores
        if '_' in file:
            filename = file.split('_')
            
            # Make sure filename has enough parts before accessing index 5
            if len(filename) > 5 and any(query in filename[5] for query in queries) and filename[0] == 'I6413690' and filename[1] == 'all' and filename[4] == 'AF':
                

                date = []
                if filename[2] == filename[3]:
                    date = filename[2]
                else:
                    date = f'{filename[2]}_{filename[3]}'

                file_path = os.path.join(folder_path, file)
                new_filename = f'{filename[5]}_{date}.csv'
                new_filepath = os.path.join(folder_path, new_filename)
                os.rename(file_path, new_filepath)

    return

def get_filename_from_path(path):

    return os.path.basename(path)

def melt_nav(file_path):

    df = pd.read_csv(file_path)

    filename = os.path.basename(file_path)

    df['Filename'] = filename

    df_melted = pd.melt(df, id_vars=['ReportDate', 'Filename', 'ClientAccountID'])

    #df_melted.to_excel('/Users/agm_crf/Downloads/nav_melted.xlsx', index=False)

    #subprocess.run(['open', '/Users/agm_crf/Downloads/nav_melted.xlsx'])

    return df_melted

def get_most_recent_file(files):
    """
    Get the most recent file from a list of files based on creation time.
    
    :param files: List of file dictionaries
    :return: Most recent file dictionary
    """
    logger.info('Getting most recent file.')
    files.sort(key=lambda f: datetime.strptime(f['createdTime'], '%Y-%m-%dT%H:%M:%S.%fZ'), reverse=True)
    most_recent_file = files[0]
    logger.info(f'Most recent file: {most_recent_file["name"], most_recent_file["createdTime"]}')
    return most_recent_file