from pandas.tseries.offsets import BDay

from datetime import datetime

from src.utils.dates import getCurrentCST
from src.utils.logger import logger
from src.utils.response import Response
from src.utils.api import access_api  

import pandas as pd
from io import BytesIO
import base64
import yfinance as yf

logger.announcement('Initializing Reporting Module', type='info')
logger.announcement('Initialized Reporting Module', type='success')
cst_time = getCurrentCST()

"""
Extracts reports from various sources and prepare them for processing.

This function performs the following steps:
1. Reset the batch folder
2. Fetch Flex Queries
3. Upload Flex Queries to the batch folder
4. Rename files in the batch folder
5. Sort files to respective backup folders
6. Return the list of processed files in the batch folder
"""
def extract():
    logger.announcement('Generating Reports.', type='info')
    batch_folder_id = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'
    backups_folder_id = '1d9RShyGidP04XdnH87pUHsADghgOiWj3'

    # Fetch Flex Queries
    logger.announcement('Fetching Flex Queries.', type='info')
    response = access_api('/flex_query/fetch', method='POST', data={'queryIds': ['732383', '734782', '742588',]})
    if response['status'] == 'error':
        return Response.error(f'Error fetching Flex Queries.')
    flex_queries = response['content']
    logger.announcement('Flex Queries fetched.', type='success')

    # Upload Flex Queries to batch folder
    logger.announcement('Uploading Flex Queries to batch folder.', type='info')
    for key, value in flex_queries.items():
        response = access_api('/drive/upload_file', method='POST', data={
            'file_name': key, 
            'mime_type': 'text/csv', 
            'file': value, 
            'parent_folder_id': batch_folder_id
        })
        if response['status'] == 'error':
            return Response.error(f'Error uploading files.')
    logger.announcement('Flex Queries uploaded to batch folder.', type='success')

    # Rename files in batch folder
    logger.announcement('Renaming files in batch folder.', type='info')
    response = rename_files_in_batch(batch_folder_id)
    if response['status'] == 'error':
        return Response.error(f'Error renaming files.')    
    logger.announcement('Files renamed.', type='success')
    
    # Sort files to respective backup folders
    logger.announcement('Sorting files to backup folders.', type='info')
    response = sort_files_to_folders(batch_folder_id, backups_folder_id)
    if response['status'] == 'error':
        return Response.error(f'Error sorting files to backup folders.')
    logger.announcement('Files sorted to backup folders.', type='success')

    logger.announcement('Reports successfully extracted.', type='success')
    return Response.success('Reports successfully extracted and sent to backup folders.')

"""
Transform the extracted reports for further processing.

This function performs the following steps:
1. Reset the resources folder
2. Process each report according to its configuration (or default processing) and store in resources folder
3. Return the list of processed files in the resources folder
"""
def transform():
    logger.announcement('Transforming reports.', type='info')
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'

    # Reset resources folder
    logger.announcement('Resetting resources folder.', type='info')
    response = access_api('/drive/reset_folder', method='POST', data={'folder_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error resetting resources folder.')
    logger.announcement('Resources folder reset.', type='success')

    # Process files in each backup folder
    logger.announcement('Processing files.', type='info')
    for report_type, config in report_configs.items():
        logger.announcement(f'Processing {report_type.capitalize()} file.', type='info')
        response = process_report(config, resources_folder_id)
        if response['status'] == 'error':
            logger.warning(f'Error processing {report_type.capitalize()} file, not added to resources folder.')
        else:
            logger.announcement(f'{report_type.capitalize()} file processed.', type='success')
    logger.announcement('Files processed.', type='success')

    # Process finance data
    logger.announcement('Fetching finance data.', type='info')
    response = get_finance_data(resources_folder_id)
    if response['status'] == 'error':
        return Response.error(f'Error fetching finance data.')

    # Get all files in resources folder to return
    """
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': resources_folder_id})
    if response['status'] == 'error':
        logger.error(f'Error fetching files in resources folder.')
        return Response.error(f'Error fetching files in resources folder.')
    resources_files = response['content']
    """
    
    logger.announcement('Reports successfully transformed.', type='success')
    return Response.success('Reports successfully transformed.')

"""
EXTRACT HELPERS
"""
def rename_files_in_batch(batch_folder_id):
    """
    Rename files in the batch folder based on specific naming conventions.
    
    :param batch_folder_id: ID of the batch folder
    :return: Response object with updated batch files or error message
    """
    # Get the current time in CST
    today_date = cst_time.strftime('%Y%m%d%H%M')
    yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
    first_date = cst_time.replace(day=1).strftime('%Y%m%d')

    # Get all files in batch
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': batch_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in batch.')  
    batch_files = response['content']

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
        else:
            new_name = f['name']

        response = access_api('/drive/rename_file', method='POST', data={'fileId': f['id'], 'newName': new_name})
        if response['status'] == 'error':
            return Response.error(f'Error renaming file.')
        
    # Get updated list of files in batch folder
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': batch_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in batch.')  
    batch_files = response['content'] 
    return Response.success('Files renamed in batch folder.')

def sort_files_to_folders(batch_folder_id, backups_folder_id):
    """
    Sort files from the batch folder into their respective backup folders.
    
    :param batch_files: List of files in the batch folder
    :return: Response object with success message or error
    """
    folder_names = ['TasksForSubaccounts', 'ContactListSummary', 'RTD', 'Clients', '742588', '734782', '732383']
    folder_info = {}

    # Get info for all backup folders
    for folder_name in folder_names:
        response = access_api('/drive/get_folder_info', method='POST', data={'parent_id': backups_folder_id, 'folder_name': folder_name})
        if response['status'] == 'error':
            return Response.error(f'Error fetching {folder_name} Folder Info.')
        folder_info[folder_name] = response['content']

    # Assign folder info to variables
    subaccounts_folder_info = folder_info['TasksForSubaccounts']
    contacts_folder_info = folder_info['ContactListSummary']
    rtd_folder_info = folder_info['RTD']
    clients_folder_info = folder_info['Clients']
    open_positions_folder_info = folder_info['742588']
    nav_folder_info = folder_info['734782']
    client_fees_folder_info = folder_info['732383']

    # Get all files in batch
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': batch_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in batch.')
    batch_files = response['content']

    # Sort files to their respective folders
    for f in batch_files:
        # Determine the destination folder based on file name
        match f['name']:
            case name if 'clients' in name:
                new_parent_id = clients_folder_info['id']
            case name if 'ContactListSummary' in name:
                new_parent_id = contacts_folder_info['id']
            case name if 'tasks_for_subaccounts' in name:
                new_parent_id = subaccounts_folder_info['id']
            case name if 'RTD' in name:
                new_parent_id = rtd_folder_info['id']
            case name if '742588' in name:
                new_parent_id = open_positions_folder_info['id']
            case name if '734782' in name:
                new_parent_id = nav_folder_info['id']
            case name if '732383' in name:
                new_parent_id = client_fees_folder_info['id']
            case _:
                new_parent_id = 'root'

        # Move file to destination
        response = access_api('/drive/move_file', method='POST', data={'file': f, 'new_parent_id': new_parent_id})
        if response['status'] == 'error':
            return Response.error(f'Error moving file.')

    return Response.success('Files sorted into backup folders.')

"""
TRANSFORM HELPERS
"""
def get_finance_data(resources_folder_id):
    """
    Get finance data from the finance folder.
    
    :return: Response object with finance data or error message
    """

    proposals_equity_list_id = '1AqpIE7LRV40J-Aew5fA-P6gEfji3Yb-Rp5DohI9BQFY'
    
    response = access_api('/drive/export_file', method='POST', data={
        'file_id': proposals_equity_list_id, 
        'mime_type': 'text/csv'
    })
    print(response)

    try:
        # Convert binary response to DataFrame
        data = pd.read_csv(BytesIO(response))
    except:
        logger.error(f'Error converting response to DataFrame.')
        return Response.error(f'Error processing file.')

    # Rest of your code using 'data' DataFrame instead of previous undefined 'data' variable
    ticker_list = data['Ticker'].tolist()
    ticker_list

    TICKERS = ticker_list

    df = yf.download(tickers= TICKERS, period= 'max', interval= '1d')
    df = df.sort_index(ascending=False)
    df2 = df.iloc[[0,251,503,755,1007,1259]]

    # EXPORT DATASET
    filename =  'dataset_PX_5Y.csv'

    # Instead of writing to file, write to memory
    csv_buffer = BytesIO()
    df2.to_csv(csv_buffer, index=True)
    csv_buffer.seek(0)

    # Convert bytes to base64 string for JSON serialization
    csv_base64 = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')

    # Upload the CSV data from memory
    response = access_api('/drive/upload_file', method='POST', data={
        'file_name': filename,
        'mime_type': 'text/csv',
        'file': csv_base64,
        'parent_folder_id': resources_folder_id
    })

    if response['status'] == 'error':
        logger.error(f'Error uploading finance data CSV file.')
        return Response.error('Error uploading finance data.')
    return Response.success('Finance data uploaded.')

def process_report(config, resources_folder_id):
    """
    Process a single report according to its configuration.
    
    :param config: Dictionary containing report configuration
    :param resources_folder_id: ID of the resources folder
    :return: Response object with success message or error
    """
    folder_id = config['folder_id']
    output_filename = config['output_filename']
    
    # Get files in the report's backup folder
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': folder_id})
    if response['status'] == 'error':
        logger.error(f'Error fetching files in folder.')
        return Response.error(f'Error fetching files in folder.')
    files = response['content']

    if len(files) == 0:
        logger.error(f'No files found in backup folder.')
        return Response.error(f'No files found in backup folder.')

    # Get most recent file
    most_recent_file = get_most_recent_file(files)

    # Download file and read into dataframe
    response = access_api('/drive/download_file', method='POST', data={'file_id': most_recent_file['id'], 'mime_type': most_recent_file['mimeType']})
    try:
        file_data = BytesIO(response)
    except:
        return Response.error(f'Error downloading file.')
    
    if most_recent_file['mimeType'] == 'text/csv':
        df = pd.read_csv(file_data)
        df = df.fillna('')
    elif most_recent_file['mimeType'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or most_recent_file['mimeType'] == 'application/vnd.ms-excel':
        df = pd.read_excel(file_data, sheet_name=None)
    else:
        logger.error(f'Unsupported file type: {most_recent_file["mimeType"]}')
        return Response.error(f'Unsupported file type.')
    
    # Apply custom processing if specified
    if 'process_func' in config:
        df = config['process_func'](df)

    df = df.fillna('')
    
    file_dict = df.to_dict(orient='records')

    # Upload processed file to Resources folder
    response = access_api('/drive/upload_file', method='POST', data={
        'file_name': output_filename,
        'mime_type': 'text/csv',
        'file': file_dict,
        'parent_folder_id': resources_folder_id
    })
    print(response)
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV file.')

    return Response.success(f'File processed successfully.')

def process_clients_file(df):
    """
    Process the clients file by concatenating all sheets into a single dataframe.
    
    :param df: Input dataframe from Excel file
    :return: Processed dataframe
    """
    return pd.concat(df.values(), ignore_index=True)  # Concatenate all sheets into a single DataFrame

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
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    full_dict = df.to_dict(orient='records')
    response = access_api('/drive/upload_file', method='POST', data={
        'file_name': 'ibkr_open_positions_all.csv',
        'mime_type': 'text/csv',
        'file': full_dict,
        'parent_folder_id': resources_folder_id
    })
    if response['status'] == 'error':
        logger.error(f'Error uploading full open positions CSV file.')
    
    # Generate template
    df = df[(df['AssetClass'] == 'BOND') & (df['LevelOfDetail'] == 'LOT')]
    columns_order = ['ClientAccountID','AccountAlias','Model','CurrencyPrimary','FXRateToBase','AssetClass','Symbol','Description',
        'Conid','SecurityID','SecurityIDType','CUSIP','ISIN','ListingExchange','UnderlyingConid','UnderlyingSymbol',
        'UnderlyingSecurityID','UnderlyingListingExchange','Issuer','Multiplier','Strike','Expiry','Put/Call','PrincipalAdjustFactor',
        'ReportDate','Quantity','MarkPrice','PositionValue','PositionValueInBase','OpenPrice','CostBasisPrice','CostBasisMoney',
        'PercentOfNAV','FifoPnlUnrealized','UnrealizedCapitalGainsPnl','UnrealizedFxPnl','Side','LevelOfDetail','OpenDateTime',
        'HoldingPeriodDateTime','Code','OriginatingOrderID','OriginatingTransactionID','AccruedInterest','VestingDate',
        'SerialNumber','DeliveryType','CommodityType','Fineness','Weight'
    ]
    return df[columns_order]

def process_rtd_template(df):
    """
    Process the RTD file.
    
    :param df: Input dataframe
    :return: Processed dataframe
    """
    # Upload the full file
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    full_dict = df.to_dict(orient='records')
    response = access_api('/drive/upload_file', method='POST', data={
        'file_name': 'ibkr_rtd_all.csv',
        'mime_type': 'text/csv',
        'file': full_dict,
        'parent_folder_id': resources_folder_id
    })
    if response['status'] == 'error':
        logger.error(f'Error uploading full RTD CSV file.')
    
    # Sort the dataframe in the same order as the excel template
    df_rtd_template = df[['Symbol',
                            'Company Name',
                            'Financial Instrument',
                            'Position',
                            'Avg Price',
                            'Bid Size',
                            'Bid',
                            'Daily P&L',
                            'Ask',
                            'Ask Size',
                            'Ask Yield',
                            'Duration %  ', # The downloaded CSV has 2 spaces after the % symbol
                            'Sector',
                            'Industry',
                            'Maturity',
                            'Next Option Date',
                            'Coupon',
                            'Change',
                            'Change %',
                            'Last',
                            'Ticker Action',
                            'Bid Yield',
                            'Ratings',
                            'Payment Frequency',
                            'Issuer Country ' # The downloaded CSV has 1 space after the y
    ]]
    df_rtd_template['Symbol'] = df_rtd_template['Symbol'].astype(str)
    df_rtd_template = df_rtd_template[df_rtd_template['Symbol'].str.contains('IBCID')]
    return df

report_configs = {
    'clients': {
        'folder_id': '1FNcbWNptK-A5IhmLws-R2Htl85OSFrIn',
        'output_filename': 'ibkr_clients.csv',
        'process_func': process_clients_file,
    },
    'open_positions': {
        'folder_id': '1JL4__mr1XgOtnesYihHo-netWKMIGMet',
        'output_filename': 'ibkr_open_positions_template.csv',
        'process_func': process_open_positions_template,
    },
    'nav': {
        'folder_id': '1WgYA-Q9mnPYrbbLfYLuJZwUIWBYjiD4c',
        'output_filename': 'ibkr_nav_in_base.csv',
    },
    'client_fees': {
        'folder_id': '1OnSEo8B2VUF5u-VkhtzZVIzx6ABe_YB7',
        'output_filename': 'ibkr_client_fees.csv',
    },
    'rtd': {
        'folder_id': '12L3NKflYtMiisnZOpU9aa1syx2ZJA6JC',
        'output_filename': 'ibkr_rtd_template.csv',
        'process_func': process_rtd_template,
    },
}

"""
HELPER FUNCTIONS
"""

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