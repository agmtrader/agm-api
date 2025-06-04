from pandas.tseries.offsets import BDay
from datetime import datetime
from src.utils.dates import getCurrentCST
from src.utils.logger import logger
import pandas as pd
from io import BytesIO
import base64
import yfinance as yf
import time
from src.utils.flex_query import fetchFlexQueries
from src.components.accounts import read_accounts
from src.utils.connectors.drive import GoogleDrive

logger.announcement('Initializing Reporting Service', type='info')
Drive = GoogleDrive()
cst_time = getCurrentCST()
logger.announcement('Initialized Reporting Service', type='success')

def get_clients_report():
    """
    Get the clients report.
    
    :return: Response object with clients report or error message
    """
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    resources = Drive.get_files_in_folder(resources_folder_id)

    # Filter by Clients file
    clients_file_ids = [file['id'] for file in resources if 'ibkr_clients.csv' in file['name']]
    if len(clients_file_ids) != 1:
        raise Exception('Error with clients file')
    clients_file_id = clients_file_ids[0]

    # Download nav file
    nav_file_ids = [file['id'] for file in resources if 'ibkr_nav_in_base.csv' in file['name']]
    if len(nav_file_ids) != 1:
        raise Exception('Error with nav file')
    nav_file_id = nav_file_ids[0]

    # Download clients file
    clients_df = pd.DataFrame(Drive.download_file(file_id=clients_file_id, parse=True))

    # Download nav file
    nav_df = pd.DataFrame(Drive.download_file(file_id=nav_file_id, parse=True))

    accounts = read_accounts()
    accounts_df = pd.DataFrame(accounts)

    # Add new columns to clients dataframe
    clients_df['AccountHolder'] = clients_df['First Name'] + ' ' + clients_df['Last Name']
    clients_df['NAV'] = clients_df['Account ID'].map(nav_df.set_index('ClientAccountID')['Total'])

    clients_df = clients_df.fillna('')
    nav_df = nav_df.fillna('')
    accounts_df = accounts_df.fillna('')

    return {'clients': clients_df.to_dict(orient='records'), 'accounts': accounts_df.to_dict(orient='records')}

def get_accrued_interest_report():

    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    resources = Drive.get_files_in_folder(resources_folder_id)

    # Filter by Open Positions file
    open_positions_ids = [file['id'] for file in resources if 'ibkr_open_positions_template.csv' in file['name']]
    if len(open_positions_ids) != 1:
        raise Exception('Error with open positions file')
    open_positions_id = open_positions_ids[0]

    # Download Open Positions file
    open_positions = Drive.download_file(file_id=open_positions_id, parse=True)
    open_positions_df = pd.DataFrame(open_positions)
    open_positions_df = open_positions_df.fillna('')

    return open_positions_df.to_dict(orient='records')

"""
Extracts reports from various sources and prepare them for processing.

This function performs the following steps:
1. Reset the batch folder
2. Fetch Flex Queries
3. Upload Flex Queries to the batch folder
4. Rename files in the batch folder
5. Sort files to respective backup folders
6. Return the list of processed files in the batch folder

TODO:
- Fetch and upload the other four sources:
    - Clients List
    - Contact List Summary
    - RTD
    - Tasks for Subaccounts

"""
def extract():
    logger.announcement('Generating Reports.', type='info')
    batch_folder_id = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'
    backups_folder_id = '1d9RShyGidP04XdnH87pUHsADghgOiWj3'

    # Fetch Flex Queries
    logger.announcement('Fetching Flex Queries.', type='info')
    flex_queries = fetchFlexQueries(['732383', '734782', '742588'])
    logger.announcement('Flex Queries fetched.', type='success')

    # Upload Flex Queries to batch folder
    logger.announcement('Uploading Flex Queries to batch folder.', type='info')
    for key, value in flex_queries.items():
        Drive.upload_file(file_name=key, mime_type='text/csv', file_data=value, parent_folder_id=batch_folder_id)
    logger.announcement('Flex Queries uploaded to batch folder.', type='success')
    time.sleep(2)

    # Rename files in batch folder
    logger.announcement('Renaming files in batch folder.', type='info')
    number_renamed = rename_files_in_batch(batch_folder_id)
    logger.announcement(f'{number_renamed} files renamed successfully.', type='success')
    
    # Sort files to respective backup folders
    logger.announcement('Sorting files to backup folders.', type='info')
    number_sorted = sort_files_to_folders(batch_folder_id, backups_folder_id)
    logger.announcement(f'{number_sorted} files sorted to backup folders.', type='success')

    logger.announcement('Reports successfully extracted.', type='success')
    return {'status': 'success', 'number_renamed': number_renamed, 'number_sorted': number_sorted}

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
        else:
            new_name = f['name']

        try:
            Drive.rename_file(file_id=f['id'], new_name=new_name)
            count += 1
        except Exception as e:
            raise Exception(f'Error renaming file: {e}')
        
    logger.announcement(f'{count} files renamed.', type='success')
    return count

def sort_files_to_folders(batch_folder_id, backups_folder_id):
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
        except Exception as e:
            raise Exception(f'Error moving file to destination: {e}')

    logger.announcement(f'{count} files moved to backup folders.', type='success')
    return count

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

    # Clear resources folder
    logger.announcement('Clearing resources folder.', type='info')
    Drive.clear_folder(folder_id=resources_folder_id)
    logger.announcement('Resources folder cleared.', type='success')

    # Process files in each backup folder
    logger.announcement('Processing files.', type='info')
    for report_type, config in report_configs.items():
        logger.announcement(f'Processing {report_type.capitalize()} file.', type='info')
        process_report(config, resources_folder_id)
    logger.announcement('Files processed.', type='success')

    # Process finance data
    logger.announcement('Fetching finance data.', type='info')
    get_finance_data(resources_folder_id)

    logger.announcement('Reports successfully transformed.', type='success')
    return {'status': 'success'}

"""
TRANSFORM HELPERS
"""
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
    files = Drive.get_files_in_folder(folder_id)

    if len(files) == 0:
        logger.error(f'No files found in backup folder.')
        raise Exception('No files found in backup folder.')

    # Get most recent file
    most_recent_file = get_most_recent_file(files)

    # Download file and read into dataframe
    f = Drive.download_file(file_id=most_recent_file['id'], parse=True)
    print(f)
    file_df = pd.DataFrame(f)
    file_df = file_df.fillna('')

    # Apply custom processing if specified
    if 'process_func' in config:
        file_df = config['process_func'](file_df)

    # Upload processed file to Resources folder
    file_dict = file_df.to_dict(orient='records')
    Drive.upload_file(file_name=output_filename, mime_type='text/csv', file_data=file_dict, parent_folder_id=resources_folder_id)
    return

"""
REPORT PROCESSING FUNCTIONS
"""
def process_rtd_template(df):
    """
    Process the RTD file.
    
    :param df: Input dataframe
    :return: Processed dataframe
    """
    # Upload the full file
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    full_dict = df.to_dict(orient='records')
    Drive.upload_file(file_name='ibkr_rtd.csv', mime_type='text/csv', file_data=full_dict, parent_folder_id=resources_folder_id)
    
    # Sort the dataframe in the same order as the excel template
    template_columns = [
        'Symbol',
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
        'Duration %  ',
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
        'Issuer Country '
    ]
    logger.info(f'{df}')

    df_rtd_template = df[template_columns].copy()
    df_rtd_template['Symbol'] = df_rtd_template['Symbol'].astype(str)
    df_rtd_template = df_rtd_template[df_rtd_template['Symbol'].str.contains('IBCID')]
    return df_rtd_template

# TODO IBKR CLIENTS -> Filename column
def process_clients_file(sheets_df):
    """
    Process the clients file by concatenating all sheets into a single dataframe.
    
    :param df: Input dataframe from Excel file
    :return: Processed dataframe
    """
    concatenated_df = pd.DataFrame()
    for sheet_name, sheet_data in sheets_df.items():
        sheet_df = pd.DataFrame(sheet_data)
        sheet_df['MasterAccount'] = sheet_name
        concatenated_df = pd.concat([concatenated_df, sheet_df])
    return concatenated_df

# TODO TEMPLATE SAVE TWICE: (template up to AX)/(template up to CL)
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

# TODO Filter columns
def get_finance_data(resources_folder_id):
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
    df = df.sort_index(ascending=False)
    df2 = df.iloc[[0,251,503,755,1007,1259]]

    # EXPORT DATASET
    filename =  'dataset_PX_5Y.csv'

    # Instead of writing to file, write to memory
    csv_buffer = BytesIO()
    df2.to_csv(csv_buffer, index=True)
    csv_buffer.seek(0)

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
        'output_filename': 'ibkr_rtd_template.csv',
        'process_func': process_rtd_template,
    },
    'client_fees': {
        'folder_id': '1OnSEo8B2VUF5u-VkhtzZVIzx6ABe_YB7',
        'output_filename': 'ibkr_client_fees.csv',
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