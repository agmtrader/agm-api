from pandas.tseries.offsets import BDay

from datetime import datetime

from app.helpers.dates import getCurrentCST

from app.helpers.logger import logger
from app.helpers.response import Response
import pandas as pd
from io import BytesIO
import base64
import os

logger.info('Initializing Reporting Module')

url = os.getenv('API_URL')
logger.success('Initialized Reporting Module')

cst_time = getCurrentCST()

import requests
def access_api(endpoint, method='GET', data=None):
    print(url + endpoint, data, method)
    auth = requests.post(url + '/login', json={
        'username': 'admin',
        'password': 'password'
    })
    response = requests.request(method, url + endpoint, json=data, headers={
        'Authorization': f'Bearer {auth.json()["access_token"]}'
    }).json()
    print(response)
    return response


def extract():

    logger.info('Generating Reports')
    batch_folder_id = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'

    # Reset batch folder
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': batch_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching Flex Queries.')
    batch_files = response['content']
    if len(batch_files) > 0:
        response = access_api('/drive/delete_files', method='POST', data={'file_ids': [f['id'] for f in batch_files]})
        if response['status'] == 'error':
            return Response.error(f'Error deleting files in batch folder.')

    # Fetch Flex Queries
    response = access_api('/flex_query/fetch', method='POST', data={'queryIds': ['732383', '734782', '742588']})
    if response['status'] == 'error':
        return Response.error(f'Error fetching Flex Queries.')
    flex_queries = response['content']

    # Upload flex queries to batch folder
    # TODO: Fix upload_csv_files route
    response = access_api('/drive/upload_csv_files', method='POST', data={'files': flex_queries, 'parent_id': batch_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV files.')

    # Rename files in batch folder
    response = renameFilesInBatch(batch_folder_id)
    if response['status'] == 'error':
        return Response.error(f'Error renaming files.')    
    batch_files = response['content']

    # Sort files to respective backup folders
    response = sortFilesToFolders(batch_files)
    if response['status'] == 'error':
        return Response.error(f'Error sorting files to backup folders.')

    logger.info('Reports successfully generated.')
    return Response.success(batch_files)

def renameFilesInBatch(batch_folder_id):
  
    # Get the current time in CST
    today_date = cst_time.strftime('%Y%m%d%H%M')
    yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
    first_date = cst_time.replace(day=1).strftime('%Y%m%d')

    logger.info('Renaming files in batch folder.')

    # Get all files in batch
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': batch_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in batch.')  
    batch_files = response['content']

    # Rename files
    for f in batch_files:

        if ('742588' in f['name']):
            f['new_name'] = ('742588_' + yesterday_date + '.csv')
        elif ('734782' in f['name']):
            f['new_name'] = ('734782_' + yesterday_date + '.csv')
        elif ('732383' in f['name']):
            f['new_name'] = ('732383_' + first_date + '_' + yesterday_date + '.csv')
        elif ('clients' in f['name']):
            f['new_name'] = ('clients ' + today_date + ' agmtech212' + '.xls')
        elif ('tasks_for_subaccounts' in f['name']):
            f['new_name'] = ('tasks_for_subaccounts ' + today_date + ' agmtech212' + '.csv')
        elif ('ContactListSummary' in f['name']):
            f['new_name'] = ('ContactListSummary ' + today_date + ' agmtech212' + '.csv')
        else:
            f['new_name'] = f['name']
    response = access_api('/drive/rename_files', method='POST', data={'files': batch_files})
    if response['status'] == 'error':
        return Response.error(f'Error renaming files.')
    batch_files = response['content']
    return Response.success(batch_files)

def sortFilesToFolders(batch_files):

    logger.info('Sorting files into backup folders.')

    backups_folder_id = '1d9RShyGidP04XdnH87pUHsADghgOiWj3'
    folder_names = ['TasksForSubaccounts', 'ContactListSummary', 'RTD', 'Clients', '742588', '734782', '732383']
    folder_info = {}

    # Get info for all backup folders
    for folder_name in folder_names:
        response = access_api('/drive/get_folder_info', method='POST', data={'parent_id': backups_folder_id, 'folder_name': folder_name})
        if response['status'] == 'error':
            return Response.error(f'Error fetching {folder_name} Folder Info.')
        folder_info[folder_name] = response['content']
    subaccounts_folder_info = folder_info['TasksForSubaccounts']
    contacts_folder_info = folder_info['ContactListSummary']
    rtd_folder_info = folder_info['RTD']
    clients_folder_info = folder_info['Clients']
    open_positions_folder_info = folder_info['742588']
    nav_folder_info = folder_info['734782']
    client_fees_folder_info = folder_info['732383']

    # Get all files in batch
    batch_folder_id = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': batch_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in batch.')
    batch_files = response['content']

    # Find destination for each file
    for f in batch_files:
        
        # Set new file's destination
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
        logger.info(f"File '{f['name']}' moved to new parent folder.")

    return Response.success('Files sorted into backup folders.')



def transform():
    logger.info('Transforming reports.')
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'

    # Reset resources folder
    logger.info(f'Resetting resources folder.')
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in resources folder.')
    resources_files = response['content']
    if len(resources_files) > 0:
        logger.info(f'Clearing resources folder.')
        response = access_api('/drive/delete_files', method='POST', data={'file_ids': [f['id'] for f in resources_files]})
        if response[0]['status'] == 'error':
            return Response.error(f'Error deleting files in resources folder.')
    logger.success(f'Resources folder reset.')

    # Process files
    response = processClients()
    if response['status'] == 'error':
        return Response.error(f'Error processing clients file.')

    response = processOpenPositions()
    if response['status'] == 'error':
        return Response.error(f'Error processing open positions file.')
    
    response = processNav()
    if response['status'] == 'error':
        return Response.error(f'Error processing NAV file.')
    
    response = processClientFees()
    if response['status'] == 'error':
        return Response.error(f'Error processing client fees file.')
    
    # Get all files in resources folder to return
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in resources folder.')
    resources_files = response['content']
    
    return Response.success(resources_files)

def processClients():

    logger.info('Processing Clients reports.')

    # Process clients file
    clients_folder_id = '1FNcbWNptK-A5IhmLws-R2Htl85OSFrIn'
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': clients_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in clients folder.')
    clients_files = response['content']

    # Get most recent file
    clients_files.sort(key=lambda f: datetime.strptime(f['name'].split(' ')[1], '%Y%m%d%H%M'))
    most_recent_file = clients_files[-1]

    # Download file and read into dataframe
    response = access_api('/drive/download_file', method='POST', data={'file_id': most_recent_file['id']})
    client_data = BytesIO(response)
    sheets_dict = pd.read_excel(client_data, sheet_name=None)
    clients_df = pd.concat(sheets_dict.values(), ignore_index=True)

    # Prepare file for upload
    csv_buffer = BytesIO()
    clients_df.to_csv(csv_buffer, index=False)
    csv_base64 = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')

    # Upload file to Resources
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    response = access_api('/drive/upload_file', method='POST', data={'raw_file': csv_base64, 'file_name': 'ibkr_clients.csv', 'mime_type': 'text/csv', 'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV file.')

    return Response.success('Clients file processed.')

# 742588
def processOpenPositions():

    logger.info('Processing Open Positions reports.')
    open_positions_folder_id = '1JL4__mr1XgOtnesYihHo-netWKMIGMet'
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': open_positions_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in open positions folder.')
    open_positions_files = response['content']

    # Get most recent file
    open_positions_files.sort(key=lambda f: datetime.strptime(f['name'].split('_')[1].split('.')[0], '%Y%m%d'))
    most_recent_file = open_positions_files[-1]

    # Download file and read into dataframe
    response = access_api('/drive/download_file', method='POST', data={'file_id': most_recent_file['id']})
    open_positions_data = BytesIO(response)
    open_positions_df = pd.read_csv(open_positions_data)

    # Prepare file for upload
    csv_buffer = BytesIO()
    open_positions_df.to_csv(csv_buffer, index=False)
    csv_base64 = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')

    # Upload file to Resources
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    response = access_api('/drive/upload_file', method='POST', data={'raw_file': csv_base64, 'file_name': 'ibkr_open_positions_all.csv', 'mime_type': 'text/csv', 'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV file.')
    
    # Generate template
    open_positions_df = open_positions_df[(open_positions_df['AssetClass'] == 'BOND') & (open_positions_df['LevelOfDetail'] == 'LOT')]
    columns_order = ['ClientAccountID','AccountAlias','Model','CurrencyPrimary','FXRateToBase','AssetClass','Symbol','Description',
        'Conid','SecurityID','SecurityIDType','CUSIP','ISIN','ListingExchange','UnderlyingConid','UnderlyingSymbol',
        'UnderlyingSecurityID','UnderlyingListingExchange','Issuer','Multiplier','Strike','Expiry','Put/Call','PrincipalAdjustFactor',
        'ReportDate','Quantity','MarkPrice','PositionValue','PositionValueInBase','OpenPrice','CostBasisPrice','CostBasisMoney',
        'PercentOfNAV','FifoPnlUnrealized','UnrealizedCapitalGainsPnl','UnrealizedFxPnl','Side','LevelOfDetail','OpenDateTime',
        'HoldingPeriodDateTime','Code','OriginatingOrderID','OriginatingTransactionID','AccruedInterest','VestingDate',
        'SerialNumber','DeliveryType','CommodityType','Fineness','Weight'
    ]
    open_positions_df = open_positions_df[columns_order]

    response = access_api('/drive/upload_file', method='POST', data={'raw_file': csv_base64, 'file_name': 'ibkr_open_positions_lot_(template).csv', 'mime_type': 'text/csv', 'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV file.')

    return Response.success('Open positions file processed.')

# 734782
def processNav():
    logger.info('Processing NAV reports.')
    nav_folder_id = '1WgYA-Q9mnPYrbbLfYLuJZwUIWBYjiD4c'
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': nav_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in NAV folder.')
    nav_files = response['content']

    # Get most recent file
    nav_files.sort(key=lambda f: datetime.strptime(f['name'].split('_')[1].split('.')[0], '%Y%m%d'))
    most_recent_file = nav_files[-1]

    # Download file and read into dataframe
    response = access_api('/drive/download_file', method='POST', data={'file_id': most_recent_file['id']})
    nav_data = BytesIO(response)
    nav_df = pd.read_csv(nav_data)

    # Prepare file for upload
    csv_buffer = BytesIO()
    nav_df.to_csv(csv_buffer, index=False)
    csv_base64 = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')

    # Upload file to Resources
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    response = access_api('/drive/upload_file', method='POST', data={'raw_file': csv_base64, 'file_name': 'ibkr_nav_in_base.csv', 'mime_type': 'text/csv', 'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV file.')
    
    return Response.success('NAV file processed.')

# 732383
def processClientFees():
    logger.info('Processing Client Fees reports.')
    client_fees_folder_id = '1OnSEo8B2VUF5u-VkhtzZVIzx6ABe_YB7'
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': client_fees_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in client fees folder.')
    client_fees_files = response['content']

    # Get most recent file
    client_fees_files.sort(key=lambda f: datetime.strptime(f['name'].split('_')[2].split('.')[0], '%Y%m%d'))
    most_recent_file = client_fees_files[-1]

    # Download file and read into dataframe
    response = access_api('/drive/download_file', method='POST', data={'file_id': most_recent_file['id']})
    client_fees_data = BytesIO(response)
    client_fees_df = pd.read_csv(client_fees_data)

    # Prepare file for upload
    csv_buffer = BytesIO()
    client_fees_df.to_csv(csv_buffer, index=False)
    csv_base64 = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')

    # Upload file to Resources
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    response = access_api('/drive/upload_file', method='POST', data={'raw_file': csv_base64, 'file_name': 'ibkr_client_fees.csv', 'mime_type': 'text/csv', 'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV file.')
    
    return Response.success('Client fees file processed.')

def processRTD():
    logger.info('Processing resources.')
    rtd_folder_id = '12L3NKflYtMiisnZOpU9aa1syx2ZJA6JC'
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': rtd_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in RTD folder.')
    rtd_files = response['content']

    # Get most recent file
    rtd_files.sort(key=lambda f: datetime.strptime(f['name'].split(' ')[1].split('.')[0], '%Y%m%d%H%M'))
    most_recent_file = rtd_files[-1]

    # Download file and read into dataframe
    response = access_api('/drive/download_file', method='POST', data={'file_id': most_recent_file['id']})
    rtd_data = BytesIO(response)
    rtd_df = pd.read_csv(rtd_data)
    
    # Prepare file for upload
    csv_buffer = BytesIO()
    rtd_df.to_csv(csv_buffer, index=False)
    csv_base64 = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')

    # Upload file to Resources
    resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
    response = access_api('/drive/upload_file', method='POST', data={'raw_file': csv_base64, 'file_name': 'ibkr_rtd.csv', 'mime_type': 'text/csv', 'parent_id': resources_folder_id})
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV file.')
    
    return Response.success('Resources successfully transformed.')




def load():

    logger.info('Uploading reports to database.')
    response = access_api('/drive/get_files_in_folder', method='POST', data={'parent_id': '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'})

    if response['status'] == 'error':
        return Response.error(f'Error fetching files in resources folder.')
    
    resources_files = response['content']
    for f in resources_files:
        response = access_api('/drive/download_file', method='POST', data={'file_id': f['id']})
        data = BytesIO(response)
        file_df = pd.read_csv(data)
        file_dict = file_df.to_dict(orient='records')

        response = access_api('/database/clear_collection', method='POST', data={'path': 'db/reporting'})
        if response['status'] == 'error':
            return Response.error(f'Error clearing collection.')

        # Upload file to database
        for col in file_dict:
            response = access_api('/database/create', method='POST', data={'data': col, 'path': 'db/reporting', 'id': f['name'].split('.')[0]})
            if response['status'] == 'error':
                return Response.error(f'Error uploading file to database.')
        
    logger.success('Reports successfully uploaded to database.')
    return Response.success('Reports successfully uploaded to database.')
