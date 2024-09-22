import requests as rq
from pandas.tseries.offsets import BDay

from app.helpers.dates import getCurrentCST

from app.helpers.logger import logger
from app.helpers.response import Response

logger.info('Initializing Reporting Module')
url = 'http://127.0.0.1:5001'
logger.success('Initialized Reporting Module')

cst_time = getCurrentCST()

def generate():

    logger.info('Generating Reports')

    # Get relevant folder IDs
    response = rq.post(url + '/drive/get_shared_drive_info', json={'drive_name': 'ETL'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching ETL Shared Drive Info.')
    etl_id = response['content']['id']

    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': etl_id, 'folder_name': 'batch'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Batch Folder Info.')
    batch_folder_id = response['content']['id']

    response = rq.post(url + '/flex_query/fetch', json={'queryIds': ['732383', '734782', '742588']}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Flex Queries.')
    flex_queries = response['content']

    # Upload files to batch folder
    response = rq.post(url + '/drive/upload_csv_files', json={'files': flex_queries, 'parent_id': batch_folder_id}).json()
    if response['status'] == 'error':
        return Response.error(f'Error uploading CSV files.')

    response = renameFilesInBatch(batch_folder_id)
    if response['status'] == 'error':
        return Response.error(f'Error renaming files.')
    
    batch_files = response['content']

    logger.info('Reports successfully generated.')
    return Response.success(batch_files)

def uploadReportsToDatabase():

    logger.info('Uploading reports to database.')

    response = rq.post(url + '/flex_query/get_files_in_folder', json={'parent_id': '1a-t3vp5vSbs0eBEO4ZdbS_5n9Sqlo-DN'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in batch.')
    batch_files = response['content']

    # Sort reports into backup folders
    response = sortFilesToFolders(batch_files)
    if response['status'] == 'error':
        return Response.error(f'Error sorting files.')
    
    # Get Interactive Brokers Shared Drive ID
    response = rq.post(url + '/drive/get_shared_drive_info', json={'drive_name': 'Interactive Brokers'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Interactive Brokers Shared Drive Info.')
    ibkr_folder_info = response['content']

    # Get Queries folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': ibkr_folder_info['id'], 'folder_name': 'Queries'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Queries Folder Info.')
    queries_folder_info = response['content']

    # Get Clients folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'Clients'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Clients Folder Info.')
    clients_folder_info = response['content']

    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': clients_folder_info['id'], 'folder_name': 'Clients_IBKR'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Clients_IBKR Folder Info.')
    clients_ibkr_folder_info = response['content']

    response = rq.post(url + '/drive/get_files_in_folder', json={'parent_id': clients_ibkr_folder_info['id']}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Clients Folder Info.')
    client_files_info = response['content']

    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': clients_folder_info['id'], 'folder_name': '2024'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching 2024 Folder Info.')
    folder_2024_info = response['content']

    logger.info(f'Clients files: {client_files_info}')

    return Response.success('Reports uploaded to drive backup folders.')

def renameFilesInBatch(batch_folder_id):
  
    # Get the current time in CST
    today_date = cst_time.strftime('%Y%m%d%H%M')
    yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
    first_date = cst_time.replace(day=1).strftime('%Y%m%d')

    logger.info('Renaming files in batch folder.')

    # Get all files in batch
    response = rq.post(url + '/drive/get_files_in_folder', json={'parent_id': batch_folder_id}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching files in batch.')  
    batch_files = response['content']

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

    response = rq.post(url + '/drive/rename_files', json={'files': batch_files}).json()
    if response['status'] == 'error':
        return Response.error(f'Error renaming files.')

    batch_files = response['content']
    return Response.success(batch_files)

def sortFilesToFolders(batch_files):

    logger.info('Sorting files into backup folders.')
    current_year = cst_time.year

    # Get Interactive Brokers Shared Drive ID
    response = rq.post(url + '/drive/get_shared_drive_info', json={'drive_name': 'Interactive Brokers'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Interactive Brokers Shared Drive Info.')
    ibkr_folder_info = response['content']

    # Get Queries folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': ibkr_folder_info['id'], 'folder_name': 'Queries'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Queries Folder Info.')
    queries_folder_info = response['content']

    # Get Parent Tasks For Sub Accounts folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'Tasks For Sub Accounts'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Tasks For Sub Accounts Folder Info.')
    parent_subaccounts_folder_info = response['content']

    # Get this year's Tasks For Sub Accounts folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': parent_subaccounts_folder_info['id'], 'folder_name': f'tasks_for_sub_accounts_{current_year}'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Tasks For Sub Accounts Folder Info.')
    subaccounts_folder_info = response['content']

    # Get Parent Contact List Summary folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'Contact List Summary'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Contact List Summary Folder Info.')
    parent_contacts_folder_info = response['content']

    # Get this year's Contact List Summary folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': parent_contacts_folder_info['id'], 'folder_name': f'Contact List Summary {current_year}'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Contact List Summary Folder Info.')
    contacts_folder_info = response['content']

    # Get Parent RTD folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'RTD'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching RTD Folder Info.')
    parent_rtd_folder_info = response['content']

    # Get this year's RTD folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': parent_rtd_folder_info['id'], 'folder_name': f'RTD_{current_year}'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching RTD Folder Info.')
    rtd_folder_info = response['content']


    # Get Clients folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'Clients'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Clients Folder Info.')
    clients_folder_info = response['content']

    # Get Open Positions folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'Open Positions'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Open Positions Folder Info.')
    open_positions_folder_info = response['content']

    # Get parent 742588 folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': open_positions_folder_info['id'], 'folder_name': '742588'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching 742588 Folder Info.')
    parent_742588_folder_info = response['content']

    # Get this year's 742588 folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': parent_742588_folder_info['id'], 'folder_name': f'742588_{current_year}'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching 742588 Folder Info.')
    folder_742588_info = response['content']


    # Get NAV in Base folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'NAV in Base'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching NAV in Base Folder Info.')
    nav_folder_info = response['content']

    # Get parent 734782 folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': nav_folder_info['id'], 'folder_name': '734782'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching 734782 Folder Info.')
    parent_734782_folder_info = response['content']

    # Get this year's 734782 folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': parent_734782_folder_info['id'], 'folder_name': f'734782_{current_year}'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching 734782 Folder Info.')
    folder_734782_info = response['content']


    # Get Client Fees folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': queries_folder_info['id'], 'folder_name': 'Client Fees'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching Client Fees Folder Info.')
    client_fees_folder_info = response['content']

    # Get parent 734782 folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': client_fees_folder_info['id'], 'folder_name': '732383'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching 732383 Folder Info.')
    parent_732383_folder_info = response['content']

    # Get this year's 734782 folder ID
    response = rq.post(url + '/drive/get_folder_info', json={'parent_id': parent_732383_folder_info['id'], 'folder_name': f'732383_{current_year}'}).json()
    if response['status'] == 'error':
        return Response.error(f'Error fetching 732383 Folder Info.')
    folder_732383_info = response['content']


    # Print all folder information
    if False:
        print('IBKR folder info:', ibkr_folder_info)

        print('Queries folder info:', queries_folder_info)

        print('Parent subaccounts folder info:', parent_subaccounts_folder_info)
        print('Subaccounts folder info:', subaccounts_folder_info)

        print('Parent contacts folder info:', parent_contacts_folder_info)
        print('Contacts folder info:', contacts_folder_info)

        print('Parent RTD folder info:', parent_rtd_folder_info)
        print('RTD folder info:', rtd_folder_info)

        print('Clients folder info:', clients_folder_info)

        print('Open positions folder info:', open_positions_folder_info)
        print('Parent 742588 folder info:', parent_742588_folder_info)
        print('742588 folder info:', folder_742588_info)

        print('NAV folder info:', nav_folder_info)
        print('Parent 734782 folder info:', parent_734782_folder_info)
        print('734782 folder info:', folder_734782_info)

        print('Client fees folder info:', client_fees_folder_info)
        print('Parent 732383 folder info:', parent_732383_folder_info)
        print('732383 folder info:', folder_732383_info)

    # Move files to respective folder in backups
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
                new_parent_id = folder_742588_info['id']
            case name if '734782' in name:
                new_parent_id = folder_734782_info['id']
            case name if '732383' in name:
                new_parent_id = folder_732383_info['id']
            case _:
                new_parent_id = 'root'

        response = rq.post(url + '/drive/move_file', json={'file_id': f, 'new_parent_id': new_parent_id}).json()
        if response['status'] == 'error':
            return Response.error(f'Error moving file.')
        logger.info(f"File '{f['name']}' moved to new parent folder.")

    return Response.success('Files sorted into backup folders.')