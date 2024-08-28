from agm import AGM
from datetime import datetime

import pytz
from pandas.tseries.offsets import BDay

import requests as rq

Drive = AGM().Drive

def fetchReports():

  url = 'http://127.0.0.1:5000'

  # Fetch reports from Flex Query Service
  #response = rq.post(url + '/fetchReports', json={'queryIds':['732383', '734782', '742588']})
  response = rq.post(url + '/fetchFlexQueries', json={'queryIds':['732383']})
  print('\nGenerating reports. Please wait.\n')
  flex_queries = response.json()

  # Get relevant folder IDs
  etl_id = Drive.getSharedDriveInfo('ETL')['id']
  batch_folder_id = Drive.getFolderInfo(etl_id, 'batch')['id']

  # Upload files to batch folder
  Drive.uploadCSVFiles(flex_queries, batch_folder_id)
  return batch_folder_id

def renameFilesInBatch():
  
  # Get all files in batch
  batch_files = Drive.getFilesInFolder(batch_folder_id)
  print('Batch files: ', batch_files, '\n')
  input('Press Enter to continue. Check batch files first.')

  # Rename files
  # Add new name to each file
  for f in batch_files:
    match f['name']:
      case '742588':
        f['new_name'] = ('742588_' + yesterday_date + '.csv')
      case '734782':
          f['new_name'] = ('734782_' + yesterday_date + '.csv')
      case '732383':
          f['new_name'] = ('732383_' + first_date + '_' + yesterday_date + '.csv')
      case 'clients':
          f['new_name'] = ('clients ' + today_date + ' agmtech212' + '.xls')
      case 'tasks_for_subaccounts':
          f['new_name'] = ('tasks_for_subaccounts ' + today_date + ' agmtech212' + '.csv')
      case 'ContactListSummary':
          f['new_name'] = ('ContactListSummary ' + today_date + ' agmtech212' + '.csv')
      case _:
        f['new_name'] = f['name']
  batch_files = Drive.renameFiles(batch_files)
  return batch_files

def sortFilesToFolders():
  # Sort files into folders
  # Get Interactive Brokers Shared Drive ID
  ibkr_folder_info = Drive.getSharedDriveInfo('Interactive Brokers')
  # Get Queries folder ID
  queries_folder_info = Drive.getFolderInfo(ibkr_folder_info['id'], 'Queries')
  """"""
  # Get Parent Tasks For Sub Accounts folder ID
  parent_subaccounts_folder_info = Drive.getFolderInfo(queries_folder_info['id'], 'Tasks For Sub Accounts')
  # Get this year's Tasks For Sub Accounts folder ID
  subaccounts_folder_info = Drive.getFolderInfo(parent_subaccounts_folder_info['id'], f'tasks_for_sub_accounts_{current_year}')
  """"""
  # Get Parent Contact List Summary folder ID
  parent_contacts_folder_info = Drive.getFolderInfo(queries_folder_info['id'], 'Contact List Summary')
  # Get this year's Contact List Summary folder ID
  contacts_folder_info = Drive.getFolderInfo(parent_contacts_folder_info['id'], f'Contact List Summary {current_year}')
  """"""
  # Get Parent RTD folder ID
  parent_rtd_folder_info = Drive.getFolderInfo(queries_folder_info['id'], 'RTD')
  # Get this year's RTD folder ID
  rtd_folder_info = Drive.getFolderInfo(parent_rtd_folder_info['id'], f'RTD_{current_year}')
  """"""
  # Get Clients folder ID
  clients_folder_info = Drive.getFolderInfo(queries_folder_info['id'], 'Clients')
  """"""
  # Get Open Positions folder ID
  open_positions_folder_info = Drive.getFolderInfo(queries_folder_info['id'], 'Open Positions')
  # Get parent 742588 folder ID
  parent_742588_folder_info = Drive.getFolderInfo(open_positions_folder_info['id'], '742588')
  # Get this year's 742588 folder ID
  folder_742588_info = Drive.getFolderInfo(parent_742588_folder_info['id'], f'742588_{current_year}')
  """"""
  # Get NAV in Base folder ID
  nav_folder_info = Drive.getFolderInfo(queries_folder_info['id'], 'NAV in Base')
  # Get parent 734782 folder ID
  parent_734782_folder_info = Drive.getFolderInfo(nav_folder_info['id'], '734782')
  # Get this year's 734782 folder ID
  folder_734782_info = Drive.getFolderInfo(parent_734782_folder_info['id'], f'734782_{current_year}')
  """"""
  # Get Client Fees folder ID
  client_fees_folder_info = Drive.getFolderInfo(queries_folder_info['id'], 'Client Fees')
  # Get parent 734782 folder ID
  parent_732383_folder_info = Drive.getFolderInfo(client_fees_folder_info['id'], '732383')
  # Get this year's 734782 folder ID
  folder_732383_info = Drive.getFolderInfo(parent_732383_folder_info['id'], f'732383_{current_year}')
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
        new_parent_id = etl_id

    updated_file = Drive.moveFile(f, new_parent_id)
    print(f"File '{f['name']}' moved to new parent folder. {updated_file}")

# Get relevant folder IDs
etl_id = Drive.getSharedDriveInfo('ETL')['id']
batch_folder_id = Drive.getFolderInfo(etl_id, 'batch')['id']

# Get the current time in CST
cst = pytz.timezone('America/Costa_Rica')
cst_time = datetime.now(cst)
today_date = cst_time.strftime('%Y%m%d%H%M')
yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
first_date = cst_time.replace(day=1).strftime('%Y%m%d')
current_year = cst_time.year

fetchReports()
batch_files = renameFilesInBatch()
sortFilesToFolders()