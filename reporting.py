from agm import AGM

Drive = AGM().Drive

# Get relevant folder IDs
etl_id = Drive.getSharedDriveInfo('ETL')['id']
batch_folder_id = Drive.getFolderInfo(etl_id, 'batch')['id']

# Use Google API to get all files inside batch folder
files = Drive.getFilesInFolder(batch_folder_id)
print(files)

# Rename files in batch folder
status = Drive.renameFiles(files)
print(status)

import os

# Get all files inside batch folder
path_batch = '/content/gdrive/Shareddrives/ETL/batch/'

batch_files = os.listdir(path_batch)
#print('Batch files: ', batch_files)

# Get ETL folder ID
"""
etl_folder_info = get_shared_drive_info('ETL')
#print(etl_folder_info)

# Get batch and backups folder ID
batch_folder_info = get_folder_info(etl_folder_info['id'], 'batch')
#print(batch_folder_info)

# Get all files inside batch folder
batch_files_info = []
for f in batch_files:
  file_info = get_file_info(batch_folder_info['id'], f)
  batch_files_info.append(file_info)

print('Files in batch: ', batch_files_info)

# Get IDs for each folder inside Queries

# Get current year to backup files to correct folder

current_year = datetime.date.today().year

# Get Interactive Brokers Shared Drive ID

ibkr_folder_info = get_shared_drive_info('Interactive Brokers')
#print(ibkr_folder_info)

# Get Queries folder ID

queries_folder_info = get_folder_info(ibkr_folder_info['id'], 'Queries')
#print(queries_folder_info)

# Get Parent Contact List Summary folder ID

parent_subaccounts_folder_info = get_folder_info(queries_folder_info['id'], 'Tasks For Sub Accounts')
#print(parent_subaccounts_folder_info)

# Get this year's Contact List Summary folder ID

subaccounts_folder_info = get_folder_info(parent_subaccounts_folder_info['id'], f'tasks_for_sub_accounts_{current_year}')
#print(subaccounts_folder_info)

# Get Parent Contact List Summary folder ID

parent_contacts_folder_info = get_folder_info(queries_folder_info['id'], 'Contact List Summary')
#print(parent_contacts_folder_info)

# Get this year's Contact List Summary folder ID

contacts_folder_info = get_folder_info(parent_contacts_folder_info['id'], f'Contact List Summary {current_year}')
#print(contacts_folder_info)

# Get Parent RTD folder ID

parent_rtd_folder_info = get_folder_info(queries_folder_info['id'], 'RTD')
#print(parent_rtd_folder_info)

# Get this year's RTD folder ID

rtd_folder_info = get_folder_info(parent_rtd_folder_info['id'], f'RTD_{current_year}')
#print(rtd_folder_info)

# Get Clients folder ID

clients_folder_info = get_folder_info(queries_folder_info['id'], 'Clients')
#print(clients_folder_info)

# Get Open Positions folder ID

open_positions_folder_info = get_folder_info(queries_folder_info['id'], 'Open Positions')
#print(open_positions_folder_info)

# Get this year's 742588 folder ID

parent_742588_folder_info = get_folder_info(open_positions_folder_info['id'], '742588')
#print(parent_742588_folder_info)

# Get this year's 742588 folder ID

folder_742588_info = get_folder_info(parent_742588_folder_info['id'], f'742588_{current_year}')
#print(folder_742588_info)

# Get NAV in Base folder ID

nav_folder_info = get_folder_info(queries_folder_info['id'], 'NAV in Base')
#print(nav_folder_info)

# Get this year's 734782 folder ID

parent_734782_folder_info = get_folder_info(nav_folder_info['id'], '734782')
#print(parent_734782_folder_info)

# Get this year's 734782 folder ID

folder_734782_info = get_folder_info(parent_734782_folder_info['id'], f'734782_{current_year}')
#print(folder_734782_info)

# Get Client Fees folder ID
client_fees_folder_info = get_folder_info(queries_folder_info['id'], 'Client Fees')
#print(client_fees_folder_info)

# Get this year's 734782 folder ID
parent_732383_folder_info = get_folder_info(client_fees_folder_info['id'], '732383')
#print(parent_732383_folder_info)

# Get this year's 734782 folder ID
folder_732383_info = get_folder_info(parent_732383_folder_info['id'], f'732383_{current_year}')
#print(folder_732383_info)

# Move files to respective folder in backups

# Get info from one file in batch

for batch_file in batch_files_info:

  f = (
      service.files()
      .get(
          supportsAllDrives=True,
          fileId = batch_file['id']
      )).execute()

  # Upload batch file contents to server for new file

  media = MediaFileUpload(path_batch + f['name'], mimetype=f['mimeType'])

  # Set new file's destination

  if 'clients' in f['name']:

    file_parent = clients_folder_info['id']

  elif 'ContactListSummary' in f['name']:

    file_parent = contacts_folder_info['id']

  elif 'tasks_for_subaccounts' in f['name']:

    file_parent = subaccounts_folder_info['id']

  elif 'RTD' in f['name']:

    file_parent = rtd_folder_info['id']

  elif '742588' in f['name']:

    file_parent = folder_742588_info['id']

  elif '734782' in f['name']:

    file_parent = folder_734782_info['id']

  elif '732383' in f['name']:

    file_parent = folder_732383_info['id']

  else:

    file_parent = etl_folder_info['id']

  # Create new file metadata with properties of original file and new destination

  file_metadata = {
    'name': f['name'],
    'parents': [file_parent],
    'mimeType': f['mimeType']
  }

  # Create the new file in its respective folder

  if True: # fix
    created_file = (
        service.files().create(
          supportsAllDrives=True,
          body=file_metadata,
          media_body=media,
          fields='id'
        )).execute()

    print('Created file in backups:', f['name'])

  else:

    print('File already exists in backups.')

    # Delete files from batch | fix permissions

body_value = {'trashed': True}

for batch_file in batch_files_info:

    response = (service.files().update(
      supportsAllDrives=True,
      fileId = batch_file['id'],
      body = body_value
    )).execute()

print('Cleared batch.')
"""