from datetime import datetime
import pytz

from pandas.tseries.offsets import BDay

from agm import AGM

cst = pytz.timezone('America/Costa_Rica')

# Get the current time in CST
cst_time = datetime.now(cst)

today_date = cst_time.strftime('%Y%m%d%H%M')
yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
first_date = cst_time.replace(day=1).strftime('%Y%m%d')

Drive = AGM().Drive

# Get relevant folder IDs
etl_id = Drive.getSharedDriveInfo('ETL')['id']
batch_folder_id = Drive.getFolderInfo(etl_id, 'batch')['id']

# Use Google API to get all files inside batch folder
files = Drive.getFilesInFolder(batch_folder_id)
#files = [{'id': file['id']} for file in files]

# Loop through each file
for f in files:
  print(f)
  # Rename directly using Google API

"""
for batch_file in batch_files:

  renamed_file = ''

  if '742588' in batch_file:

    renamed_file = ('742588_' + yesterday_date + '.csv')

  elif '734782' in batch_file:

    renamed_file = ('734782_' + yesterday_date + '.csv')

  elif '732383' in batch_file:

    renamed_file = ('732383_' + first_date + '_' + yesterday_date + '.csv')

  elif 'clients' in batch_file:

    # Check if file sheet name contains I285407 (Panama ID)

    if (pd.ExcelFile(path_batch + batch_file).sheet_names[0] == 'I285407'):
      renamed_file = ('clients ' + today_date + ' herca757' + '.xls')
    else:
      renamed_file = ('clients ' + today_date + ' agmtech212' + '.xls')

  elif 'tasks_for_subaccounts' in batch_file:

    # Check if file contains account with Account Number F10740574 (BVI)

    df1 = pd.read_csv(path_batch + batch_file)

    if (len(df1[df1['IB Account ID'] == 'F10740574']) == 0):
      renamed_file = ('tasks_for_subaccounts ' + today_date + ' herca757' + '.csv')
    else:
      renamed_file = ('tasks_for_subaccounts ' + today_date + ' agmtech212' + '.csv')

  elif 'ContactListSummary' in batch_file:

    # Check if file contains Andres Aguilar Account
    # Find new fix

    df1 = pd.read_csv(path_batch + batch_file)

    if (len(df1[df1['Account Number'] == 'U13926601']) != 0):
      renamed_file = ('ContactListSummary ' + today_date + ' agmtech212' + '.csv')
    else:
      renamed_file = ('ContactListSummary ' + today_date + ' herca757' + '.csv')

  # RTD
  else:
    renamed_file = batch_file

  renamed_files.append(renamed_file)
  os.rename(path_batch + batch_file, path_batch + renamed_file)

print('Original files: ', batch_files)
print('Renamed files: ', renamed_files)

import os
import shutil

from datetime import datetime
import pytz

from pandas.tseries.offsets import BDay
import pandas as pd

import gspread
from google.colab import drive
from google.auth import default

# Mount GoogleDrive
#from google.colab import drive
#drive.mount('/content/gdrive')

# Authorize Google Sheets
creds, _ = default()
gs = gspread.authorize(creds)

cst = pytz.timezone('America/Costa_Rica')

# Get the current time in CST
cst_time = datetime.now(cst)

today_date = cst_time.strftime('%Y%m%d%H%M')
yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
first_date = cst_time.replace(day=1).strftime('%Y%m%d')

path_batch = '/content/gdrive/Shareddrives/ETL/batch/'
batch_files = os.listdir(path_batch)
renamed_files = []

df1 = pd.DataFrame()

for batch_file in batch_files:

  renamed_file = ''

  if '742588' in batch_file:

    renamed_file = ('742588_' + yesterday_date + '.csv')

  elif '734782' in batch_file:

    renamed_file = ('734782_' + yesterday_date + '.csv')

  elif '732383' in batch_file:

    renamed_file = ('732383_' + first_date + '_' + yesterday_date + '.csv')

  elif 'clients' in batch_file:

    # Check if file sheet name contains I285407 (Panama ID)

    if (pd.ExcelFile(path_batch + batch_file).sheet_names[0] == 'I285407'):
      renamed_file = ('clients ' + today_date + ' herca757' + '.xls')
    else:
      renamed_file = ('clients ' + today_date + ' agmtech212' + '.xls')

  elif 'tasks_for_subaccounts' in batch_file:

    # Check if file contains account with Account Number F10740574 (BVI)

    df1 = pd.read_csv(path_batch + batch_file)

    if (len(df1[df1['IB Account ID'] == 'F10740574']) == 0):
      renamed_file = ('tasks_for_subaccounts ' + today_date + ' herca757' + '.csv')
    else:
      renamed_file = ('tasks_for_subaccounts ' + today_date + ' agmtech212' + '.csv')

  elif 'ContactListSummary' in batch_file:

    # Check if file contains Andres Aguilar Account
    # Find new fix

    df1 = pd.read_csv(path_batch + batch_file)

    if (len(df1[df1['Account Number'] == 'U13926601']) != 0):
      renamed_file = ('ContactListSummary ' + today_date + ' agmtech212' + '.csv')
    else:
      renamed_file = ('ContactListSummary ' + today_date + ' herca757' + '.csv')

  # RTD
  else:
    renamed_file = batch_file

  renamed_files.append(renamed_file)
  os.rename(path_batch + batch_file, path_batch + renamed_file)

print('Original files: ', batch_files)
print('Renamed files: ', renamed_files)

import os
import shutil

from datetime import datetime
import pytz

from pandas.tseries.offsets import BDay
import pandas as pd

import gspread
from google.colab import drive
from google.auth import default

# Mount GoogleDrive
#from google.colab import drive
#drive.mount('/content/gdrive')

# Authorize Google Sheets
creds, _ = default()
gs = gspread.authorize(creds)

cst = pytz.timezone('America/Costa_Rica')

# Get the current time in CST
cst_time = datetime.now(cst)

today_date = cst_time.strftime('%Y%m%d%H%M')
yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
first_date = cst_time.replace(day=1).strftime('%Y%m%d')

path_batch = '/content/gdrive/Shareddrives/ETL/batch/'
batch_files = os.listdir(path_batch)
renamed_files = []

df1 = pd.DataFrame()

for batch_file in batch_files:

  renamed_file = ''

  if '742588' in batch_file:

    renamed_file = ('742588_' + yesterday_date + '.csv')

  elif '734782' in batch_file:

    renamed_file = ('734782_' + yesterday_date + '.csv')

  elif '732383' in batch_file:

    renamed_file = ('732383_' + first_date + '_' + yesterday_date + '.csv')

  elif 'clients' in batch_file:

    # Check if file sheet name contains I285407 (Panama ID)

    if (pd.ExcelFile(path_batch + batch_file).sheet_names[0] == 'I285407'):
      renamed_file = ('clients ' + today_date + ' herca757' + '.xls')
    else:
      renamed_file = ('clients ' + today_date + ' agmtech212' + '.xls')

  elif 'tasks_for_subaccounts' in batch_file:

    # Check if file contains account with Account Number F10740574 (BVI)

    df1 = pd.read_csv(path_batch + batch_file)

    if (len(df1[df1['IB Account ID'] == 'F10740574']) == 0):
      renamed_file = ('tasks_for_subaccounts ' + today_date + ' herca757' + '.csv')
    else:
      renamed_file = ('tasks_for_subaccounts ' + today_date + ' agmtech212' + '.csv')

  elif 'ContactListSummary' in batch_file:

    # Check if file contains Andres Aguilar Account
    # Find new fix

    df1 = pd.read_csv(path_batch + batch_file)

    if (len(df1[df1['Account Number'] == 'U13926601']) != 0):
      renamed_file = ('ContactListSummary ' + today_date + ' agmtech212' + '.csv')
    else:
      renamed_file = ('ContactListSummary ' + today_date + ' herca757' + '.csv')

  # RTD
  else:
    renamed_file = batch_file

  renamed_files.append(renamed_file)
  os.rename(path_batch + batch_file, path_batch + renamed_file)

print('Original files: ', batch_files)
print('Renamed files: ', renamed_files)
"""