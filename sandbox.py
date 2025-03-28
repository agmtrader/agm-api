import requests
import os
from src.utils.logger import logger
import pandas as pd
import json  # Add json import

# Use localhost instead of 0.0.0.0
url = f'http://127.0.0.1:{os.getenv("PORT")}'

def access_api(endpoint, method='GET', data=None):
    try:
        auth = requests.post(
            url + '/token', 
            json={'token': 'vIkY4of6iVgXRwLTMpHM', 'scopes': 'all'},
        )
        
        response = requests.request(
            method, 
            url + endpoint, 
            json=data, 
            headers={'Authorization': f'Bearer {auth.json()["access_token"]}'},
        )
        
        try:
            return response.json()
        except:
            return response.content
            
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

"""
# Get all clients from form and backup
columns = ['AccountID', 'IBKRUsername', 'IBKRPassword', 'TemporalEmail', 'Advisor', 'MasterAccount', 'TemporalPassword', 'AccountNumber', 'TicketID', 'UserID']

old_accounts_df = pd.read_csv('accounts_form.csv')

old_accounts_df['AccountID'] = pd.to_datetime(old_accounts_df['Marca temporal'], format='mixed').dt.strftime('%Y%m%d%H%M%S')
old_accounts_df['Advisor'] = ''
old_accounts_df['AccountNumber'] = old_accounts_df['Numero de cuenta']
old_accounts_df['IBKRUsername'] = old_accounts_df['Usuario']
old_accounts_df['IBKRPassword'] = old_accounts_df['Contraseña']
old_accounts_df['TemporalEmail'] = old_accounts_df['Dirección de Correo']
old_accounts_df['TemporalPassword'] = old_accounts_df['Contraseña']
old_accounts_df['TicketID'] = ''
old_accounts_df['MasterAccount'] = ''
old_accounts_df['UserID'] = ''

# Create a new dataframe with renamed columns
extracted_df = old_accounts_df[columns]
extracted_df = extracted_df.fillna('')

accounts_df = pd.read_csv('web_accounts.csv')
accounts_df = accounts_df.fillna('')
accounts_array = accounts_df.to_dict(orient='records')

merged_df = pd.concat([extracted_df, accounts_df], ignore_index=True)

def ensure_unique_account_ids(df):
    # Create a copy to avoid modifying the original dataframe
    df = df.copy()
    
    # Find duplicates
    duplicates = df[df.duplicated(subset=['AccountID'], keep='first')]
    
    # For each duplicate, increment the timestamp by seconds
    for idx in duplicates.index:
        original_id = df.loc[idx, 'AccountID']
        # Get count of this ID to use as increment
        count = len(df[df['AccountID'] == original_id][:idx])
        # Add seconds to the timestamp
        new_id = str(int(original_id) + count)
        df.loc[idx, 'AccountID'] = new_id
    
    return df

# Apply the function to ensure unique AccountIDs
final_df = ensure_unique_account_ids(merged_df)

final_df.to_csv('merged_accounts.csv', index=False)
final_array = final_df.to_dict(orient='records')

print(final_df)

for account in accounts_array[:2]:
    access_api('/accounts/create', method='POST', data={'data': account, 'id': account['AccountID']})
"""

"""
ids = pd.read_csv('ids_form.csv')
new_df = pd.DataFrame(columns=['DocumentID', 'DocumentInfo', 'FileInfo', 'Uploader', 'Category'])

new_df['DocumentID'] = pd.to_datetime(ids['Marca temporal'], format='mixed').dt.strftime('%Y%m%d%H%M%S')

# Create DocumentInfo dictionaries for each row
def create_document_info(row):
    return {
        'account_number': row['Account Number'],
        'country_of_birth': row['Place of Birth'],
        'country_of_issue': row['Country of Issue'],
        'date_of_birth': row['Date of Birth'],
        'expiration_date': row['Expiration Date'],
        'full_name': row['Full Name'],
        'gender': row['Gender'],
        'id_number': row['Document Number'],
        'issued_date': row['Issued Date'],
        'type': row['Type']
    }

new_df['DocumentInfo'] = ids.apply(create_document_info, axis=1)
new_df['FileInfo'] = ''
new_df['Uploader'] = ids['Dirección de correo electrónico']
new_df['Category'] = 'identity'

# Convert JSON columns to strings before saving
new_df['DocumentInfo'] = new_df['DocumentInfo'].apply(json.dumps)
new_df['FileInfo'] = new_df['FileInfo'].apply(lambda x: json.dumps(x) if x else '')

# Check for duplicate DocumentIDs
duplicates = new_df[new_df.duplicated(subset=['DocumentID'], keep=False)]
if len(duplicates) > 0:
    
    # For each duplicate, increment the timestamp by seconds
    for doc_id in duplicates['DocumentID'].unique():
        # Get all rows with this DocumentID
        mask = new_df['DocumentID'] == doc_id
        duplicate_indices = new_df[mask].index
        
        # For each duplicate after the first one, add seconds to the timestamp
        for i, idx in enumerate(duplicate_indices[1:], 1):
            original_id = int(new_df.loc[idx, 'DocumentID'])
            new_id = str(original_id + i)  # Add i seconds to the timestamp
            new_df.loc[idx, 'DocumentID'] = new_id

# Get file info for each document and store in FileInfo
for index, row in ids.iterrows():
    if 'drive.google.com' in str(row['Upload']):
        file_id = row['Upload'].split('id=')[-1]
        if index % 100 == 0:
            print(f'{index} / {len(ids)}')
        try:
            file_info = access_api('/drive/get_file_info_by_id', method='POST', data={'file_id': file_id})
            if file_info.get('error'):
                new_df.at[index, 'FileInfo'] = ''
            else:
                # Convert the Python dict to a proper JSON string with double quotes
                new_df.at[index, 'FileInfo'] = json.dumps(file_info)
        except Exception as e:
            pass
"""


"""
# Save to CSV
df = pd.read_csv('ids.csv')
df_list = df.to_dict(orient='records')
for row in df_list:
    
    row['DocumentID'] = str(int(row['DocumentID']))
    
    # Parse DocumentInfo and handle nan values
    doc_info = json.loads(row['DocumentInfo'])
    for key, value in doc_info.items():
        if pd.isna(value):
            doc_info[key] = ''
    row['DocumentInfo'] = doc_info
    
    # Parse FileInfo and handle nan values
    if pd.notna(row['FileInfo']) and row['FileInfo']:
        file_info = json.loads(row['FileInfo'])
        for key, value in file_info.items():
            if pd.isna(value):
                file_info[key] = ''
        row['FileInfo'] = file_info
    else:
        row['FileInfo'] = ''
    
    print(row)
"""

access_api('/account_management/accounts', method='GET')