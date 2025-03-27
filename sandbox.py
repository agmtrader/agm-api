import requests
import os
from src.utils.logger import logger
import pandas as pd

# Use localhost instead of 0.0.0.0
url = f'http://127.0.0.1:{os.getenv("PORT")}'

def access_api(endpoint, method='GET', data=None):
    try:
        auth = requests.post(
            url + '/token', 
            json={'token': 'vIkY4of6iVgXRwLTMpHM', 'scopes': 'all'},
        )

        print(auth.json())
        
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