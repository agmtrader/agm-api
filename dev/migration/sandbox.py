import requests
import os
import pandas as pd
import random
import string
from datetime import datetime
from difflib import SequenceMatcher

#url = f'http://127.0.0.1:{os.getenv("PORT")}'
url = 'https://api.agmtechnology.com'

def access_api(endpoint, method='GET', data=None):
    try:
        auth = requests.post(
            url + '/token', 
            json={'token': 'bdb68ccc-213e-4a44-b481-b11d9d45da02', 'scopes': 'all'},
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
        raise e

def parse_timestamp(timestamp_str):
    formats = [
        '%d/%m/%Y %H:%M',
        '%m/%d/%Y %H:%M',
        '%d/%m/%Y %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S.%f',
        '%m/%d/%Y %H:%M:%S.%f',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(timestamp_str, fmt)
            return dt.strftime('%Y%m%d%H%M%S')
        except ValueError:
            continue
    
    print(f"Failed to parse timestamp: {timestamp_str}")
    return None

def generate_password(length=6):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def increment_timestamp(timestamp):
    # Convert timestamp string to datetime
    dt = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
    # Add one second
    dt = dt + pd.Timedelta(seconds=1)
    # Convert back to string
    return dt.strftime('%Y%m%d%H%M%S')

def similar(a, b):
    """Calculate string similarity ratio between two strings."""
    return SequenceMatcher(None, a, b).ratio()

def process_tickets():

    """ PROCESS TICKETS """
    # Read tickets from csv files
    tickets_df1 = pd.read_csv('sources/tickets_form1.csv')
    tickets_df2 = pd.read_csv('sources/tickets_form2.csv')
    tickets_df1 = tickets_df1.fillna('')
    tickets_df2 = tickets_df2.fillna('')
    tickets = []
    
    # First pass: collect all timestamps
    all_timestamps = set()
    for index, row in tickets_df1.iterrows():
        timestamp = parse_timestamp(row['Marca temporal'])
        if timestamp in all_timestamps:
            timestamp = increment_timestamp(timestamp)
        all_timestamps.add(timestamp)
        
        ticket = {
            'Advisor': '',
            'Status': 'Opened',
            'TicketID': timestamp,
            'UserID': timestamp, 
            'email': row['Correo Electrónico / E-mail Address'],
            'first_name': row['Nombre (incluir segundos nombres) / Name (Include middle name)'],
            'last_name': row['Apellido / Last Name'],
            'id': row['No. de Identificación (no utilizar caracters ni espacio) / ID Number (Please do not use special characters or leave blank spaces)'].replace('-', '').strip(),
            'phone': row['Número de Teléfono (Celular) / Phone (Mobile)']
        }
        tickets.append(ticket)

    for index, row in tickets_df2.iterrows():
        timestamp = parse_timestamp(row['Marca temporal'])
        if timestamp in all_timestamps:
            timestamp = increment_timestamp(timestamp)
        all_timestamps.add(timestamp)
        
        ticket = {
            'Advisor': '',
            'Status': 'Opened',
            'TicketID': timestamp,
            'UserID': timestamp,   
            'email': row['Correo Electrónico / E-mail Address'],
            'first_name': row['Nombre (incluir segundos nombres) / Name (Include middle name)'],
            'last_name': row['Apellido / Last Name'],
            'phone': row['Número de Teléfono (Celular) / Phone (Mobile)']
        }
        tickets.append(ticket)

    # Save tickets to csv file
    tickets_df = pd.DataFrame(tickets)

    live_tickets = access_api('/tickets/read', 'POST', {})
    for ticket in live_tickets:
        ticket = {
            'Advisor': '',
            'Status': 'Opened',
            'TicketID': ticket['TicketID'],
            'UserID': ticket['UserID'],   
            'email': ticket['ApplicationInfo']['email'],
            'first_name': ticket['ApplicationInfo'].get('first_name', ''),
            'last_name': ticket['ApplicationInfo'].get('last_name', ''),
            'phone': ticket['ApplicationInfo'].get('phone', '')
        }
        tickets_df = pd.concat([tickets_df, pd.DataFrame([ticket])], ignore_index=True)
    
    tickets_df.to_csv('outputs/tickets.csv', index=False)
    return tickets_df

def process_accounts():
    """ Process accounts """
    accounts = access_api('/accounts/read', 'POST', {'query': {}})
    accounts_df = pd.DataFrame(accounts)
    accounts_df.to_csv('outputs/accounts.csv', index=False)
    return accounts_df

def match_emails_to_clients(emails_df, clients_df):
    # Create lookup dictionaries for faster matching
    clients_by_account_id = {}
    clients_by_user_hierarchy = {}
    
    for idx, client in clients_df.iterrows():
        account_id = client['Account ID']
        user_hierarchy = client['Username']
        
        if account_id:
            clients_by_account_id[account_id] = client
        if user_hierarchy:
            if user_hierarchy not in clients_by_user_hierarchy:
                clients_by_user_hierarchy[user_hierarchy] = []
            clients_by_user_hierarchy[user_hierarchy].append(client)
    
    matched_records = []
    unmatched_emails = []
    matched_client_indices = set()
    
    # Match emails to clients
    for idx, email_record in emails_df.iterrows():
        account_number = email_record['AccountNumber']
        title = email_record['Title']
        
        matched_client = None
        match_type = None
        
        # Try to match by AccountNumber first (more reliable)
        if account_number and account_number in clients_by_account_id:
            matched_client = clients_by_account_id[account_number]
            match_type = 'AccountNumber'
            
        # If no match by AccountNumber, try by Title/User Hierarchy
        elif title and title in clients_by_user_hierarchy:
            potential_clients = clients_by_user_hierarchy[title]
            if len(potential_clients) == 1:
                matched_client = potential_clients[0]
                match_type = 'Title_Unique'
            else:
                matched_client = potential_clients[0]
                match_type = 'Title_Multiple'
        
        if matched_client is not None:
            # Create matched record
            matched_record = {
                # Email fields
                'Email_Title': title,
                'Email_AccountNumber': account_number,
                'Email_EmailAddress': email_record['EmailAddress'],
                'Email_TemporalEmail': email_record['TemporalEmail'],
                'Email_TicketEmail': email_record['Ticket_Email'],
                
                # Client fields
                'Client_Username': matched_client['Username'],
                'Client_AccountID': matched_client['Account ID'],
                'Client_Title': matched_client['Title'],
                'Client_EmailAddress': matched_client['Email Address'],
                'Client_Status': matched_client['Status'],
                
                # Match metadata
                'Match_Type': match_type
            }
            matched_records.append(matched_record)
            
            # Track that this client was matched
            client_idx = clients_df[clients_df['Account ID'] == matched_client['Account ID']].index[0]
            matched_client_indices.add(client_idx)
            
        else:
            unmatched_email = {
                'Email_Title': title,
                'Email_AccountNumber': account_number,
                'Email_EmailAddress': email_record['EmailAddress'],
                'Reason': 'No matching client found'
            }
            unmatched_emails.append(unmatched_email)
    
    # Create DataFrames and save
    matched_df = pd.DataFrame(matched_records)
    unmatched_emails_df = pd.DataFrame(unmatched_emails)
    
    return matched_df, unmatched_emails_df

#tickets_df = process_tickets()
accounts_df = process_accounts()

""" Process clients """
# Read all sheets from the Excel file and combine them
excel_file = pd.ExcelFile('sources/clients.xls')
all_sheets = []

for sheet_name in excel_file.sheet_names:
    print(f"Reading sheet: {sheet_name}")
    sheet_df = pd.read_excel('sources/clients.xls', sheet_name=sheet_name)
    sheet_df['Source_Sheet'] = sheet_name  # Add column to track which sheet data came from
    all_sheets.append(sheet_df)

# Combine all sheets into one DataFrame
clients_df = pd.concat(all_sheets, ignore_index=True)
clients_df.to_csv('outputs/clients.csv', index=False)

# Execute the matching
emails_df = pd.read_csv('sources/emails.csv')
emails_df = emails_df.fillna('')

matched_emails_df, unmatched_emails_df = match_emails_to_clients(emails_df, clients_df)

# Merge with email data
merged_df = pd.merge(
    clients_df,
    matched_emails_df,
    left_on='Account ID',
    right_on='Email_AccountNumber',
    how='left',
    suffixes=('', '_email')
)

# Merge with accounts data for more temporal emails
# Rename the accounts TemporalEmail to avoid confusion and ensure proper consolidation
accounts_merge_df = accounts_df[['ibkr_username', 'temporal_email']].copy()
accounts_merge_df = accounts_merge_df.rename(columns={'temporal_email': 'TemporalEmail_Accounts'})

merged_df_with_accounts = pd.merge(
    merged_df,
    accounts_merge_df,
    left_on='Username',
    right_on='ibkr_username',
    how='left'
)

# Proper consolidation of temporal emails
# Priority: Use TemporalEmail_Accounts if available, otherwise use Email_TemporalEmail
def consolidate_temporal_email(row):
    accounts_email = row.get('TemporalEmail_Accounts', '')
    emails_email = row.get('Email_TemporalEmail', '')
    
    # If accounts has email, use it (it's more reliable)
    if pd.notna(accounts_email) and accounts_email != '':
        return accounts_email
    # Otherwise, use email from emails data
    elif pd.notna(emails_email) and emails_email != '':
        return emails_email
    # If neither, return empty string
    else:
        return ''

merged_df_with_accounts['Email_TemporalEmail'] = merged_df_with_accounts.apply(consolidate_temporal_email, axis=1)

# Drop the extra columns from the merge after consolidation
merged_df_final = merged_df_with_accounts.drop(columns=['IBKRUsername', 'TemporalEmail_Accounts'], errors='ignore')

# Save merged data
merged_df_final = merged_df_final.fillna('')
merged_df_final[['Username', 'Account ID', 'Title', 'Status', 'Date Opened', 'Phone Number', 'SLS Devices', 'Email Address', 'Email_TemporalEmail', 'Email_TicketEmail']].to_csv('outputs/all.csv', index=False)

print("\n")
print("Account Analysis:")
print(f"Total: {len(merged_df_final)}")
print("\n")
print(f"Clients with no phone number: {len(merged_df_final[merged_df_final['Phone Number'] == ''])}")
print(f"Clients with no SLS Devices: {len(merged_df_final[merged_df_final['SLS Devices'] == ''])}")
print(f"Clients with no email address: {len(merged_df_final[merged_df_final['Email Address'] == ''])}")
print("\n")
print(f"Clients with no temporal email: {len(merged_df_final[merged_df_final['Email_TemporalEmail'] == ''])}")
print(f"Clients with no ticket email: {len(merged_df_final[merged_df_final['Email_TicketEmail'] == ''])}")
print("\n")

# account_ids missing email change -> user_id -> contact_id -> send contact for the email change