import requests
import os
import pandas as pd
import random
import string
from datetime import datetime
from difflib import SequenceMatcher

url = f'http://127.0.0.1:{os.getenv("PORT")}'
url = 'https://api.agmtechnology.com'

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

    # Accounts from old system
    accounts_df = pd.read_csv('sources/accounts.csv')
    accounts_df = accounts_df.fillna('')

    # Created accounts
    accounts_df2 = pd.read_csv('sources/accounts2.csv')
    accounts_df2 = accounts_df2.fillna('')

    # Live accounts from new system
    live_accounts = access_api('/accounts/read', 'POST', {})
    live_accounts_df = pd.DataFrame(live_accounts)
    live_accounts_df = live_accounts_df.drop(columns=['id'])

    # Merge all accounts
    accounts_df = pd.concat([accounts_df, live_accounts_df, accounts_df2], ignore_index=True)
    accounts_df['AccountID'] = accounts_df['AccountID'].astype(int)
    accounts_df.to_csv('outputs/accounts.csv', index=False)
    return accounts_df

def match_emails_to_clients(emails_df, clients_df):
    print("Matching emails to clients...")
    
    # Create lookup dictionaries for faster matching
    clients_by_account_id = {}
    clients_by_user_hierarchy = {}
    
    for idx, client in clients_df.iterrows():
        account_id = client['Account Hierarchy - Account ID']
        user_hierarchy = client['User Hierarchy - Account User']
        
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
                'Client_UserHierarchy_AccountUser': matched_client['User Hierarchy - Account User'],
                'Client_AccountHierarchy_AccountID': matched_client['Account Hierarchy - Account ID'],
                'Client_AccountTitle': matched_client['Account Title'],
                'Client_EmailAddress': matched_client['Email Address'],
                'Client_Status': matched_client['Status'],
                'Client_AdvisorName': matched_client['Advisor Name'],
                
                # Match metadata
                'Match_Type': match_type
            }
            matched_records.append(matched_record)
            
            # Track that this client was matched
            client_idx = clients_df[clients_df['Account Hierarchy - Account ID'] == matched_client['Account Hierarchy - Account ID']].index[0]
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

tickets_df = process_tickets()
accounts_df = process_accounts()

""" Process clients """
clients_df = pd.read_excel('sources/clients_web.xlsx')
clients_df.to_csv('outputs/clients.csv', index=False)

# Execute the matching
emails_df = pd.read_csv('sources/emails.csv')
emails_df = emails_df.fillna('')

matched_emails_df, unmatched_emails_df = match_emails_to_clients(emails_df, clients_df)

# Merge with email data
merged_df = pd.merge(
    clients_df,
    matched_emails_df,
    left_on='Account Hierarchy - Account ID',
    right_on='Email_AccountNumber',
    how='left',
    suffixes=('', '_email')
)

# TODO: Why does IBKR not give us emails for some clients?
# TODO: Merge with accounts data for more temporal emails
# TODO: Merge with tickets data for more ticket emails

# Save merged data
merged_df = merged_df.fillna('')
merged_df[['Account Hierarchy - Master Account', 'User Hierarchy - Account User', 'Account Hierarchy - Account ID', 'Account Title', 'Status', 'Date Opened', 'Phone Number', 'SLS Devices', 'Advisor Name', 'Email Address', 'Email_TemporalEmail', 'Email_TicketEmail']].to_csv('outputs/all.csv', index=False)

print("Account Analysis:")
print(f"Total: {len(merged_df)}")
print("\n")
print(f"Clients with no advisor: {len(clients_df[clients_df['Advisor Name'] == ''])}")
print(f"Clients with an Unknown advisor: {len(clients_df[clients_df['Advisor Name'] == 'Unknown'])}")
print("\n")
print(f"Clients with no email address: {len(merged_df[merged_df['Email Address'] == ''])}")
print(f"Clients with no temporal email: {len(merged_df[merged_df['Email_TemporalEmail'] == ''])}")
print(f"Clients with no ticket email: {len(merged_df[merged_df['Email_TicketEmail'] == ''])}")
print("\n")
print(f"Clients with no SLS Devices: {len(merged_df[merged_df['SLS Devices'] == ''])}")
print(f"Clients with no phone number: {len(merged_df[merged_df['Phone Number'] == ''])}")