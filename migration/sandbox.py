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
        raise

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
            'id': row['No. de Identificación (no utilizar caracters ni espacio) / ID Number (Please do not use special characters or leave blank spaces)'].replace('-', '').strip()
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
            'id': row['No. de Identificación (no utilizar caracters ni espacio) / ID Number (Please do not use special characters or leave blank spaces)'].replace('-', '').strip()
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
            'id': ticket['ApplicationInfo'].get('id_number', '')
        }
        tickets_df = pd.concat([tickets_df, pd.DataFrame([ticket])], ignore_index=True)
    
    tickets_df.to_csv('outputs/tickets.csv', index=False)
    return tickets_df

tickets_df = process_tickets()

""" Process accounts """

# Accounts from old system
accounts_df = pd.read_csv('sources/accounts.csv')
accounts_df = accounts_df.fillna('')

# Created accounts
accounts_df2 = pd.read_csv('sources/new_accounts.csv')
accounts_df2 = accounts_df2.fillna('')

# Live accounts from new system
live_accounts = access_api('/accounts/read', 'POST', {})
live_accounts_df = pd.DataFrame(live_accounts)
live_accounts_df = live_accounts_df.drop(columns=['id'])

# Merge all accounts
accounts_df = pd.concat([accounts_df, live_accounts_df, accounts_df2], ignore_index=True)
accounts_df['AccountID'] = accounts_df['AccountID'].astype(int)

""" Process clients """
# Read clients from excel file
clients_df = pd.DataFrame()
excel_file = pd.ExcelFile('sources/clients.xls')
for sheet_name in excel_file.sheet_names:
    sheet_df = pd.read_excel(excel_file, sheet_name=sheet_name)
    sheet_df['MasterAccount'] = sheet_name
    clients_df = pd.concat([clients_df, sheet_df], ignore_index=True)
clients_df = clients_df.fillna('')

"""Create new accounts for all clients using IBKR Data as source"""
new_accounts = []
for index, row in clients_df.iterrows():
    new_accounts.append({
        'Title': row.get('Title', ''),
        'EmailAddress': row.get('Email Address', ''),
        'SLS': row.get('SLS Devices', ''),
        'AccountID': '',
        'AccountStatus': row.get('Status', ''),
        'AccountNumber': row.get('Account ID', ''),
        'Advisor': '',
        'IBKRUsername': row.get('Username', ''),
        'IBKRPassword': '',
        'MasterAccount': row.get('MasterAccount', ''),
        'TemporalEmail': '',
        'TemporalPassword': '',
        'TicketID': '',
        'UserID': '',
    })

new_accounts_df = pd.DataFrame(new_accounts)

"""Fill in existing accounts using IBKRUsername as key"""
all_accounts = []
for index, new_account in new_accounts_df.iterrows():
    username = new_account['IBKRUsername']
    if pd.notna(username) and username.strip():  # Check if username exists and is not empty
        # Find matching account in existing accounts_df
        matching_account = accounts_df[accounts_df['IBKRUsername'] == username]
        
        if not matching_account.empty:
            # Account found - merge information
            account = {
                'Status': 'Matched',
                'Title': new_account['Title'],
                'EmailAddress': new_account['EmailAddress'],
                'SLS': new_account['SLS'],
                'AccountID': int(matching_account.iloc[0].get('AccountID', '')),
                'AccountNumber': new_account['AccountNumber'],
                'Advisor': matching_account.iloc[0].get('Advisor', ''),
                'AccountStatus': new_account['AccountStatus'],
                'IBKRUsername': username,
                'IBKRPassword': matching_account.iloc[0].get('IBKRPassword', ''),
                'MasterAccount': new_account['MasterAccount'],
                'TemporalEmail': matching_account.iloc[0].get('TemporalEmail', ''),
                'TemporalPassword': matching_account.iloc[0].get('TemporalPassword', ''),
                'Status': matching_account.iloc[0].get('Status', ''),
            }
            
        else:
            # Account not found - keep new account info with blank values for existing fields
            account = {
                'Status': 'New',
                'Title': new_account['Title'],
                'EmailAddress': new_account['EmailAddress'],
                'SLS': new_account['SLS'],
                'AccountID': '',
                'AccountNumber': new_account['AccountNumber'],
                'Advisor': '',
                'AccountStatus': new_account['AccountStatus'],
                'IBKRUsername': username,
                'IBKRPassword': '',
                'MasterAccount': new_account['MasterAccount'],
                'TemporalEmail': '',
                'TemporalPassword': '',
            }
        all_accounts.append(account)

# Create DataFrame with all accounts
all_accounts_df = pd.DataFrame(all_accounts)

"""Fill in Ticket_Email using AccountNumber as key"""

# Read already existing emails
emails_df = pd.read_csv('sources/emails.csv')
emails_df = emails_df.fillna('')

emails_2_df = pd.read_csv('sources/emails_2.csv')
emails_2_df = emails_2_df.fillna('')

# Fill in Ticket_Email from emails_df
for index, row in all_accounts_df.iterrows():
    matching_email = emails_df[emails_df['AccountNumber'] == row['AccountNumber']]
    if not matching_email.empty:
        all_accounts_df.at[index, 'Ticket_Email'] = matching_email.iloc[0]['Ticket_Email']

# Fill in Ticket_Email from emails_2_df
for index, row in all_accounts_df.iterrows():
    matching_email = emails_2_df[emails_2_df['AccountNumber'] == row['AccountNumber']]
    if not matching_email.empty:
        all_accounts_df.at[index, 'Ticket_Email'] = matching_email.iloc[0]['Ticket_Email']

"""Fill in TicketID using Ticket_Email as key"""
all_accounts_df['TicketID'] = ''
for index, row in all_accounts_df.iterrows():
    if pd.notna(row['Ticket_Email']) and row['Ticket_Email']:
        matching_ticket = tickets_df[tickets_df['email'] == row['Ticket_Email']]
        if not matching_ticket.empty:
            all_accounts_df.at[index, 'TicketID'] = matching_ticket.iloc[0]['TicketID']

"""Fill in Advisors using AccountNumber as key"""
# Accounts from PowerBI app
clients_powerbi_df = pd.read_excel('sources/clients_web.xlsx')
clients_powerbi_df = clients_powerbi_df.fillna('')

# Fill in advisor information from PowerBI app
for index, row in all_accounts_df.iterrows():
    matching_account = clients_powerbi_df[clients_powerbi_df['Account Hierarchy - Account ID'] == row['AccountNumber'].strip()]
    if not matching_account.empty:
        if matching_account.iloc[0]['Advisor Name'] != 'Unknown':
            all_accounts_df.at[index, 'Advisor'] = matching_account.iloc[0]['Advisor Name']
        else:
            all_accounts_df.at[index, 'Advisor'] = matching_account.iloc[0]['Advisor G-Form']

"""Add AdvisorEmail using Advisor as key"""
advisors_df = pd.read_csv('sources/advisors.csv')
advisors_df = advisors_df.fillna('')

for index, row in all_accounts_df.iterrows():
    matching_advisor = advisors_df[advisors_df['Advisor'] == row['Advisor']]
    if not matching_advisor.empty:
        all_accounts_df.at[index, 'AdvisorEmail'] = matching_advisor.iloc[0]['Email']

"""Post process and Analysis"""
all_accounts_df = all_accounts_df.fillna('')
all_accounts_df[['Title', 'AccountNumber', 'AccountID','MasterAccount', 'EmailAddress', 'AccountStatus', 'IBKRUsername', 'Advisor', 'AdvisorEmail', 'SLS', 'TemporalEmail', 'Ticket_Email', 'TicketID']].to_csv('outputs/all_accounts.csv', index=False)

print("Account Analysis:")
print(f"Total accounts: {len(all_accounts_df)}")
print(f"Accounts with no account id: {len(all_accounts_df[all_accounts_df['AccountID'] == ''])}")
print(f"Accounts with no advisor: {len(all_accounts_df[all_accounts_df['Advisor'] == ''])}")
print(f"Accounts with an Unknown advisor: {len(all_accounts_df[all_accounts_df['Advisor'] == 'Unknown'])}")

new_excel = all_accounts_df[['AccountNumber','Advisor','TicketID']]
new_excel.to_excel('outputs/new_accounts.xlsx', index=False)