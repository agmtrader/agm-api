import pandas as pd
import requests

def access_api(endpoint, method='GET', data=None):
    url = 'https://api.agmtechnology.com'
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
        raise

# Read clients from csv
accounts = access_api('/accounts/read', 'POST', {'query': {}})
all_accounts_df = pd.DataFrame(accounts)
all_accounts_df = all_accounts_df.fillna('')

# Filter for open accounts
all_accounts_df = all_accounts_df[all_accounts_df['Status'] == 'Open']

# Filter for accounts with no changed email
no_changed_email = all_accounts_df[all_accounts_df['Email Address'] == all_accounts_df['Email_TemporalEmail']]

# Clean phone numbers - convert from float format to clean integers
no_changed_email['Phone Number'] = no_changed_email['Phone Number'].apply(lambda x: str(int(float(x))) if x != '' and pd.notna(x) else '')

# Save accounts with no changed email
no_changed_email[['Title', 'Phone Number', 'Email Address', 'Email_TemporalEmail', 'Email_TicketEmail']].to_csv('outputs/no_changed_email.csv', index=False)

# Print results
print(f"Accounts with no ticket email: {len(no_changed_email[no_changed_email['Email_TicketEmail'] == ''])}")
print(f"Accounts with no temporal email: {len(no_changed_email[no_changed_email['Email_TemporalEmail'] == ''])}")
print(f"Accounts with no IBKR email address: {len(no_changed_email[no_changed_email['Email Address'] == ''])}")
print(f"Accounts with no phone number: {len(no_changed_email[no_changed_email['Phone Number'] == ''])}")
print("\n")
print(f"Accounts with no changed email: {len(no_changed_email)}")

# Get set of emails to send
no_changed_email_dict = no_changed_email.to_dict(orient='records')

# Get set of emails to send and send emails
emails_to_send = set()
for user in no_changed_email_dict:
    emails_to_send.add(user['Email_TicketEmail'])
    
print('Emails to send: ' + str(len(emails_to_send)))

for email in emails_to_send:
    access_api('/email/send_email/email_change', 'POST', data={'client_email': email, 'advisor_email': ''})
    print("Email sent to: " + email)