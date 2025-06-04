import pandas as pd

all_accounts_df = pd.read_csv('outputs/all_accounts.csv')
all_accounts_df = all_accounts_df.fillna('')

all_accounts_df = all_accounts_df[all_accounts_df['AccountStatus'] == 'Open']

no_changed_email = all_accounts_df[all_accounts_df['TemporalEmail'] == all_accounts_df['EmailAddress']]

no_ticket_email = no_changed_email[no_changed_email['Ticket_Email'] == '']
no_changed_email_no_advisor = no_changed_email[all_accounts_df['Advisor'] == '']
no_changed_email_unknown_advisor = no_changed_email[all_accounts_df['Advisor'] == 'Unknown']

print(f"Accounts with no ticket email: {len(no_ticket_email)}")
print(f"Accounts with no changed email and no advisor: {len(no_changed_email_no_advisor)}")
print(f"Accounts with no changed email and unknown advisor: {len(no_changed_email_unknown_advisor)}")
print(f"Accounts with no changed email: {len(no_changed_email)}")
print(f"Accounts with no SLS Devices: {len(all_accounts_df[all_accounts_df['SLS'] == ''])}")
no_changed_email[['Title', 'AccountNumber', 'EmailAddress', 'TemporalEmail', 'Ticket_Email', 'Advisor']].to_excel('outputs/no_changed_email.xlsx', index=False)

no_changed_email_dict = no_changed_email.to_dict(orient='records')

import requests
url = 'http://127.0.0.1:5000'
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
        raise

advisors = access_api('/advisors/read', 'POST', {'query': {}})
contacts = access_api('/contacts/read', 'POST', {'query': {}})

print(len(no_changed_email_dict))

emails_to_send = set()
for user in no_changed_email_dict:
    emails_to_send.add(user['Ticket_Email'])
    

print(len(emails_to_send))
for email in emails_to_send:
    access_api('/email/send_email/email_change', 'POST', data={'client_email': email, 'advisor_email': ''})
    print("Email sent to: " + email)