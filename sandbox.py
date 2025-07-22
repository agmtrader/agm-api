from src.utils.connectors.mongodb import MongoDB
from src.utils.connectors.supabase import db
from src.components.tools.reporting import get_clients_report
import pandas as pd

mongo = MongoDB()

"""
merged_df = pd.merge(accounts_df, clients_df, left_on='ibkr_username', right_on='Username', how='right')
merged_df = merged_df.fillna('')
merged_df = merged_df[merged_df['Email Address'] != '']
merged_df = merged_df[merged_df['temporal_email'] != '']
email_change_df = merged_df[merged_df['temporal_email'] == merged_df['Email Address']]
email_change_df = email_change_df.drop_duplicates(subset=['Email Address'])
email_change_df[['Email Address', 'temporal_email', 'Username', 'Title']].to_csv('email_change_df.csv', index=False)
"""

# Fetch all applications from MongoDB
clients = get_clients_report()
print(clients[100])

accounts = db.read('account', {})
print(accounts[100])

users = db.read('user', {})
print(users[100])

applications = mongo.read('application', {})
print(applications[30])

# ----------------------------------------------------------------------------------
# STEP 1: Build lookup dictionaries for efficient matching
# ----------------------------------------------------------------------------------

# Users by email (lowercased)
users_by_email = {
    user.get('email', '').lower(): user
    for user in users
    if user.get('email')
}

# Applications by user_id (assumes at most one application per user or we keep first)
applications_by_user_id = {}
for app in applications:
    uid = app.get('user_id')
    if uid and uid not in applications_by_user_id:
        applications_by_user_id[uid] = app

# Accounts by Account ID and by Username for quick lookup
accounts_by_account_id = {
    acc.get('ibkr_account_number'): acc for acc in accounts if acc.get('ibkr_account_number')
}
accounts_by_username = {
    acc.get('ibkr_username', '').lower(): acc for acc in accounts if acc.get('ibkr_username')
}

updates_performed = 0
unmatched_clients = []

# ----------------------------------------------------------------------------------
# STEP 2: Iterate over IBKR clients and perform linking
# ----------------------------------------------------------------------------------

for client in clients:
    client_email = str(client.get('Email Address', '')).lower()
    if not client_email:
        continue  # Skip clients without email

    # Find Supabase user by email
    user = users_by_email.get(client_email)
    if not user:
        unmatched_clients.append({**client, 'reason': 'user_not_found'})
        continue

    # Find application by user_id
    application = applications_by_user_id.get(user['id'])
    if not application:
        unmatched_clients.append({**client, 'reason': 'application_not_found'})
        continue

    # Find account by Account ID (ibkr_account_number) or Username (ibkr_username)
    account = accounts_by_account_id.get(client.get('Account ID'))
    if not account:
        username_lookup_key = str(client.get('Username', '')).lower()
        account = accounts_by_username.get(username_lookup_key)

    if not account:
        unmatched_clients.append({**client, 'reason': 'account_not_found'})
        continue

    # Link account to application if not already linked
    if account.get('application_id') != application['id']:
        try:
            
            db.update(
                'account',
                query={'id': account['id']},
                data={'application_id': application['id']}
            )
            updates_performed += 1
            print(
                f"Linked application {application['id']} -> account {account['id']} (client email: {client_email})"
            )
        except Exception as e:
            unmatched_clients.append({**client, 'reason': f'update_failed: {str(e)}'})

# ----------------------------------------------------------------------------------
# SUMMARY
# ----------------------------------------------------------------------------------

print(f"Total accounts updated: {updates_performed}")

if unmatched_clients:
    print(f"{len(unmatched_clients)} clients could not be linked. Sample:")
    for sample in unmatched_clients[:10]:
        print(sample['reason'], sample.get('Username'), sample.get('Account ID'), sample.get('Email Address'))