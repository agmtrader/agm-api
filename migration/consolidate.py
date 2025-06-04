import pandas as pd

all_accounts_df = pd.read_csv('outputs/all_accounts.csv')
all_accounts_df = all_accounts_df.fillna('')

no_ticket_email = all_accounts_df[all_accounts_df['Ticket_Email'] == '']
no_ticket_id = all_accounts_df[all_accounts_df['TicketID'] == '']

print(f"Accounts with no ticket email: {len(no_ticket_email)}")
print(f"Accounts with no ticket id: {len(no_ticket_id)}")