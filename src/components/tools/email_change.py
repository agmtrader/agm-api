from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.connectors.supabase import db
import pandas as pd
from src.components.tools.reporting import get_clients_report
from src.components.tools.email import Gmail

logger.announcement('Initializing Email Change Service', type='info')
gmail = Gmail()
logger.announcement('Initialized Email Change Service', type='success')

@handle_exception
def read_email_change() -> list:

    accounts = db.read('account', {})
    accounts_df = pd.DataFrame(accounts)

    users = db.read('user', {})
    users_df = pd.DataFrame(users)

    clients = get_clients_report()
    clients_df = pd.DataFrame(clients)

    merged_df = pd.merge(accounts_df, clients_df, left_on='ibkr_username', right_on='Username', how='left')
    merged_df = pd.merge(merged_df, users_df, left_on='user_id', right_on='id', how='left')
    merged_df = merged_df.drop_duplicates(subset=['Account ID', 'ibkr_account_number'])
    merged_df = merged_df.fillna('')

    # Email change
    accounts_that_need_email_change = merged_df[merged_df['Email Address'] != '']
    accounts_that_need_email_change = accounts_that_need_email_change[accounts_that_need_email_change['temporal_email'] != '']
    accounts_that_need_email_change = accounts_that_need_email_change[accounts_that_need_email_change['Email Address'] == accounts_that_need_email_change['temporal_email']]
    print('accounts that need email change: ', len(accounts_that_need_email_change))

    accounts_with_warning = accounts_that_need_email_change[accounts_that_need_email_change['email'] == accounts_that_need_email_change['temporal_email']]
    accounts_with_warning_no_email = accounts_that_need_email_change[accounts_that_need_email_change['email'] == '']
    print('accounts that are saved with temporary email: ', len(accounts_with_warning))
    print('accounts missing email in database: ', len(accounts_with_warning_no_email))

    accounts_that_need_email_change = accounts_that_need_email_change.drop_duplicates(subset=['email'])
    return accounts_that_need_email_change.to_dict(orient='records')