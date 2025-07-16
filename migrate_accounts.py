import pandas as pd
import numpy as np
from src.utils.connectors.supabase import db

accounts_df = pd.read_csv('dev/migration/outputs/consolidated.csv')
accounts_df = accounts_df.replace({np.nan: None})
for account in accounts_df.to_dict(orient='records'):
    try:
        db.update('account',{'ibkr_account_number': account['ibkr_account_number']}, {'advisor_code': account['advisor_code']})
    except Exception as e:
        print(e)
        continue