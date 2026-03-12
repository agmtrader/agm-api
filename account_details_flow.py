from src.components.entities.accounts import read_accounts, read_account_details, update_account
from src.components.tools.reporting import get_ibkr_details
import json

# Set up a flow to check which accounts are missing details
# (Check also clients file to see which clients dont have master account and advisor and fee template set up)

accounts = read_accounts({})
details = get_ibkr_details()

with open("details_backup.json", "w") as f:
    json.dump(details, f, indent=2)

detail_account_ids = {
    detail["account"]["accountId"]
    for detail in details
    if detail.get("account") and detail["account"].get("accountId")
}

missing_accounts = [
    account
    for account in accounts
    if account.get("ibkr_account_number") not in detail_account_ids
]

for account in missing_accounts:
    account_id = account["ibkr_account_number"]
    master_account = account["master_account"]
    try:
        if master_account is not None:
            new_details = read_account_details(account_id=account_id, master_account=master_account)
            details.append(new_details)
    except Exception as e:
        print(f"Error fetching details for account {account_id}: {e}")
        continue

with open("details.json", "w") as f:
    json.dump(details, f, indent=2)
