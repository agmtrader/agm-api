
from src.utils.exception import handle_exception
from src.components.tools.reporting import get_nav_report, get_clients_report
import pandas as pd
from src.utils.logger import logger

@handle_exception
def send_unfunded_emails():
    """
    Cross-references the NAV report with the accounts table to find
    accounts that have zero NAV (not funded).
    """
    from src.components.entities.accounts import read_accounts
    from src.components.entities.contacts import read_contacts
    from src.components.entities.advisors import read_advisors
    from src.components.tools.email import Gmail

    email = Gmail()

    # Base data
    nav_data = get_nav_report()
    accounts_data = read_accounts({})

    # Extract clients report for account status and date opened information
    clients_data = get_clients_report()

    # Extract contacts and advisors data to get email addresses
    contacts_data = read_contacts({})
    advisors_data = read_advisors({})
    
    nav_df = pd.DataFrame(nav_data)
    accounts_df = pd.DataFrame(accounts_data)
    clients_df = pd.DataFrame(clients_data)
    contacts_df = pd.DataFrame(contacts_data)
    advisors_df = pd.DataFrame(advisors_data)

    no_nav_df = nav_df[nav_df['Total'] == 0]

    # Save all accounts that have no NAV or dont even appear in the NAV report
    accounts_not_in_nav = accounts_df[~accounts_df['ibkr_account_number'].isin(nav_df['ClientAccountID'])]
    accounts_with_no_nav = accounts_df[accounts_df['ibkr_account_number'].isin(no_nav_df['ClientAccountID'])]
    
    total_accounts = pd.concat([accounts_not_in_nav, accounts_with_no_nav])

    # Filter for only accounts that have Status Open in clients
    clients_with_open_status = clients_df[clients_df['Status'] == 'Open']
    total_accounts = total_accounts[total_accounts['ibkr_account_number'].isin(clients_with_open_status['Account ID'])]

    # Parse date opened to enrich email context fields
    clients_df['Date Opened'] = pd.to_datetime(clients_df['Date Opened'], errors='coerce')

    # Merge Date Opened into total_accounts
    total_accounts = total_accounts.merge(clients_df[['Account ID', 'Date Opened']], left_on='ibkr_account_number', right_on='Account ID', how='left')

    today = pd.Timestamp.now().normalize()
    total_accounts['business_days_since_date_opened'] = total_accounts['Date Opened'].apply(
        lambda date_opened: pd.NA
        if pd.isna(date_opened)
        else max(0, len(pd.bdate_range(start=date_opened.normalize(), end=today)) - 1)
    )
    total_accounts['notice_number'] = total_accounts['business_days_since_date_opened'].apply(
        lambda business_days: pd.NA
        if pd.isna(business_days)
        else max(1, int((business_days + 4) // 5))
    )

    advisor_emails_df = advisors_df[['code', 'contact_id']].merge(
        contacts_df[['id', 'email']],
        left_on='contact_id',
        right_on='id',
        how='left'
    ).rename(columns={'email': 'advisor_email'})

    total_accounts = total_accounts.merge(
        advisor_emails_df[['code', 'advisor_email']],
        left_on='advisor_code',
        right_on='code',
        how='left'
    )

    contacts_to_email = total_accounts.merge(contacts_df, left_on='contact_id', right_on='id', how='left')
    contacts_to_email = contacts_to_email[['ibkr_account_number', 'email', 'name', 'advisor_email', 'business_days_since_date_opened', 'notice_number']]

    for contact in contacts_to_email.to_dict(orient='records')[1:]:
        client_email = contact.get('email')
        if pd.isna(client_email) or not isinstance(client_email, str) or not client_email.strip():
            logger.info(
                f"Skipping funding notification for account {contact.get('ibkr_account_number')} due to invalid email: {client_email!r}"
            )
            continue

        advisor_email = contact.get('advisor_email')
        if pd.isna(advisor_email) or not isinstance(advisor_email, str) or not advisor_email.strip():
            advisor_email = ''
        else:
            advisor_email = advisor_email.strip()

        email.send_funding_notification_email(
            content={},
            client_email=client_email.strip(),
            lang='es',
            cc=advisor_email,
            days_since_opened=contact['business_days_since_date_opened'],
            notice_number=contact['notice_number']
        )
    
    return contacts_to_email.to_dict(orient='records')
    
@handle_exception
def update_account_aliases():
    """Fetch clients report, filter accounts without alias, update each alias, and return list."""
    from src.components.tools.reporting import get_clients_report
    from src.components.entities.accounts import update_account_alias
    clients = get_clients_report()
    pending_accounts = [c for c in clients if (c.get('Alias') in (None, '')) and c.get('Status') not in ('Rejected', 'Closed', 'Funded Pending')]
    updated_accounts = []
    for account in pending_accounts:
        account_id = account.get('Account ID')
        title = account.get('Title')
        old_alias = account.get('Alias')
        master_account = account.get('Master Account')
        if account_id and title is not None:
            if master_account:
                new_alias = f"{account_id} {title}"
                try:
                    # Reuse existing helper to update alias via IBKR API
                    update_account_alias(account_id=account_id, new_alias=new_alias, master_account=master_account)
                    updated_accounts.append({
                        'account_id': account_id,
                        'old_alias': old_alias,
                        'new_alias': new_alias
                    })
                    logger.success(f"Updated alias for {account_id}: {old_alias} -> {new_alias}")
                except Exception as e:
                    logger.error(f"Failed to update alias for {account_id}: {e}")
    return {
        'updated': len(updated_accounts),
        'accounts': updated_accounts
    }
