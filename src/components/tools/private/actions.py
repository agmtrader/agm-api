
from datetime import date, datetime
from src.utils.exception import handle_exception
from src.components.tools.public.reporting import (
    get_nav_report,
    get_clients_report,
    get_ofac_sdn_list,
    get_uk_sanctions_list,
    get_un_sanctions_list,
    compare_all_sanctions_today_vs_yesterday,
)
from src.components.clients.contacts import create_contact_screening_from_contact_id
from src.utils.connectors.supabase import db
import pandas as pd
from src.utils.logger import logger
from src.utils.connectors.gmail import GmailConnector


def _screen_created_date(value):
    try:
        return datetime.strptime(str(value), '%Y%m%d%H%M%S').date()
    except (TypeError, ValueError):
        return None


@handle_exception
def run_screenings(apply_screenings: bool = True) -> dict:
    """Screen every contact linked to an account using one loaded set of lists."""
    comparison = compare_all_sanctions_today_vs_yesterday() or {}
    if comparison.get('all_available') and comparison.get('all_same'):
        return {
            'apply_screenings': apply_screenings,
            'screenings_skipped': True,
            'skip_reason': 'OFAC, UK, and UN sanctions lists unchanged vs yesterday',
            'contacts_targeted': 0,
            'screenings_executed': 0,
            'screening_errors': [],
        }

    sanctions_lists = (
        get_ofac_sdn_list() or [],
        get_uk_sanctions_list() or [],
        get_un_sanctions_list() or [],
    )
    links = db.read(table='account_contact', query={}) or []
    screenings = db.read(table='contact_screening', query={}) or []
    today = date.today()
    screened_today = {
        row.get('contact_id')
        for row in screenings
        if row.get('contact_id')
        and _screen_created_date(row.get('created')) == today
    }
    contact_ids = list(dict.fromkeys(
        link.get('contact_id') for link in links
        if link.get('account_id') and link.get('contact_id')
    ))
    result = {
        'apply_screenings': apply_screenings,
        'screenings_skipped': False,
        'contacts_targeted': len(contact_ids),
        'screenings_executed': 0,
        'screening_errors': [],
    }
    contact_ids = [contact_id for contact_id in contact_ids if contact_id not in screened_today]
    result['contacts_targeted'] = len(contact_ids)
    if not apply_screenings:
        return result

    for contact_id in contact_ids:
        try:
            create_contact_screening_from_contact_id(
                contact_id=contact_id,
                sanctions_lists=sanctions_lists,
            )
            result['screenings_executed'] += 1
        except Exception as error:
            result['screening_errors'].append(f'{contact_id}: {error}')
    return result

@handle_exception
def send_unfunded_emails():
    """
    Cross-references the NAV report with the accounts table to find
    accounts that have zero NAV (not funded).
    """
    from src.components.clients.accounts import read_accounts
    from src.components.clients.accounts import read_account_contacts
    from src.components.clients.contacts import read_contacts
    from src.components.clients.advisors import read_advisors
    from src.components.clients.accounts import send_funding_notification_email

    # Base data
    nav_data = get_nav_report()
    accounts_data = read_accounts({})
    account_contacts_data = read_account_contacts({})

    # Extract clients report for account status and date opened information
    clients_data = get_clients_report()

    # Extract contacts and advisors data to get email addresses
    contacts_data = read_contacts({})
    advisors_data = read_advisors({})
    
    nav_df = pd.DataFrame(nav_data)
    accounts_df = pd.DataFrame(accounts_data)
    account_contacts_df = pd.DataFrame(account_contacts_data)
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

    if 'contact_id' not in total_accounts.columns:
        latest_account_contacts_df = pd.DataFrame(columns=['account_id', 'contact_id'])
        if not account_contacts_df.empty:
            sortable_account_contacts_df = account_contacts_df.copy()
            sortable_account_contacts_df['_sort_key'] = sortable_account_contacts_df.apply(
                lambda row: str(row.get('updated') or row.get('created') or ''),
                axis=1,
            )
            latest_account_contacts_df = (
                sortable_account_contacts_df
                .sort_values('_sort_key', ascending=False)
                .drop_duplicates(subset=['account_id'], keep='first')[['account_id', 'contact_id']]
            )

        total_accounts = total_accounts.merge(
            latest_account_contacts_df,
            left_on='id',
            right_on='account_id',
            how='left'
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

        send_funding_notification_email(
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
    from src.components.tools.public.reporting import get_clients_report
    from src.components.clients.accounts import update_account_alias

    def _is_blank(value):
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ''
        return pd.isna(value)

    clients = get_clients_report()
    pending_accounts = [
        c for c in clients
        if _is_blank(c.get('Alias')) and c.get('Status') not in ('Rejected', 'Closed', 'Funded Pending')
    ]
    updated_accounts = []
    failed_accounts = []
    skipped_accounts = []

    for account in pending_accounts:
        account_id = str(account.get('Account ID') or '').strip()
        title = str(account.get('Title') or '').strip()
        old_alias = account.get('Alias')
        master_account = account.get('Master Account') or None

        if not account_id or not title:
            skipped_accounts.append({
                'account_id': account_id or None,
                'old_alias': old_alias,
                'master_account': master_account,
                'reason': 'Missing Account ID or Title'
            })
            logger.warning(f"Skipping alias update for account_id={account_id!r} title={title!r}")
            continue

        new_alias = f"{account_id} {title}"
        try:
            # When master_account is missing, the IBKR client falls back to the default credentials.
            update_account_alias(
                account_id=account_id,
                new_alias=new_alias,
                master_account=master_account
            )
            updated_accounts.append({
                'account_id': account_id,
                'old_alias': old_alias,
                'new_alias': new_alias,
                'master_account': master_account
            })
            logger.success(f"Updated alias for {account_id}: {old_alias} -> {new_alias}")
        except Exception as e:
            failed_accounts.append({
                'account_id': account_id,
                'old_alias': old_alias,
                'new_alias': new_alias,
                'master_account': master_account,
                'error': str(e)
            })
            logger.error(f"Failed to update alias for {account_id}: {e}")

    return {
        'pending': len(pending_accounts),
        'updated': len(updated_accounts),
        'failed': len(failed_accounts),
        'skipped': len(skipped_accounts),
        'accounts': updated_accounts,
        'failed_accounts': failed_accounts,
        'skipped_accounts': skipped_accounts
    }

@handle_exception
def send_compliance_manual_update_email():

    gmail = GmailConnector()
    message = gmail.send_email(
        {},
        "aa@agmtechnology.com,cr@agmtechnology.com,hc@agmtechnology.com,as@agmtechnology.com",
        'Compliance Manual Update Requires Review',
        'compliance_manual_update',
        bcc='',
        cc='',
    )
    return {
        "status": "sent",
        "recipient": "aa@agmtechnology.com,cr@agmtechnology.com,hc@agmtechnology.com,as@agmtechnology.com",
        "message": message,
    }
