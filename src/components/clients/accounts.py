from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.components.tools.public.reporting import get_clients_report, get_nav_report, get_ibkr_details
from sqlalchemy import text
import os
import re
import time
import uuid

logger.announcement('Initializing Accounts Service', type='info')
ibkr_web_api = IBKRWebAPI()
logger.announcement('Initialized Accounts Service', type='success')

_ACCOUNTS_METADATA_CACHE = {}
_ACCOUNTS_METADATA_CACHE_TTL_SECONDS = int(os.getenv('ACCOUNTS_METADATA_CACHE_TTL_SECONDS', '120'))

table = 'account'
account_contact_table = 'account_contact'
SENSITIVE_ACCOUNT_FIELDS = {
    'ibkr_password_secret_id',
    'temporal_password_secret_id',
}

def _sanitize_account(account: dict = None):
    if account is None:
        return None
    return {key: value for key, value in account.items() if key not in SENSITIVE_ACCOUNT_FIELDS}


def _sanitize_accounts(accounts: list = None):
    return [_sanitize_account(account) for account in accounts or []]


def _vault_secret_name(account_id: str, field: str) -> str:
    return f'account:{account_id}:{field}'


def _create_vault_secret(secret: str, name: str, description: str) -> str | None:
    if secret is None or secret == '':
        return None

    @db.with_session
    def _create_secret(session, secret: str, name: str, description: str):
        result = session.execute(
            text('select vault.create_secret(:secret, :name, :description) as id'),
            {'secret': secret, 'name': name, 'description': description},
        ).mappings().first()
        if not result or not result.get('id'):
            raise Exception('Vault did not return a secret id')
        return str(result['id'])

    return _create_secret(secret, name, description)


def _read_vault_secret(secret_id: str | None) -> str | None:
    if not secret_id:
        return None

    @db.with_session
    def _read_secret(session, secret_id: str):
        result = session.execute(
            text('select decrypted_secret from vault.decrypted_secrets where id = :secret_id'),
            {'secret_id': secret_id},
        ).mappings().first()
        return result.get('decrypted_secret') if result else None

    return _read_secret(str(secret_id))


def _prepare_account_secrets(account: dict, account_id: str | None = None) -> dict:
    if account is None:
        return account

    prepared = dict(account)
    prepared.pop('ibkr_password_secret_id', None)
    prepared.pop('temporal_password_secret_id', None)
    secret_fields = {
        'ibkr_password': 'ibkr_password_secret_id',
        'temporal_password': 'temporal_password_secret_id',
    }

    for password_field, secret_id_field in secret_fields.items():
        if password_field not in prepared:
            continue

        password_value = prepared.pop(password_field)
        prepared[secret_id_field] = None

        if password_value is None or password_value == '':
            continue

        if not account_id:
            raise Exception(f'account_id is required to store {password_field} in Vault')

        prepared[secret_id_field] = _create_vault_secret(
            password_value,
            _vault_secret_name(account_id, password_field),
            f'{password_field} for account {account_id}',
        )

    return prepared


def _resolve_account_secret(account: dict, secret_id_field: str) -> str | None:
    secret = _read_vault_secret(account.get(secret_id_field))
    if secret:
        return secret

    return None

def _normalize_join_key(value) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _get_cached_payload(cache_key: str, producer, force_refresh: bool = False):
    if force_refresh or _ACCOUNTS_METADATA_CACHE_TTL_SECONDS <= 0:
        return producer()

    cache_entry = _ACCOUNTS_METADATA_CACHE.get(cache_key)
    now = time.time()

    if cache_entry and (now - cache_entry['timestamp'] <= _ACCOUNTS_METADATA_CACHE_TTL_SECONDS):
        return cache_entry['value']

    value = producer()
    _ACCOUNTS_METADATA_CACHE[cache_key] = {
        'timestamp': now,
        'value': value
    }
    return value


def _parse_fee_template_summary(fee_template) -> str:
    if not isinstance(fee_template, dict):
        return '-'

    fee_template_name = str(fee_template.get('feeTemplateName', '')).strip()
    fee_info = str(fee_template.get('feeInfo', '')).strip()
    fee_strategy = str(fee_template.get('feeStrategy', '')).strip()
    broker_fee_info = str(fee_template.get('brokerFeeInfo', '')).strip()
    fee_effective_date = str(fee_template.get('feeEffectiveDate', '')).strip()

    parts = []

    if fee_template_name:
        parts.append(fee_template_name)
    elif fee_strategy:
        if re.search(r'automated', fee_strategy, re.IGNORECASE) and re.search(r'wrap fee', fee_strategy, re.IGNORECASE):
            parts.append('Automated Wrap Fee')
        else:
            parts.append(fee_strategy.split('.')[0].strip() or 'Custom Fee Strategy')

    percent_match = re.search(r'Percent of Equity:\s*([0-9]+(?:\.[0-9]+)?)', fee_info, re.IGNORECASE)
    if percent_match and percent_match.group(1):
        parts.append(f'{percent_match.group(1)}% Equity')
    elif fee_info:
        compact_fee_info = re.sub(r'\s+', ' ', fee_info).strip()
        parts.append(compact_fee_info[:80])

    if broker_fee_info:
        if re.search(r'no commission schedule has been defined', broker_fee_info, re.IGNORECASE):
            parts.append('IB Default Commissions')
        else:
            asset_classes = []
            for segment in broker_fee_info.split('||'):
                segment = segment.strip()
                match = re.match(r'^([A-Z]{2,10})\s+by\b', segment, re.IGNORECASE)
                if match:
                    asset_class = match.group(1).upper()
                    if asset_class not in asset_classes:
                        asset_classes.append(asset_class)
            if asset_classes:
                parts.append(f"{', '.join(asset_classes)} Commissions")
            else:
                parts.append('Custom Commission Schedule')

    if fee_effective_date:
        parts.append(f'Eff. {fee_effective_date}')

    return ' | '.join(parts) if parts else '-'


@handle_exception
def create_account(account: dict = None) -> dict:
    logger.info(f"Attempting to create account with data: {account}")
    pending_secrets = {}
    account_data = dict(account or {})
    account_data.pop('ibkr_password_secret_id', None)
    account_data.pop('temporal_password_secret_id', None)
    for password_field in ('ibkr_password', 'temporal_password'):
        if password_field in account_data:
            pending_secrets[password_field] = account_data.pop(password_field)

    account_id = db.create(table=table, data=account_data)

    if pending_secrets:
        secret_update = _prepare_account_secrets(pending_secrets, account_id=account_id)
        if secret_update:
            db.update(table=table, query={'id': account_id}, data=secret_update)

    return {'id': account_id}

@handle_exception
def create_instruction(account_id: str = None) -> dict:
    return db.create(table='account_instruction', data={'account_id': account_id})

@handle_exception
def read_accounts(query: dict = None) -> list:
    accounts = db.read(table=table, query=query)
    return _sanitize_accounts(accounts)

@handle_exception
def read_account_contacts_and_screenings(account_id: str = None) -> dict:
    if not account_id:
        raise Exception('account_id is required')

    links = db.read(table=account_contact_table, query={'account_id': account_id}) or []
    contact_ids = {
        str(link.get('contact_id')).strip()
        for link in links
        if link.get('contact_id')
    }

    if len(contact_ids) == 0:
        return {
            'account_contacts': links,
            'contacts': [],
            'screenings_by_contact_id': {}
        }

    contacts = []
    for contact_id in contact_ids:
        contact_rows = db.read(table='contact', query={'id': contact_id}) or []
        if contact_rows:
            contacts.append(contact_rows[0])

    screenings_by_contact_id = {}
    for contact_id in contact_ids:
        screenings = db.read(table='contact_screening', query={'contact_id': contact_id}) or []
        screenings_by_contact_id[contact_id] = screenings

    for contact_id, screenings in screenings_by_contact_id.items():
        screenings_by_contact_id[contact_id] = sorted(
            screenings,
            key=lambda screening: str(screening.get('created') or ''),
            reverse=True
        )

    return {
        'account_contacts': links,
        'contacts': contacts,
        'screenings_by_contact_id': screenings_by_contact_id
    }

@handle_exception
def read_accounts_with_metadata(query: dict = None, include_advisor: bool = False, force_refresh: bool = False) -> list:
    accounts = read_accounts(query=query or {})

    clients = _get_cached_payload('clients_report', get_clients_report, force_refresh=force_refresh)
    nav_report = _get_cached_payload('nav_report', get_nav_report, force_refresh=force_refresh)
    ibkr_details = _get_cached_payload('ibkr_details', get_ibkr_details, force_refresh=force_refresh)
    account_contact_rows = db.read(table=account_contact_table, query={}) or []
    all_contacts = db.read(table='contact', query={}) or []
    contacts_by_id = {c.get('id'): c for c in all_contacts if c.get('id')}
    account_contacts_by_account_id = {}
    for row in account_contact_rows:
        account_id = row.get('account_id')
        if not account_id:
            continue
        if account_id not in account_contacts_by_account_id:
            account_contacts_by_account_id[account_id] = []
        account_contacts_by_account_id[account_id].append(row)

    advisors = []
    advisor_name_by_code = {}
    if include_advisor:
        from src.components.clients.advisors import read_advisors
        advisors = read_advisors({})
        advisor_name_by_code = {
            _normalize_join_key(advisor.get('code')): advisor.get('name')
            for advisor in advisors
        }

    client_map = {}
    for client in clients or []:
        key = _normalize_join_key(client.get('Account ID'))
        if key and key not in client_map:
            client_map[key] = client

    nav_map = {}
    for nav in nav_report or []:
        key = _normalize_join_key(nav.get('ClientAccountID'))
        if key and key not in nav_map:
            nav_map[key] = nav

    fee_template_summary_by_account_id = {}
    for row in ibkr_details if isinstance(ibkr_details, list) else []:
        details_list = row.get('ibkrdetails') if isinstance(row, dict) else []
        if not isinstance(details_list, list):
            details_list = [row]

        for details in details_list:
            if not isinstance(details, dict):
                continue
            account_data = details.get('account')
            if not isinstance(account_data, dict):
                continue
            account_id = _normalize_join_key(account_data.get('accountId'))
            summary = _parse_fee_template_summary(account_data.get('feeTemplate'))
            if account_id and summary != '-' and account_id not in fee_template_summary_by_account_id:
                fee_template_summary_by_account_id[account_id] = summary

    enriched_accounts = []
    for account in accounts or []:
        account_id = _normalize_join_key(account.get('ibkr_account_number'))
        client = client_map.get(account_id)
        nav = nav_map.get(account_id)
        advisor_name = advisor_name_by_code.get(_normalize_join_key(account.get('advisor_code')), '-') if include_advisor else None

        account_enriched = {
            **account,
            'alias': client.get('Alias') if client and client.get('Alias') else '-',
            'status': client.get('Status') if client and client.get('Status') else '-',
            'nav': nav.get('Total') if nav and nav.get('Total') is not None else 0,
            'master_account_id': client.get('sheet_name') if client and client.get('sheet_name') else '-',
            'title': client.get('Title') if client and client.get('Title') else '-',
            'sls_devices': client.get('SLS Devices') if client and client.get('SLS Devices') else '-',
            'client_email_address': client.get('Email Address') if client and client.get('Email Address') else '-',
            'fee_template_summary': fee_template_summary_by_account_id.get(account_id, '-'),
            'account_contacts': account_contacts_by_account_id.get(account.get('id'), []),
            'contacts': [
                contacts_by_id.get(link.get('contact_id'))
                for link in account_contacts_by_account_id.get(account.get('id'), [])
                if link.get('contact_id') in contacts_by_id
            ],
        }

        if include_advisor:
            account_enriched['advisor_name'] = advisor_name

        enriched_accounts.append(account_enriched)

    return sorted(enriched_accounts, key=lambda account: str(account.get('created', '')), reverse=True)

@handle_exception
def read_instructions(query: dict = None) -> list:
    instructions = db.read(table='account_instruction', query=query)
    return instructions

@handle_exception
def update_account(query: dict = None, account: dict = None) -> dict:
    existing_accounts = db.read(table=table, query=query) or []
    if len(existing_accounts) == 0:
        raise Exception('Account not found')
    if len(existing_accounts) > 1:
        raise Exception('Multiple accounts found')

    prepared_account = _prepare_account_secrets(account, account_id=existing_accounts[0].get('id'))
    db.update(table=table, query=query, data=prepared_account)
    return {'status': 'success'}

@handle_exception
def send_account_credentials_email(
    account_id: str = None,
    client_email: str = None,
    lang: str = 'es',
    cc: str = '',
    send_welcome: bool = False,
    client_name: str = '',
) -> dict:
    if not account_id:
        raise Exception('Missing account_id')
    if not client_email:
        raise Exception('Missing client_email')

    accounts = db.read(table=table, query={'id': account_id}) or []
    if len(accounts) == 0:
        raise Exception('Account not found')
    if len(accounts) > 1:
        raise Exception('Multiple accounts found')

    account = accounts[0]
    username = account.get('ibkr_username')
    password = _resolve_account_secret(account, 'ibkr_password_secret_id')
    if not username or not password:
        raise Exception('Account credentials not found')

    from src.components.tools.public.email import Gmail
    email_service = Gmail()
    email_service.send_credentials_email(
        content={'username': username, 'password': password},
        client_email=client_email,
        lang=lang,
        cc=cc,
    )

    if send_welcome:
        email_service.send_welcome_email(
            content={'client_name': client_name or 'Client'},
            client_email=client_email,
            lang=lang,
        )
        db.update(table=table, query={'id': account_id}, data={'emailed_credentials': True})

    return {'status': 'success'}

@handle_exception
def send_to_ibkr(account_id: str = None, master_account: str = None, application: dict = None) -> dict:
    if not account_id:
        raise Exception('Missing account_id')

    accounts = db.read(table=table, query={'id': account_id}) or []
    if len(accounts) == 0:
        raise Exception(f'Account not found for id={account_id}')
    if len(accounts) > 1:
        raise Exception(f'Multiple accounts found for id={account_id}')

    account = accounts[0]
    application_json = application if isinstance(application, dict) else account.get('application_json')
    if not application_json:
        raise Exception('Account has no application_json')

    resolved_master_account = master_account or account.get('master_account')
    if not resolved_master_account:
        raise Exception('Missing master_account (payload and account row are empty)')

    return ibkr_web_api.send_to_ibkr(
        application={'application': application_json},
        master_account=resolved_master_account
    )


"""
Account Management API
"""
@handle_exception
def list_accounts(master_account: str = None) -> dict:
    return ibkr_web_api.list_accounts(master_account=master_account)

@handle_exception
def read_account_details(account_id: str = None, master_account: str = None) -> dict:
    return ibkr_web_api.get_account_details(account_id=account_id, master_account=master_account)

@handle_exception
def get_pending_tasks(account_id: str = None, master_account: str = None) -> list:
    return ibkr_web_api.get_pending_tasks(account_id=account_id, master_account=master_account)

@handle_exception
def get_registration_tasks(account_id: str = None, master_account: str = None) -> list:
    return ibkr_web_api.get_registration_tasks(account_id=account_id, master_account=master_account)

@handle_exception
def get_account_statements(account_id: str = None, start_date: str = None, end_date: str = None, master_account: str = None) -> dict:
    """Get account statements via IBKR API."""
    print(account_id, start_date, end_date, master_account)
    return ibkr_web_api.get_account_statements(account_id=account_id, start_date=start_date, end_date=end_date, master_account=master_account)

@handle_exception
def get_available_statements(account_id: str = None, master_account: str = None) -> dict:
    """Get available statements via IBKR API."""
    return ibkr_web_api.get_available_statements(account_id=account_id, master_account=master_account)

@handle_exception
def submit_documents(document_submission: dict = None, master_account: str = None) -> dict:
    return ibkr_web_api.submit_documents(document_submission=document_submission, master_account=master_account)

@handle_exception
def submit_all_agreements(master_account: str = None, forms: list = None) -> dict:
    return ibkr_web_api.submit_all_agreements(master_account=master_account or 'I6413690', forms=forms)

@handle_exception
def apply_fee_template(account_id: str = None, template_name: str = None, master_account: str = None) -> dict:
    """Apply a fee template to an account via IBKR API."""
    return ibkr_web_api.apply_fee_template(account_id=account_id, template_name=template_name, master_account=master_account)

@handle_exception
def add_trading_permissions(account_id: str = None, trading_permissions: list = None, master_account: str = None) -> dict:
    """Add trading permissions to an account via IBKR API."""
    return ibkr_web_api.add_trading_permissions(account_id=account_id, trading_permissions=trading_permissions, master_account=master_account)

@handle_exception
def add_clp_capability(account_id: str = None, document_submission: dict = None, master_account: str = None) -> dict:
    """Add CLP capability to an account via IBKR API."""
    return ibkr_web_api.add_clp_capability(account_id=account_id, document_submission=document_submission, master_account=master_account)

@handle_exception
def close_account(account_id: str = None, close_reason: str = None, master_account: str = None) -> dict:
    """Close account via IBKR API."""
    return ibkr_web_api.close_account(
        account_id=account_id,
        close_reason=close_reason,
        master_account=master_account
    )

@handle_exception
def update_account_alias(account_id: str = None, new_alias: str = None, master_account: str = None) -> dict:
    """Update account alias via IBKR API."""
    return ibkr_web_api.update_account_alias(account_id=account_id, new_alias=new_alias, master_account=master_account)

@handle_exception
def update_account_email(reference_user_name: str = None, new_email: str = None, access: bool = True, master_account: str = None) -> dict:
    """Update account email via IBKR API."""
    return ibkr_web_api.update_account_email(reference_user_name=reference_user_name, new_email=new_email, access=access, master_account=master_account)

@handle_exception
def change_financial_information(account_id: str = None, new_financial_information: dict = None, master_account: str = None) -> dict:
    """Change account financial information via IBKR API."""
    return ibkr_web_api.change_financial_information(
        account_id=account_id,
        new_financial_information=new_financial_information,
        master_account=master_account
    )

@handle_exception
def change_account_holder_external_id(account_id: str = None, id: str = None, master_account: str = None) -> dict:
    """Change account holder external id via IBKR API using IBKR id (entityId)."""
    external_id = str(uuid.uuid4())
    return ibkr_web_api.change_account_holder_external_id(
        accountId=account_id,
        entityId=id,
        external_id=external_id,
        master_account=master_account
    )

@handle_exception
def change_investment_experience(account_id: str = None, investment_experience: dict = None, master_account: str = None) -> dict:
    """Backward-compatible wrapper for legacy callers."""
    return change_financial_information(
        account_id=account_id,
        new_financial_information={"investmentExperience": investment_experience} if investment_experience else {},
        master_account=master_account
    )

@handle_exception
def deposit_funds(master_account: str = None, instruction: dict = None, account_id: str = None) -> dict:
    """Deposit funds via IBKR API."""
    client_instruction_id = db.create(table='account_instruction', data={'account_id': account_id})
    instruction['clientInstructionId'] = client_instruction_id
    return ibkr_web_api.deposit_funds(master_account=master_account, instruction=instruction)

@handle_exception
def withdraw_funds(master_account: str = None, instruction: dict = None, account_id: str = None) -> dict:
    """Withdraw funds via IBKR API."""
    client_instruction_id = db.create(table='account_instruction', data={'account_id': account_id})
    instruction['clientInstructionId'] = client_instruction_id
    return ibkr_web_api.withdraw_funds(master_account=master_account, instruction=instruction)

@handle_exception
def transfer_position_internally(source_account_id: str = None, target_account_id: str = None, position: int = None, transfer_quantity: int = None, master_account: str = None) -> dict:
    return ibkr_web_api.transfer_position_internally(source_account_id=source_account_id, target_account_id=target_account_id, position=position, transfer_quantity=transfer_quantity, master_account=master_account)

@handle_exception
def transfer_position_externally(account_id: str = None, client_instruction_id: int = None, contra_broker_account_id: str = None, contra_broker_dtc_code: str = None, quantity: int = None, conid: int = None, master_account: str = None) -> dict:
    return ibkr_web_api.transfer_position_externally(account_id=account_id, client_instruction_id=client_instruction_id, contra_broker_account_id=contra_broker_account_id, contra_broker_dtc_code=contra_broker_dtc_code, quantity=quantity, conid=conid, master_account=master_account)

@handle_exception
def get_status_of_instruction(client_instruction_id: str = None) -> dict:
    """Get the status of a banking instruction via IBKR API."""
    return ibkr_web_api.get_status_of_instruction(client_instruction_id=client_instruction_id)
    
@handle_exception
def view_withdrawable_cash(master_account: str = None, account_id: str = None, client_instruction_id: str = None) -> dict:
    """View the withdrawable cash for the given account via IBKR API."""
    return ibkr_web_api.view_withdrawable_cash(master_account=master_account, account_id=account_id, client_instruction_id=client_instruction_id)

@handle_exception
def view_active_bank_instructions(master_account: str = None, account_id: str = None, client_instruction_id: str = None, bank_instruction_method: str = None) -> dict:
    """View the active bank instructions for the given account via IBKR API."""
    return ibkr_web_api.view_active_bank_instructions(master_account=master_account, account_id=account_id, client_instruction_id=client_instruction_id, bank_instruction_method=bank_instruction_method)

@handle_exception
def get_wire_instructions(master_account: str = None, account_id: str = None, currency: str = "USD") -> dict:
    """Get wire instructions via IBKR API."""
    return ibkr_web_api.get_wire_instructions(master_account=master_account, account_id=account_id, currency=currency)

# Enums
@handle_exception
def get_product_country_bundles() -> dict:
    """Get product country bundles enumeration via IBKR API."""
    return ibkr_web_api.get_product_country_bundles()

@handle_exception
def get_forms(forms: list = None, master_account: str = None) -> dict:
    return ibkr_web_api.get_forms(forms=forms, master_account=master_account)

@handle_exception
def get_financial_ranges() -> dict:
    """Get financial ranges via IBKR API."""
    return ibkr_web_api.get_financial_ranges()

@handle_exception
def get_business_and_occupation() -> dict:
    """Get business and occupation via IBKR API."""
    return ibkr_web_api.get_business_and_occupation()

# Trading API
@handle_exception
def create_sso_session(credential: str = None, ip: str = None) -> dict:
    """Create an SSO session via IBKR API."""
    return ibkr_web_api.create_sso_session(credential=credential, ip=ip)

@handle_exception
def initialize_brokerage_session() -> dict:
    """Initialize a brokerage session via IBKR API."""
    return ibkr_web_api.initialize_brokerage_session()

@handle_exception
def logout_of_brokerage_session() -> dict:
    """Logout of a brokerage session via IBKR API."""
    return ibkr_web_api.logout_of_brokerage_session()

@handle_exception
def get_brokerage_accounts() -> dict:
    """Get brokerage accounts via IBKR API."""
    return ibkr_web_api.get_brokerage_accounts()

@handle_exception
def get_portfolio_analyst_performance(acct_ids: list = None, freq: str = None) -> dict:
    """Get Portfolio Analyst performance via IBKR API."""
    return ibkr_web_api.get_portfolio_analyst_performance(acct_ids=acct_ids, freq=freq)
