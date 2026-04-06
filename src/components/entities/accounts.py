from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.managers.document_manager import DocumentManager
import pandas as pd
import difflib
from src.components.tools.reporting import get_ofac_sdn_list, get_uk_sanctions_list
from src.components.tools.reporting import get_clients_report, get_nav_report, get_ibkr_details
import os
import re
import time

logger.announcement('Initializing Accounts Service', type='info')
ibkr_web_api = IBKRWebAPI()
document_manager = DocumentManager()
logger.announcement('Initialized Accounts Service', type='success')

_ACCOUNTS_METADATA_CACHE = {}
_ACCOUNTS_METADATA_CACHE_TTL_SECONDS = int(os.getenv('ACCOUNTS_METADATA_CACHE_TTL_SECONDS', '120'))


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
    account_id = db.create(table='account', data=account)
    return {'id': account_id}

@handle_exception
def create_instruction(account_id: str = None) -> dict:
    return db.create(table='account_instruction', data={'account_id': account_id})

@handle_exception
def read_accounts(query: dict = None) -> list:
    accounts = db.read(table='account', query=query)
    return accounts


@handle_exception
def read_accounts_with_metadata(query: dict = None, include_advisor: bool = False, force_refresh: bool = False) -> list:
    accounts = read_accounts(query=query or {})

    clients = _get_cached_payload('clients_report', get_clients_report, force_refresh=force_refresh)
    nav_report = _get_cached_payload('nav_report', get_nav_report, force_refresh=force_refresh)
    ibkr_details = _get_cached_payload('ibkr_details', get_ibkr_details, force_refresh=force_refresh)

    advisors = []
    advisor_name_by_code = {}
    if include_advisor:
        from src.components.entities.advisors import read_advisors
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
    logger.info(f"Attempting to update account with query: {query} and data: {account}")
    db.update(table='account', query=query, data=account)
    return {'status': 'success'}

@handle_exception
def upload_document(account_id: str = None, file_name: str = None, file_length: int = None, sha1_checksum: str = None, mime_type: str = None, data: str = None, category: str = None, type: str = None, issued_date: str = None, expiry_date: str = None, name: str = None) -> dict:
    logger.info(f"Uploading document: {file_name} to account: {account_id}, file_length: {file_length}, sha1_checksum: {sha1_checksum}, mime_type: {mime_type}, data: {data}")
    return document_manager.upload_document(account_id=account_id, file_name=file_name, file_length=file_length, sha1_checksum=sha1_checksum, mime_type=mime_type, data=data, category=category, type=type, issued_date=issued_date, expiry_date=expiry_date, name=name)

@handle_exception
def read_account_documents(account_id: str = None) -> list:
    """
    Read all documents for an account
    Args:
        account_id: The ID of the account to read documents for
    Returns:
        A list of documents for the account
    """
    account_documents = db.read(table='account_document', query={'account_id': account_id})
    documents = []
    for account_document in account_documents:
        document = db.read(table='document', query={'id': account_document['document_id']})
        for d in document:
            documents.append(d)
    return documents, account_documents

@handle_exception
def update_account_document(document_id: str = None, category: str = None, name: str = None, type: str = None, issued_date: str = None, expiry_date: str = None, comment: str = None) -> dict:
    logger.info(f"Updating account document: {document_id}, category: {category}, name: {name}, type: {type}, issued_date: {issued_date}, expiry_date: {expiry_date}, comment: {comment}")
    return db.update(table='account_document', query={'document_id': document_id}, data={
        'category': category,
        'name': name,
        'type': type,
        'issued_date': issued_date,
        'expiry_date': expiry_date,
        'comment': comment
    })

@handle_exception
def delete_document(document_id: str = None) -> dict:
    db.delete(table='account_document', query={'document_id': document_id})
    db.delete(table='document', query={'id': document_id})
    return {'status': 'success'}

@handle_exception
def read_account_screenings(account_id: str = None) -> list:
    return db.read(table='account_screening', query={'account_id': account_id})

@handle_exception
def screen_person(account_id: str = None, holder_name: str = None, residence_country: str = None, risk_score: float = None, created: str = None) -> dict:
    
    greylist = [
        'Albania',
        'Armenia',
        'Barbados',
        'Burkina Faso',
        'Haiti',
        'Ghana',
        'Gibraltar',
        'Democratic Republic of the Congo',
        'Yemen',
        'Jordan',
        'Cambodia',
        'Cayman Islands',
        'Mali',
        'Morocco',
        'Mozambique',
        'Nigeria',
        'United Arab Emirates',
        'Panama',
        'Senegal',
        'Syria',
        'Tanzania',
        'Turkey',
        'Uganda',
        'Philippines',
        'South Africa',
        'South Sudan',
        'Jamaica',
    ]

    blacklist = [
        'Democratic People\'s Republic of Korea (DPRK)',
        'Iran',
        'Myanmar'
    ]

    ofac_results = []
    uk_results = []
    ofac_sdn_list = get_ofac_sdn_list()
    ofac_df = pd.DataFrame(ofac_sdn_list)

    uk_sanctions_list = get_uk_sanctions_list()
    uk_df = pd.DataFrame(uk_sanctions_list)

    similarity_threshold = 0.8

    if residence_country in blacklist:
        fatf_status = 'Black listed'
    elif residence_country in greylist:
        fatf_status = 'Grey listed'
    else:
        fatf_status = 'Not listed'

    for index, row in ofac_df.iterrows():
        sdn_name = str(row['name']).strip()
        if sdn_name == '':
            continue
        similarity = difflib.SequenceMatcher(None, holder_name.lower(), sdn_name.lower()).ratio()
        if similarity >= similarity_threshold:
            data = {
                'name': sdn_name,
                'entity_number': row['entity_number'],
                'type': row['type'],
                'program': row['program'],
                'title': row['title'],
                'similarity': similarity,
                'call_sign': row['call_sign'],
                'vessel_type': row['vessel_type'],
                'tonnage': row['tonnage'],
                'gross_registered_tonnage': row['gross_registered_tonnage'],
                'vessel_flag': row['vessel_flag'],
                'vessel_owner': row['vessel_owner'],
                'more_info': row['more_info'],
                'source': 'OFAC'
            }

            ofac_results.append(data)

    uk_name_columns = ['Name 6', 'Name 1', 'Name 2', 'Name 3', 'Name 4', 'Name 5']
    for index, row in uk_df.iterrows():
        candidate_names = []
        for col in uk_name_columns:
            value = str(row.get(col, '')).strip()
            if value != '':
                candidate_names.append(value)

        if len(candidate_names) == 0:
            continue

        for candidate_name in candidate_names:
            similarity = difflib.SequenceMatcher(None, holder_name.lower(), candidate_name.lower()).ratio()
            if similarity < similarity_threshold:
                continue

            data = {
                'name': candidate_name,
                'entity_number': row.get('Unique ID', ''),
                'type': row.get('Designation Type', ''),
                'program': row.get('Regime Name', ''),
                'title': row.get('Title', ''),
                'similarity': similarity,
                'call_sign': '',
                'vessel_type': row.get('Type of ship', ''),
                'tonnage': row.get('Tonnage of ship', ''),
                'gross_registered_tonnage': '',
                'vessel_flag': row.get('Current believed flag of ship', ''),
                'vessel_owner': row.get('Current owner/operator (s)', ''),
                'more_info': row.get('Other Information', ''),
                'source': 'UK'
            }
            uk_results.append(data)

    uk_status = uk_results

    return db.create(table='account_screening', data={
        'account_id': account_id,
        'holder_name': holder_name,
        'ofac_results': ofac_results,
        'uk_status': uk_status,
        'fatf_status': fatf_status,
        'risk_score': risk_score if risk_score is not None else 0,
        'created': created
    })


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
