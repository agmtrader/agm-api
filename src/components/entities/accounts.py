from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.managers.document_manager import DocumentManager
import pandas as pd
import difflib
from src.components.tools.reporting import get_ofac_sdn_list, get_uk_sanctions_list

logger.announcement('Initializing Accounts Service', type='info')
ibkr_web_api = IBKRWebAPI()
document_manager = DocumentManager()
logger.announcement('Initialized Accounts Service', type='success')

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
def change_investment_experience(account_id: str = None, investment_experience: dict = None, master_account: str = None) -> dict:
    """Change account financial information via IBKR API."""
    return ibkr_web_api.change_investment_experience(account_id=account_id, investment_experience=investment_experience, master_account=master_account)

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
