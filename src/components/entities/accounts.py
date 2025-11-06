from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.managers.document_manager import DocumentManager

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
def upload_document(account_id: str = None, file_name: str = None, file_length: int = None, sha1_checksum: str = None, mime_type: str = None, data: str = None) -> dict:
    logger.info(f"Uploading document: {file_name} to account: {account_id}, file_length: {file_length}, sha1_checksum: {sha1_checksum}, mime_type: {mime_type}, data: {data}")
    return document_manager.upload_document(account_id=account_id, file_name=file_name, file_length=file_length, sha1_checksum=sha1_checksum, mime_type=mime_type, data=data)

@handle_exception
def read_documents_by_account_id(account_id: str = None) -> list:
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
    return documents

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
def apply_fee_template(account_id: str = None, template_name: str = None, master_account: str = None) -> dict:
    """Apply a fee template to an account via IBKR API."""
    return ibkr_web_api.apply_fee_template(account_id=account_id, template_name=template_name, master_account=master_account)

@handle_exception
def update_account_alias(account_id: str = None, new_alias: str = None, master_account: str = None) -> dict:
    """Update account alias via IBKR API."""
    return ibkr_web_api.update_account_alias(account_id=account_id, new_alias=new_alias, master_account=master_account)

@handle_exception
def submit_documents(document_submission: dict = None, master_account: str = None) -> dict:
    return ibkr_web_api.submit_documents(document_submission=document_submission, master_account=master_account)

@handle_exception
def update_account_email(reference_user_name: str = None, new_email: str = None, access: bool = True, master_account: str = None) -> dict:
    """Update account email via IBKR API."""
    return ibkr_web_api.update_account_email(reference_user_name=reference_user_name, new_email=new_email, access=access, master_account=master_account)

@handle_exception
def update_pending_aliases(master_account: str = None) -> dict:
    """Fetch clients report, filter accounts without alias, update each alias, and return list."""
    from src.components.tools.reporting import get_clients_report
    clients = get_clients_report()
    pending_accounts = [c for c in clients if (c.get('Alias') in (None, '')) and c.get('Status') not in ('Rejected', 'Closed', 'Funded Pending')]
    updated_accounts = []
    for account in pending_accounts:
        account_id = account.get('Account ID')
        title = account.get('Title')
        if account_id and title is not None:
            new_alias = f"{account_id} {title}"
            try:
                # Reuse existing helper to update alias via IBKR API
                update_account_alias(account_id=account_id, new_alias=new_alias, master_account=master_account)
                updated_accounts.append({
                    'account_id': account_id,
                    'new_alias': new_alias
                })
            except Exception as e:
                logger.error(f"Failed to update alias for {account_id}: {e}")
    return {
        'updated': len(updated_accounts),
        'accounts': updated_accounts
    }

@handle_exception
def add_trading_permissions(account_id: str = None, trading_permissions: list = None, master_account: str = None) -> dict:
    """Add trading permissions to an account via IBKR API."""
    return ibkr_web_api.add_trading_permissions(account_id=account_id, trading_permissions=trading_permissions, master_account=master_account)

@handle_exception
def add_clp_capability(account_id: str = None, document_submission: dict = None, master_account: str = None) -> dict:
    """Add CLP capability to an account via IBKR API."""
    return ibkr_web_api.add_clp_capability(account_id=account_id, document_submission=document_submission, master_account=master_account)

@handle_exception
def change_financial_information(account_id: str = None, investment_experience: dict = None, master_account: str = None) -> dict:
    """Change account financial information via IBKR API."""
    return ibkr_web_api.change_financial_information(account_id=account_id, investment_experience=investment_experience, master_account=master_account)

# Cash Transfers
@handle_exception
def deposit_funds(master_account: str = None, client_instruction_id: str = None, account_id: str = None, amount: float = None, currency: str = "USD", bank_instruction_method: str = "WIRE", is_ira: bool = False, sending_institution: str = None, identifier: str = None, special_instruction: str = None, bank_instruction_name: str = None, sender_institution_name: str = None) -> dict:
    """Deposit funds via IBKR API."""
    return ibkr_web_api.deposit_funds(master_account=master_account, client_instruction_id=client_instruction_id, account_id=account_id, amount=amount, currency=currency, bank_instruction_method=bank_instruction_method, is_ira=is_ira, sending_institution=sending_institution, identifier=identifier, special_instruction=special_instruction, bank_instruction_name=bank_instruction_name, sender_institution_name=sender_institution_name)

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

# Enums
@handle_exception
def get_security_questions() -> dict:
    """Get security questions via IBKR API."""
    return ibkr_web_api.get_security_questions()

@handle_exception
def get_product_country_bundles() -> dict:
    """Get product country bundles enumeration via IBKR API."""
    return ibkr_web_api.get_product_country_bundles()

@handle_exception
def get_forms(forms: list = None, master_account: str = None) -> dict:
    return ibkr_web_api.get_forms(forms=forms, master_account=master_account)

# Wire instructions
@handle_exception
def get_wire_instructions(master_account: str = None, account_id: str = None, currency: str = "USD") -> dict:
    """Get wire instructions via IBKR API."""
    return ibkr_web_api.get_wire_instructions(master_account=master_account, account_id=account_id, currency=currency)