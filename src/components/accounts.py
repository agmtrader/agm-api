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
def read_accounts(query: dict = None) -> list:
    accounts = db.read(table='account', query=query)
    return accounts

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

# IBKR Web API
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
def submit_documents(document_submission: dict = None, master_account: str = None) -> dict:
    return ibkr_web_api.submit_documents(document_submission=document_submission, master_account=master_account)

@handle_exception
def get_forms(forms: list = None, master_account: str = None) -> dict:
    return ibkr_web_api.get_forms(forms=forms, master_account=master_account)

@handle_exception
def process_documents(documents: list = None, master_account: str = None) -> dict:
    return ibkr_web_api.process_documents(documents=documents, master_account=master_account)

@handle_exception
def apply_fee_template(account_id: str = None, template_name: str = None, master_account: str = None) -> dict:
    """Apply a fee template to an account via IBKR API."""
    return ibkr_web_api.apply_fee_template(account_id=account_id, template_name=template_name, master_account=master_account)

@handle_exception
def update_account_alias(account_id: str = None, new_alias: str = None, master_account: str = None) -> dict:
    """Update account alias via IBKR API."""
    return ibkr_web_api.update_account_alias(account_id=account_id, new_alias=new_alias, master_account=master_account)

@handle_exception
def update_account_email(reference_user_name: str = None, new_email: str = None, access: bool = True, master_account: str = None) -> dict:
    """Update account email via IBKR API."""
    return ibkr_web_api.update_account_email(reference_user_name=reference_user_name, new_email=new_email, access=access, master_account=master_account)

@handle_exception
def get_security_questions() -> dict:
    return ibkr_web_api.get_security_questions()

@handle_exception
def update_pending_aliases(master_account: str = None) -> dict:
    """Fetch clients report, filter accounts without alias, update each alias, and return list."""
    from src.components.tools.reporting import get_clients_report  # local import to avoid circular dependency
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
def add_trading_permissions(
    reference_account_id: str = None,
    trading_permissions: list = None,
    documents: list  = None,
    master_account: str = None,
) -> dict:
    """Add trading permissions to an account via IBKR API."""
    return ibkr_web_api.add_trading_permissions(
        reference_account_id=reference_account_id,
        trading_permissions=trading_permissions or [],
        documents=documents,
        master_account=master_account,
    )

@handle_exception
def get_exchange_bundles(master_account: str = None) -> dict:
    """Get exchange bundles enumeration via IBKR API."""
    return ibkr_web_api.get_exchange_bundles(master_account=master_account)

# Trading API
@handle_exception
def create_sso_session(credential: str = None, ip: str = None) -> dict:
    return ibkr_web_api.create_sso_session(credential=credential, ip=ip)

@handle_exception
def initialize_brokerage_session() -> dict:
    return ibkr_web_api.initialize_brokerage_session()

@handle_exception
def logout_of_brokerage_session() -> dict:
    return ibkr_web_api.logout_of_brokerage_session()

@handle_exception
def get_brokerage_accounts() -> dict:
    return ibkr_web_api.get_brokerage_accounts()
