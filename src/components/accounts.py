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
    if account is None:
        account = {}
    logger.info(f"Attempting to create account with data: {account}")
    account_id = db.create(table='account', data=account)
    return {'id': account_id}

@handle_exception
def read_accounts(query: dict = None) -> list:
    accounts = db.read(table='account', query=query)
    return accounts

@handle_exception
def upload_document(account_id: str = None, file_name: str = None, file_length: int = None, sha1_checksum: str = None, mime_type: str = None, data: str = None) -> dict:
    print(f"Uploading document: {file_name} to account: {account_id}, file_length: {file_length}, sha1_checksum: {sha1_checksum}, mime_type: {mime_type}, data: {data}")
    return document_manager.upload_document(account_id=account_id, file_name=file_name, file_length=file_length, sha1_checksum=sha1_checksum, mime_type=mime_type, data=data)

@handle_exception
def read_documents_by_account_id(account_id: str = None) -> list:
    account_documents = db.read(table='account_document', query={'account_id': account_id})
    documents = []
    for account_document in account_documents:
        document = db.read(table='document', query={'id': account_document['document_id']})
        for d in document:
            documents.append(d)
    return documents

# Account Management
@handle_exception
def list_accounts() -> dict:
    return ibkr_web_api.list_accounts()

@handle_exception
def read_account_details(account_id: str = None) -> dict:
    return ibkr_web_api.get_account_details(account_id=account_id)

@handle_exception
def update_account(account_management_requests: dict = None) -> dict:
    return ibkr_web_api.update_account(account_management_requests=account_management_requests)

@handle_exception
def get_pending_tasks(account_id: str = None) -> list:
    return ibkr_web_api.get_pending_tasks(account_id=account_id)

@handle_exception
def get_registration_tasks(account_id: str = None) -> list:
    return ibkr_web_api.get_registration_tasks(account_id=account_id)

@handle_exception
def get_forms(forms: list = None) -> dict:
    return ibkr_web_api.get_forms(forms=forms)

@handle_exception
def create_sso_browser_session() -> str:
    return ibkr_web_api.create_sso_browser_session()

@handle_exception
def process_documents(documents: list = None) -> dict:
    return ibkr_web_api.process_documents(documents=documents)