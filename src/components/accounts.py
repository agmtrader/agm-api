from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.managers.document_center import DocumentCenter
from src.utils.logger import logger
from src.components.documents.client_documents import upload_poa, upload_poi
from src.utils.connectors.account_management import AccountManagement

logger.announcement('Initializing Accounts Service', type='info')
documents = DocumentCenter('clients')
account_management = AccountManagement()
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
def read_account_contact(account_id: str = None, query: dict = None) -> dict:
    account_filter = {'id': account_id}
    
    accounts_list = db.read(table='account', query=account_filter)
    if not accounts_list:
        raise Exception('Account not found or access denied.')
    if len(accounts_list) > 1:
        raise Exception('Multiple accounts found, data integrity issue.')
    
    account_data = accounts_list[0]
    user_id_linked_to_account = account_data.get('user_id')
    if not user_id_linked_to_account:
        raise Exception(f"Account {account_id} does not have a user_id linked.")

    user_list = db.read(table='user', query={'id': user_id_linked_to_account})
    if not user_list:
        raise Exception(f"User record not found for user_id: {user_id_linked_to_account}.")
    contact_id = user_list[0].get('contact_id')
    if not contact_id:
        raise Exception(f"User {user_id_linked_to_account} does not have a contact_id.")

    final_contact_query = {'id': contact_id, **query}
    logger.info(f"Reading contact with final query: {final_contact_query}")
    contacts = db.read(table='contact', query=final_contact_query)
    
    if len(contacts) == 1:
        return contacts[0]
    elif not contacts:
        raise Exception(f"Contact not found for contact_id: {contact_id} with applied query.")
    else:
        raise Exception('Contact query returned multiple results for the same ID, check query parameters.')

@handle_exception
def read_account_docs(account_id: str = None, query: dict = None) -> list:
    """
    Reads the POA and POI documents for a given account, ensuring user has access.
    """
    if query is None:
        query = {}

    account_filter = {'id': account_id}
    
    verified_accounts = db.read(table='account', query=account_filter)
    if not verified_accounts:
        raise Exception(f"Account {account_id} not found or access denied.")
    
    doc_query = {'account_id': account_id, **query}
    docs = documents.read(doc_query)
    return docs

@handle_exception
def update_account_info(account_info: dict = None, account_id: str = None, query: dict = None) -> str:
    if query is None:
        query = {}
    
    # Base query for update targets the specific account_id.
    update_target_query = {'id': account_id}

    final_update_query = {**update_target_query, **query}
    
    updated_id = db.update(table='account', data=account_info, query=final_update_query)
    if not updated_id:
        raise Exception(f"Failed to update account info for account {account_id} with query {final_update_query}. Account not found or not modified.")
    return account_id

@handle_exception
def upload_account_poa(f: dict = None, document_info: dict = None, user_id_from_payload: str = None, account_id: str = None) -> str:
    account_filter = {'id': account_id}
    verified_accounts = db.read(table='account', query=account_filter)
    if not verified_accounts:
        raise Exception(f"Account {account_id} not found or access denied for user {user_id_from_payload} for POA upload.")

    return upload_poa(f=f, document_info=document_info, user_id_for_document=user_id_from_payload, account_id=account_id)

@handle_exception
def upload_account_poi(f: dict = None, document_info: dict = None, user_id_from_payload: str = None, account_id: str = None, account_info: dict = None) -> str:
    account_filter = {'id': account_id}
    verified_accounts = db.read(table='account', query=account_filter)
    if not verified_accounts:
        raise Exception(f"Account {account_id} not found or access denied for user {user_id_from_payload} for POI upload.")

    return upload_poi(f=f, document_info=document_info, user_id_for_document=user_id_from_payload, account_id=account_id, account_info=account_info)

# Account Management
@handle_exception
def read_account_details(account_id: str = None) -> dict:
    return account_management.get_account_details(account_id=account_id)

@handle_exception
def update_account(account_management_requests: dict = None) -> dict:
    return account_management.update_account(account_management_requests=account_management_requests)

@handle_exception
def get_pending_tasks(account_id: str = None) -> list:
    return account_management.get_pending_tasks(account_id=account_id)

@handle_exception
def get_registration_tasks(account_id: str = None) -> list:
    return account_management.get_registration_tasks(account_id=account_id)

@handle_exception
def get_forms(forms: list = None) -> dict:
    return account_management.get_forms(forms=forms)

@handle_exception
def create_sso_browser_session() -> str:
    return account_management.create_sso_browser_session()