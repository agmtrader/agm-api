from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from typing import List

logger.announcement('Initializing Accounts Service', type='info')
ibkr_web_api = IBKRWebAPI()
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