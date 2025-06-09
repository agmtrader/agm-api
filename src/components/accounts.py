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
def create_account(account: dict = None, invoking_user_id: str = None) -> dict:
    if account is None:
        account = {}
    if invoking_user_id:
        account['user_id'] = invoking_user_id
        logger.info(f"Account data will be associated with invoking_user_id: {invoking_user_id}")
    elif not account.get('user_id'):
        # If no invoking_user_id (e.g. 'all' scope) and no user_id in payload, this might be an issue
        logger.warning("Creating account without explicit user_id and no invoking_user_id provided.")
        # Depending on rules, you might raise an Exception here or allow it.

    logger.info(f"Attempting to create account with data: {account}")
    account_id = db.create(table='account', data=account)
    return {'id': account_id}

@handle_exception
def read_accounts(query: dict = None, invoking_user_id: str = None) -> list:
    if query is None:
        query = {}
    if invoking_user_id:
        query['user_id'] = invoking_user_id
        logger.info(f"Reading accounts for user: {invoking_user_id} with query: {query}")
    else:
        logger.info(f"Reading accounts (no specific user filter - 'all' scope or public). Query: {query}")
        
    accounts = db.read(table='account', query=query)
    return accounts

@handle_exception
def read_account_contact(account_id: str = None, query: dict = None, invoking_user_id: str = None) -> dict:
    if query is None:
        query = {}

    account_filter = {'id': account_id}
    if invoking_user_id:
        account_filter['user_id'] = invoking_user_id
        logger.info(f"Verifying invoking user {invoking_user_id} access to account {account_id}")
    
    accounts_list = db.read(table='account', query=account_filter)
    if not accounts_list:
        logger.error(f"Account {account_id} not found or access denied for user {invoking_user_id}.")
        raise Exception('Account not found or access denied.')
    if len(accounts_list) > 1:
        logger.error(f"Data integrity issue: Multiple accounts found for ID {account_id} with user filter {invoking_user_id}.")
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
def read_account_docs(account_id: str = None, query: dict = None, invoking_user_id: str = None) -> list:
    """
    Reads the POA and POI documents for a given account, ensuring user has access.
    """
    if query is None:
        query = {}

    # Verify invoking user has access to the account_id first
    account_filter = {'id': account_id}
    if invoking_user_id:
        account_filter['user_id'] = invoking_user_id
        logger.info(f"Verifying invoking user {invoking_user_id} access to account {account_id} for reading documents.")
    
    verified_accounts = db.read(table='account', query=account_filter)
    if not verified_accounts:
        raise Exception(f"Account {account_id} not found or access denied for user {invoking_user_id}.")
    
    # Assuming 'documents.read' filters by account_id and other query params.
    # The 'documents' table in schema has 'user_id', 'account_id', 'drive_id', etc.
    # If documents are directly linked to user_id AND account_id, the query needs to reflect that.
    # For now, we assume documents.read() is smart enough or query needs adjustment here.
    doc_query = {'account_id': account_id, **query}
    if invoking_user_id:
         # If documents are also directly scoped by user_id in the 'document' table:
        doc_query['user_id'] = invoking_user_id 

    logger.info(f"Reading documents with query: {doc_query}")
    # The original code queried 'individual_account_application' which is not in the provided schema.
    # Then called documents.read(query). Assuming documents.read() queries the 'document' table.
    # If there's an intermediate table like 'account_documents', that logic would go here.
    docs = documents.read(doc_query) # DocumentCenter.read needs to handle this query
    return docs

@handle_exception
def update_account_info(account_info: dict = None, account_id: str = None, query: dict = None, invoking_user_id: str = None) -> str:
    if query is None:
        query = {}
    
    # Base query for update targets the specific account_id.
    update_target_query = {'id': account_id}
    if invoking_user_id:
        # Ensure the update is only on an account owned by the invoking user.
        update_target_query['user_id'] = invoking_user_id
        logger.info(f"Updating account info for account {account_id} by user {invoking_user_id}. Query: {update_target_query}")
    else:
        logger.info(f"Updating account info for account {account_id} (no specific user filter). Query: {update_target_query}")

    # The original code updated 'individual_account_application'. 
    # Assuming this should be 'account' table based on schema for general info.
    # If 'individual_account_application' is a separate related table, then db.update target and query needs adjustment.
    # For now, assuming update is on 'account' table.
    # The 'query' parameter passed to the function can contain additional conditions for the update.
    final_update_query = {**update_target_query, **query}

    # Before updating, it's good practice to check if the record exists and is accessible
    # This check is implicitly handled by db.update if it returns a count of updated rows or specific ID.
    # If db.update doesn't throw error or return 0 for no match, an explicit read might be needed first.
    
    updated_id = db.update(table='account', data=account_info, query=final_update_query)
    # db.update should ideally return the ID of the updated record or confirm success.
    # If it returns count, check if count > 0.
    if not updated_id: # or if updated_count == 0
        raise Exception(f"Failed to update account info for account {account_id} with query {final_update_query}. Account not found or not modified.")
    return account_id # Returning original account_id assuming success

@handle_exception
def upload_account_poa(f: dict = None, document_info: dict = None, user_id_from_payload: str = None, account_id: str = None, invoking_user_id: str = None) -> str:
    # Verify invoking_user_id has access to account_id
    account_filter = {'id': account_id}
    if invoking_user_id:
        account_filter['user_id'] = invoking_user_id
    
    verified_accounts = db.read(table='account', query=account_filter)
    if not verified_accounts:
        raise Exception(f"Account {account_id} not found or access denied for user {invoking_user_id} for POA upload.")

    # The user_id for document linking in upload_poa can be invoking_user_id or user_id_from_payload
    # depending on business logic (e.g., admin uploading on behalf of another user).
    # For security, invoking_user_id establishes permission to act on account_id.
    # The actual user_id field in the 'document' table might be set to invoking_user_id.
    logger.info(f"User {invoking_user_id} uploading POA for account {account_id}. User from payload: {user_id_from_payload}")
    return upload_poa(f=f, document_info=document_info, user_id_for_document=invoking_user_id or user_id_from_payload, account_id=account_id)

@handle_exception
def upload_account_poi(f: dict = None, document_info: dict = None, user_id_from_payload: str = None, account_id: str = None, account_info: dict = None, invoking_user_id: str = None) -> str:
    # Verify invoking_user_id has access to account_id
    account_filter = {'id': account_id}
    if invoking_user_id:
        account_filter['user_id'] = invoking_user_id
    
    verified_accounts = db.read(table='account', query=account_filter)
    if not verified_accounts:
        raise Exception(f"Account {account_id} not found or access denied for user {invoking_user_id} for POI upload.")

    logger.info(f"User {invoking_user_id} uploading POI for account {account_id}. User from payload: {user_id_from_payload}")
    # The 'account_info' parameter was in the original route but not in the original component function signature for upload_account_poi.
    # Assuming upload_poi might use it. If upload_poi is from client_documents.py, it also needs update.
    return upload_poi(f=f, document_info=document_info, user_id_for_document=invoking_user_id or user_id_from_payload, account_id=account_id, account_info=account_info)

# Account Management
@handle_exception
def read_account_details(account_id: str = None, invoking_user_id: str = None) -> dict:
    # AccountManagement class needs to be updated to handle invoking_user_id for authorization
    # For now, just passing it along. It should verify invoking_user_id has access to account_id.
    logger.info(f"Reading account details for {account_id}, invoked by {invoking_user_id}")
    return account_management.get_account_details(account_id=account_id, invoking_user_id=invoking_user_id)

@handle_exception
def update_account(account_management_requests: dict = None, invoking_user_id: str = None) -> dict:
    logger.info(f"Updating account via management for requests: {account_management_requests}, invoked by {invoking_user_id}")
    return account_management.update_account(account_management_requests=account_management_requests, invoking_user_id=invoking_user_id)

@handle_exception
def get_pending_tasks(account_id: str = None, invoking_user_id: str = None) -> list:
    logger.info(f"Getting pending tasks for account {account_id}, invoked by {invoking_user_id}")
    return account_management.get_pending_tasks(account_id=account_id, invoking_user_id=invoking_user_id)

@handle_exception
def get_registration_tasks(account_id: str = None, invoking_user_id: str = None) -> list:
    logger.info(f"Getting registration tasks for account {account_id}, invoked by {invoking_user_id}")
    return account_management.get_registration_tasks(account_id=account_id, invoking_user_id=invoking_user_id)

@handle_exception
def get_forms(forms: list = None, invoking_user_id: str = None) -> dict:
    # Depending on logic, get_forms might be user-specific or general.
    logger.info(f"Getting forms: {forms}, invoked by {invoking_user_id}")
    return account_management.get_forms(forms=forms, invoking_user_id=invoking_user_id)

@handle_exception
def create_sso_browser_session(invoking_user_id: str = None) -> str:
    # SSO session creation might inherently be tied to the invoking_user_id for logging or context.
    logger.info(f"Creating SSO browser session, invoked by {invoking_user_id}")
    # The hardcoded credential and IP should ideally be dynamic or configured.
    return account_management.create_sso_browser_session(credential="askjgn470", ip="200.229.8.74", invoking_user_id=invoking_user_id)