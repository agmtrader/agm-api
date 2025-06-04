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
    account_id = db.create(table='account', data=account)
    return {'id': account_id}

@handle_exception
def read_accounts(query=None) -> list:
    accounts = db.read(table='account', query=query)
    return accounts

@handle_exception
def read_account_contact(account_id: str = None, query: dict = None) -> list:
    user_id = None
    account = db.read(table='account', query={'id': account_id})
    if len(account) != 1:
        raise Exception('Account has zero, two or more matches.')
    user_id = account[0]['user_id']

    user = db.read(table='user', query={'id': user_id})
    if len(user) != 1:
        raise Exception('User has zero, two or more matches.')
    contact_id = user[0]['contact_id']

    contacts = db.read(table='contact', query={'id': contact_id, **query})
    if len(contacts) == 1:
        return contacts[0]
    else:
        raise Exception('Account Contact has zero, two or more matches.')

@handle_exception
def read_account_docs(account_id: str = None, query: dict = None) -> list:
    """
    Reads the POA and POI documents for a given account
    """
    query = {'account_id': account_id, **query}
    account = db.read(table='individual_account_application', query=query)
    if len(account) != 1:
        raise Exception('Account has zero, two or more matches.')
    docs = documents.read(query)
    return docs

@handle_exception
def update_account_info(account_info: dict = None, account_id: str = None, query: dict = None) -> str:
    query = {'account_id': account_id, **query}
    account_id = db.update(table='individual_account_application', data=account_info, query=query)
    return account_id

@handle_exception
def upload_account_poa(f: dict = None, document_info: dict = None, user_id: str = None, account_id: str = None) -> str:
    return upload_poa(f=f, document_info=document_info, user_id=user_id, account_id=account_id)

@handle_exception
def upload_account_poi(f: dict = None, document_info: dict = None, user_id: str = None, account_id: str = None) -> str:
    return upload_poi(f=f, document_info=document_info, user_id=user_id, account_id=account_id)

# Account Management
@handle_exception
def read_account_details(account_id: str = None) -> dict:
    return account_management.get_account_details(account_id=account_id)

@handle_exception
def get_pending_tasks(account_id: str = None) -> list:
    return account_management.get_pending_tasks(account_id=account_id)

@handle_exception
def get_registration_tasks(account_id: str = None) -> list:
    return account_management.get_registration_tasks(account_id=account_id)