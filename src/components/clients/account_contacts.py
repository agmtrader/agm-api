from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Account Contacts Service', type='info')
logger.announcement('Initialized Account Contacts Service', type='success')

table = 'account_contact'


@handle_exception
def create_account_contact(account_contact: dict = None):
    if not account_contact:
        raise Exception('account_contact payload is required')

    account_contact_id = db.create(table=table, data=account_contact)
    return {'id': account_contact_id}


@handle_exception
def read_account_contacts(query=None):
    account_contacts = db.read(table=table, query=query or {})
    return account_contacts


@handle_exception
def update_account_contact(query: dict = None, account_contact: dict = None):
    if not query:
        raise Exception('query is required')
    if not account_contact:
        raise Exception('account_contact payload is required')

    db.update(table=table, query=query, data=account_contact)
    return {'status': 'success'}


@handle_exception
def delete_account_contact(query: dict = None):
    if not query:
        raise Exception('query is required')

    db.delete(table=table, query=query)
    return {'status': 'success'}
