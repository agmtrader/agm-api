from src.utils.exception import handle_exception
import json
from src.helpers.database import Firebase
from src.utils.logger import logger
Database = Firebase()

logger.announcement('Initializing Accounts Service', type='info')
logger.announcement('Initialized Accounts Service', type='success')

@handle_exception
def create_account(data, id):
    account = Database.create(path='db/clients/accounts', data=data, id=id)
    return account

@handle_exception
def read_accounts(query=None):
    accounts = Database.read(path='db/clients/accounts', query=query)
    return accounts

@handle_exception
def update_account(data, query=None):
    account = Database.update(path='db/clients/accounts', data=data, query=query)
    return account

@handle_exception
def delete_account(query=None):
    account = Database.delete(path='db/clients/accounts', query=query)
    return account