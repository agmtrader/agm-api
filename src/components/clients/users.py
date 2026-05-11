from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Users Service', type='info')
logger.announcement('Initialized Users Service', type='success')

@handle_exception
def create_user(user: dict = None):
    user_id = db.create(table='user', data=user)
    return user_id

@handle_exception
def read_users(query=None):
    users = db.read(table='user', query=query)
    return users

@handle_exception
def read_user_by_id(id: str) -> dict:
    user = db.read(table='user', query={'id': id})
    if len(user) == 1:
        return user[0]
    else:
        logger.error(f'Single entry has {len(user)} matches.')
        raise Exception(f'Single entry has {len(user)} matches.')

@handle_exception
def update_user(query: dict = None, user: dict = None):
    if query is None:
        raise Exception("Query must be provided.")
    if user is None:
        raise Exception("User must be provided.")
    user = db.update(table='user', query=query, data=user)
    return {'id': user}