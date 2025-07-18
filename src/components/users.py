from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.components.contacts import create_contact

logger.announcement('Initializing Users Service', type='info')
logger.announcement('Initialized Users Service', type='success')

@handle_exception
def create_user(user: dict = None):
    user_id = db.create(table='user', data=user)
    # Create contact for the user
    contact_data = {
        'name': user.get('name', ''),
        'email': user.get('email', ''),
        'user_id': user_id
    }
    create_contact(contact=contact_data)
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
        raise Exception(f'Single entry has {len(user)} matches.')

@handle_exception
def read_user_by_credentials(email, password):
    user = db.read(table='user', query={'email': email, 'password': password})
    if len(user) == 1:
        return user[0]
    else:
        raise Exception(f'Single entry has {len(user)} matches.')

@handle_exception
def update_user(user):
    user = db.update(table='user', data=user)
    return user