from src.utils.exception import handle_exception
import json
from src.helpers.database import Firebase
from src.utils.logger import logger
Database = Firebase()

logger.announcement('Initializing Users Service', type='info')
logger.announcement('Initialized Users Service', type='success')

@handle_exception
def create_user(data, id):
    user = Database.create(path='users', data=data, id=id)
    user = json.loads(user.data.decode('utf-8'))
    return user

@handle_exception
def read_users(query=None):
    users = Database.read(path='users', query=query)
    users = json.loads(users.data.decode('utf-8'))
    return users

@handle_exception
def update_user(data, query=None):
    user = Database.update(path='users', data=data, query=query)
    user = json.loads(user.data.decode('utf-8'))
    return user

# Backend
@handle_exception
def read_user_by_id(id):
    user = Database.read(path='users', query={'id': id})
    user = json.loads(user.data.decode('utf-8'))
    if len(user) > 1:
        raise Exception('Multiple users found for id: ' + id)
    if len(user) == 0:
        return None
    return user[0]

@handle_exception
def read_user_by_credentials(username, password):
    user = Database.read(path='users', query={'username': username, 'password': password})
    user = json.loads(user.data.decode('utf-8'))
    if len(user) > 1:
        raise Exception('Multiple users found for username: ' + username)
    if len(user) == 0:
        return None
    return user[0]