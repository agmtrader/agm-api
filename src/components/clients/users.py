from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger
import bcrypt

logger.announcement('Initializing Users Service', type='info')
logger.announcement('Initialized Users Service', type='success')

SENSITIVE_USER_FIELDS = {'password', 'password_hash'}

def hash_password(password: str) -> str:
    if not password:
        raise Exception("Password must be provided.")
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=12)).decode('utf-8')

def verify_password(password: str, password_hash: str) -> bool:
    if not password or not password_hash:
        return False
    try:
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
    except ValueError:
        return False

def sanitize_user(user: dict = None):
    if user is None:
        return None
    return {key: value for key, value in user.items() if key not in SENSITIVE_USER_FIELDS}

def sanitize_users(users: list = None):
    return [sanitize_user(user) for user in users or []]

@handle_exception
def create_user(user: dict = None):
    if user is None:
        raise Exception("User must be provided.")
    if 'password' in user:
        user['password_hash'] = hash_password(user['password'])
        user['password'] = '__password_hash_only__'
    user_id = db.create(table='user', data=user)
    return user_id

@handle_exception
def read_users(query=None, include_sensitive: bool = False):
    users = db.read(table='user', query=query)
    if include_sensitive:
        return users
    return sanitize_users(users)

@handle_exception
def read_user_by_id(id: str) -> dict:
    user = read_users(query={'id': id})
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
    if 'password' in user:
        user['password_hash'] = hash_password(user['password'])
        user['password'] = '__password_hash_only__'
    user = db.update(table='user', query=query, data=user)
    return {'id': user}
