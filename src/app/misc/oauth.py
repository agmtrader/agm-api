from flask import Blueprint, request
from src.utils.logger import logger
import json
from src.components.users import read_user_by_credentials, create_user, read_users

bp = Blueprint('oauth', __name__)

@bp.route('/login', methods=['POST'])
def login():
    logger.announcement('OAuth2 login request.')
    payload = request.get_json(force=True)
    username = payload['username']
    password = payload['password']
    logger.announcement(f'OAuth2 login successful.', 'success')
    return read_user_by_credentials(username, password)

@bp.route('/create', methods=['POST'])
def create():
    logger.announcement('OAuth2 create request.')
    payload = request.get_json(force=True)
    data = payload['data']
    id = payload['id']
    
    # Check if user email already exists
    user = read_users(query={'email': data['email']})
    user = json.loads(user.data.decode('utf-8'))
    if user and len(user) > 0:
        raise Exception('User email already exists')

    # Check if user username already exists
    user = read_users(query={'username': data['username']})
    user = json.loads(user.data.decode('utf-8'))
    if user and len(user) > 0:
        raise Exception('User username already exists')

    return create_user(data=data, id=id)