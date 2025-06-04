from flask import Blueprint, request
from src.utils.logger import logger
import json
from src.components.users import read_user_by_credentials, create_user, read_users

bp = Blueprint('oauth', __name__)

@bp.route('/login', methods=['POST'])
def login():
    logger.announcement('OAuth2 login request.')
    payload = request.get_json(force=True)
    email = payload['email']
    password = payload['password']
    logger.announcement(f'OAuth2 login successful.', 'success')
    return read_user_by_credentials(email, password)

@bp.route('/create', methods=['POST'])
def create():
    logger.announcement('OAuth2 create request.')
    payload = request.get_json(force=True)
    user = payload['user']
    
    # Check if user email already exists
    existing_user = read_users(query={'email': user['email']})

    if existing_user and len(existing_user) > 1:
        raise Exception('User email already exists')
    
    return {'id': create_user(user=user)}