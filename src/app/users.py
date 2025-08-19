from flask import Blueprint, request
from src.components.users import read_users, update_user, create_user
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('users', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create():
    payload = request.get_json(force=True)
    user = payload['user']
    
    # Check if user email already exists
    existing_user = read_users(query={'email': user['email']})

    if existing_user and len(existing_user) > 1:
        raise Exception('User email already exists')
    
    user = create_user(user=user)
    return user

@bp.route('/login', methods=['POST'])
@format_response
def login():
    payload = request.get_json(force=True)
    email = payload['email']
    password = payload['password']

    users = read_users(query={'email': email, 'password': password})
    if len(users) == 1:
        return users[0]
    else:
        raise Exception(f'Single entry has {len(users)} matches.')

@bp.route('/read', methods=['GET'])
@verify_scope('users/read')
@format_response
def read_users_route():
    query = {}
    id = request.args.get('id', None)
    user_id = request.args.get('user_id', None)
    lead_id = request.args.get('lead_id', None)
    if id:
        query['id'] = id
    if user_id:
        query['user_id'] = user_id
    if lead_id:
        query['lead_id'] = lead_id
    return read_users(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('users/update')
@format_response
def update_user_route():
    payload = request.get_json(force=True)
    user = payload.get('user', None)
    return update_user(user=user)