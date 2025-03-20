from flask import Blueprint, request
from src.components.users import read_users, update_user, create_user
from src.utils.scope_manager import verify_scope, enforce_user_filter

bp = Blueprint('users', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('users/create')
@enforce_user_filter(field_name='id')
def create_user_route():
    payload = request.get_json(force=True)
    data = payload['data']
    id = payload['id']
    return create_user(data=data, id=id)

@bp.route('/read', methods=['POST'])
@verify_scope('users/read')
@enforce_user_filter(field_name='id')
def read_users_route():
    payload = request.get_json(force=True)
    query = payload['query']
    return read_users(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('users/update')
@enforce_user_filter(field_name='id')
def update_user_route():
    payload = request.get_json(force=True)
    data = payload['data']
    query = payload['query']
    return update_user(data=data, query=query)