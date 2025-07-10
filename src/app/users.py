from flask import Blueprint, request
from src.components.users import read_users, update_user, create_user
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('users', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('users/create')
@format_response
def create_user_route():
    payload = request.get_json(force=True)
    user = payload.get('user', None)
    return create_user(user=user)

@bp.route('/read', methods=['POST'])
@verify_scope('users/read')
@format_response
def read_users_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_users(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('users/update')
@format_response
def update_user_route():
    payload = request.get_json(force=True)
    user = payload.get('user', None)
    return update_user(user=user)