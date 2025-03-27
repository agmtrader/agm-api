
from flask import Blueprint, request
from src.components.accounts import read_accounts, update_account, create_account, delete_account
from src.utils.scope_manager import verify_scope, enforce_user_filter

bp = Blueprint('accounts', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('accounts/create')
def create_account_route():
    payload = request.get_json(force=True)
    data = payload['data']
    id = payload['id']
    return create_account(data=data, id=id)

@bp.route('/read', methods=['POST'])
@verify_scope('accounts/read')
@enforce_user_filter()
def read_accounts_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_accounts(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('accounts/update')
def update_account_route():
    payload = request.get_json(force=True)
    data = payload['data']
    query = payload.get('query', None)
    return update_account(data=data, query=query)

@bp.route('/delete', methods=['POST'])
@verify_scope('accounts/delete')
def delete_account_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return delete_account(query=query)