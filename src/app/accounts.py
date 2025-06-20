from flask import Blueprint, request
from src.components.accounts import create_account, read_accounts, read_account_contact, update_account_info
from src.components.accounts import list_accounts, get_pending_tasks, get_registration_tasks, read_account_details, get_forms, update_account, create_sso_browser_session
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('accounts', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('accounts/create')
@format_response
def create_route():
    payload = request.get_json(force=True)
    account_data = payload.get('account', None)
    return create_account(account=account_data)
 
@bp.route('/read', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def read_route():
    payload = request.get_json(force=True)
    query_params = payload.get('query', None)
    return read_accounts(query=query_params)

@bp.route('/read_contact', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def read_contact_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    query_params = payload.get('query', None)
    return read_account_contact(account_id=account_id, query=query_params)

@bp.route('/update_info', methods=['POST'])
@verify_scope('accounts/update')
@format_response
def update_info_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    query_params = payload.get('query', None)
    account_info_data = payload.get('account_info', None)
    return update_account_info(account_info=account_info_data, account_id=account_id, query=query_params)

# Account Management
@bp.route('/list', methods=['GET'])
@verify_scope('accounts/read')
@format_response
def list_accounts_route():
    return list_accounts()

@bp.route('/details', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def read_accounts_details_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    return read_account_details(account_id=account_id)

@bp.route('/registration_tasks', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def registration_tasks_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_registration_tasks(account_id=account_id)

@bp.route('/forms', methods=['POST'])
@verify_scope('accounts/forms')
@format_response
def get_forms_route():
    payload = request.get_json(force=True)
    forms_data = payload.get('forms', None)
    return get_forms(forms=forms_data)

@bp.route('/pending_tasks', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def pending_tasks_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_pending_tasks(account_id=account_id)

@bp.route('/update', methods=['POST'])
@verify_scope('accounts/update')
@format_response
def update_route():
    payload = request.get_json(force=True)
    account_management_requests_data = payload.get('account_management_requests', None)
    return update_account(account_management_requests=account_management_requests_data)

@bp.route('/create_sso_browser_session', methods=['GET'])
@verify_scope('accounts/create_sso_browser_session')
@format_response
def create_sso_browser_session_route():
    return create_sso_browser_session()