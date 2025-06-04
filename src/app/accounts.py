from flask import Blueprint, request
from src.components.accounts import create_account, read_accounts, upload_account_poa, upload_account_poi, update_account_info, read_account_docs, read_account_contact, get_pending_tasks, get_registration_tasks, read_account_details
from src.utils.managers.scope_manager import verify_scope, enforce_user_filter
from src.utils.response import format_response

bp = Blueprint('accounts', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('accounts/create')
@enforce_user_filter()
@format_response
def create_route():
    payload = request.get_json(force=True)
    account = payload.get('account', None)
    return create_account(account=account)
 
@bp.route('/read', methods=['POST'])
@verify_scope('accounts/read')
@enforce_user_filter()
@format_response
def read_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_accounts(query=query)

@bp.route('/details', methods=['POST'])
@verify_scope('accounts/read')
@enforce_user_filter()
@format_response
def read_accounts_details_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    return read_account_details(account_id=account_id)

@bp.route('/registration_tasks', methods=['POST'])
@verify_scope('accounts/read')
@enforce_user_filter()
@format_response
def registration_tasks_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_registration_tasks(account_id=account_id)

@bp.route('/pending_tasks', methods=['POST'])
@verify_scope('accounts/read')
@enforce_user_filter()
@format_response
def pending_tasks_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_pending_tasks(account_id=account_id)

@bp.route('/read_contact', methods=['POST'])
@verify_scope('accounts/read')
@enforce_user_filter()
@format_response
def read_contact_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    query = payload.get('query', None)
    return read_account_contact(account_id=account_id, query=query)

@bp.route('/read_documents', methods=['POST'])
@verify_scope('accounts/read')
@enforce_user_filter()
@format_response
def read_documents_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    query = payload.get('query', None)
    return read_account_docs(account_id=account_id, query=query)

@bp.route('/update_info', methods=['POST'])
@verify_scope('accounts/update')
@enforce_user_filter()
@format_response
def update_info_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    query = payload.get('query', None)
    account_info = payload.get('account_info', None)
    return update_account_info(account_info=account_info, account_id=account_id, query=query)

@bp.route('/upload_poa', methods=['POST'])
@verify_scope('accounts/upload')
@enforce_user_filter()
@format_response
def upload_poa_route():
    payload = request.get_json(force=True)
    f = payload.get('f', None)
    document_info = payload.get('document_info', None)
    user_id = payload.get('user_id', None)
    account_id = payload.get('account_id', None)
    return upload_account_poa(f=f, document_info=document_info, user_id=user_id, account_id=account_id)

@bp.route('/upload_poi', methods=['POST'])
@verify_scope('accounts/upload')
@enforce_user_filter()
@format_response
def upload_poi_route():
    payload = request.get_json(force=True)
    f = payload.get('f', None)
    document_info = payload.get('document_info', None)
    user_id = payload.get('user_id', None)
    account_id = payload.get('account_id', None)
    account_info = payload.get('account_info', None)
    return upload_account_poi(f=f, document_info=document_info, user_id=user_id, account_id=account_id, account_info=account_info)