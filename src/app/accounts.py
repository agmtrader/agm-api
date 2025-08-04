from flask import Blueprint, request
from src.components.accounts import create_account, read_accounts, upload_document, read_documents_by_account_id
from src.components.accounts import list_accounts, get_pending_tasks, get_registration_tasks
from src.components.accounts import read_account_details, get_forms, update_account
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

@bp.route('/upload_document', methods=['POST'])
@verify_scope('accounts/upload_document')
@format_response
def upload_document_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    file_name = payload.get('file_name', None)
    file_length = payload.get('file_length', None)
    sha1_checksum = payload.get('sha1_checksum', None)
    mime_type = payload.get('mime_type', None)
    data = payload.get('data', None)
    return upload_document(account_id=account_id, file_name=file_name, file_length=file_length, sha1_checksum=sha1_checksum, mime_type=mime_type, data=data)

@bp.route('/read_documents', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def read_documents_by_account_id_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    return read_documents_by_account_id(account_id=account_id)

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

@bp.route('/pending_tasks', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def pending_tasks_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_pending_tasks(account_id=account_id)

@bp.route('/forms', methods=['POST'])
@verify_scope('accounts/forms')
@format_response
def get_forms_route():
    payload = request.get_json(force=True)
    forms_data = payload.get('forms', None)
    return get_forms(forms=forms_data)

@bp.route('/update', methods=['POST'])
@verify_scope('accounts/update')
@format_response
def update_route():
    payload = request.get_json(force=True)
    account_management_requests_data = payload.get('account_management_requests', None)
    return update_account(account_management_requests=account_management_requests_data)

"""
@bp.route('/documents', methods=['POST'])
@verify_scope('accounts/documents')
@format_response
def process_documents_route():
    documents = ['3230', '3024', '4070', '3044', '3089', '4304', '4404', '5013', '5001', '4024', '9130', '3074', '3203', '3070', '3094', '3071', '4587', '2192', '2191', '3077', '4399', '4684', '2109', '4016', '4289']
    return process_documents(documents=documents)
"""