from flask import Blueprint, request
from src.components.accounts import create_account, read_accounts, upload_document, read_documents_by_account_id
from src.components.accounts import get_pending_tasks, get_registration_tasks
from src.components.accounts import read_account_details, get_forms, submit_account_management_requests, update_account, get_security_questions
from src.components.accounts import apply_fee_template, update_account_alias, update_account_email, update_pending_aliases
from src.utils.response import format_response

bp = Blueprint('accounts', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_route():
    payload = request.get_json(force=True)
    account_data = payload.get('account', None)
    return create_account(account=account_data)
 
@bp.route('/read', methods=['GET'])
@format_response        
def read_route():
    query = {}
    id = request.args.get('id', None)
    user_id = request.args.get('user_id', None)
    if id:
        query['id'] = id
    if user_id:
        query['user_id'] = user_id
    return read_accounts(query=query)

@bp.route('/update', methods=['POST'])
@format_response
def update_account_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    account = payload.get('account', None)
    return update_account(query=query, account=account)

@bp.route('/documents', methods=['GET'])
@format_response
def read_documents_by_account_id_route():
    account_id = request.args.get('account_id', None)
    return read_documents_by_account_id(account_id=account_id)

@bp.route('/documents', methods=['POST'])
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

# Account Management
@bp.route('/ibkr/details', methods=['GET'])
@format_response
def read_accounts_details_route():
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    return read_account_details(account_id=account_id, master_account=master_account)

@bp.route('/ibkr/update', methods=['POST'])
@format_response
def submit_account_management_requests_route():
    payload = request.get_json(force=True)
    account_management_requests_data = payload.get('account_management_requests', None)
    master_account = payload.get('master_account', None)
    return submit_account_management_requests(account_management_requests=account_management_requests_data, master_account=master_account)

@bp.route('/ibkr/registration_tasks', methods=['GET'])
@format_response
def registration_tasks_route():
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_registration_tasks(account_id=account_id, master_account=master_account)

@bp.route('/ibkr/pending_tasks', methods=['GET'])
@format_response
def pending_tasks_route():
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_pending_tasks(account_id=account_id, master_account=master_account)

@bp.route('/ibkr/fee_template', methods=['POST'])
@format_response
def apply_fee_template_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id')
    template_name = payload.get('template_name')
    master_account = payload.get('master_account', None)
    if not account_id or not template_name:
        return {"error": "Missing account_id or template_name"}, 400
    return apply_fee_template(account_id=account_id, template_name=template_name, master_account=master_account)

@bp.route('/ibkr/account_email', methods=['POST'])
@format_response
def update_account_email_route():
    payload = request.get_json(force=True)
    reference_user_name = payload.get('reference_user_name')
    new_email = payload.get('new_email')
    access = payload.get('access', True)
    master_account = payload.get('master_account', None)
    if not reference_user_name or new_email is None:
        return {"error": "Missing reference_user_name or new_email"}, 400
    return update_account_email(reference_user_name=reference_user_name, new_email=new_email, access=access, master_account=master_account)

@bp.route('/ibkr/security_questions', methods=['GET'])
@format_response
def get_security_questions_route():
    master_account = request.args.get('master_account', None)
    return get_security_questions(master_account=master_account)

@bp.route('/ibkr/forms', methods=['POST'])
@format_response
def get_forms_route():
    payload = request.get_json(force=True)
    forms_data = payload.get('forms', None)
    master_account = payload.get('master_account', None)
    return get_forms(forms=forms_data, master_account=master_account)

@bp.route('/ibkr/pending_alias', methods=['PATCH'])
@format_response
def update_pending_aliases_route():
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    return update_pending_aliases(master_account=master_account)