from flask import Blueprint, request
from src.components.entities.accounts import create_account, read_accounts, submit_documents, upload_document, create_instruction, read_instructions, delete_document, read_account_documents
from src.components.entities.accounts import read_account_details, get_forms, submit_documents, update_account, get_security_questions, get_pending_tasks, get_registration_tasks, apply_fee_template, update_account_email, update_pending_aliases, add_trading_permissions, get_product_country_bundles, view_withdrawable_cash, view_active_bank_instructions, get_status_of_instruction, add_clp_capability, deposit_funds, get_wire_instructions, change_financial_information, withdraw_funds, create_user_for_account, transfer_position_internally, transfer_position_externally
from src.components.entities.accounts import logout_of_brokerage_session, initialize_brokerage_session, create_sso_session, get_brokerage_accounts
from src.utils.response import format_response

bp = Blueprint('accounts', __name__)

@bp.route('/ibkr/pending_alias', methods=['PATCH'])
@format_response
def update_pending_aliases_route():
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    return update_pending_aliases(master_account=master_account)

# Test

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
    documents, account_documents = read_account_documents(account_id=account_id)
    return {'documents': documents, 'account_documents': account_documents }

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
    category = payload.get('category', None)
    type = payload.get('type', None)
    issued_date = payload.get('issued_date', None)
    expiry_date = payload.get('expiry_date', None)
    name = payload.get('name', None)
    return upload_document(account_id=account_id, file_name=file_name, file_length=file_length, sha1_checksum=sha1_checksum, mime_type=mime_type, data=data, category=category, type=type, issued_date=issued_date, expiry_date=expiry_date, name=name)

@bp.route('/documents', methods=['DELETE'])
@format_response
def delete_document_route():
    payload = request.get_json(force=True)
    document_id = payload.get('document_id', None)
    if not document_id:
        return {"error": "Missing document_id"}, 400
    return delete_document(document_id=document_id)
 
@bp.route('/instructions', methods=['GET'])
@format_response
def read_instruction_route():
    query = {}  
    account_id = request.args.get('account_id', None)
    if account_id:  
        query['account_id'] = account_id
    return read_instructions(query=query)

# Account Management
@bp.route('/ibkr/details', methods=['GET'])
@format_response
def read_accounts_details_route():
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    return read_account_details(account_id=account_id, master_account=master_account)

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

@bp.route('/ibkr/documents', methods=['POST'])
@format_response
def submit_documents_route():
    payload = request.get_json(force=True)
    document_submission_data = payload.get('document_submission', None)
    master_account = payload.get('master_account', None)
    return submit_documents(document_submission=document_submission_data, master_account=master_account)

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

@bp.route('/ibkr/user', methods=['POST'])
@format_response
def create_user_for_account_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    prefix = payload.get('prefix', None)
    user_name = payload.get('user_name', None)
    external_id = payload.get('external_id', None)
    authorized_trader = payload.get('authorized_trader', False)
    master_account = payload.get('master_account', None)
    return create_user_for_account(account_id=account_id, prefix=prefix, user_name=user_name, external_id=external_id, authorized_trader=authorized_trader, master_account=master_account)

@bp.route('/ibkr/trading_permissions', methods=['POST'])
@format_response
def add_trading_permissions_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id')
    trading_permissions = payload.get('trading_permissions', [])
    master_account = payload.get('master_account', None)    
    return add_trading_permissions(account_id=account_id, trading_permissions=trading_permissions, master_account=master_account)

@bp.route('/ibkr/change_financial_information', methods=['POST'])
@format_response
def change_financial_information_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    investment_experience = payload.get('investment_experience', None)
    master_account = payload.get('master_account', None)
    return change_financial_information(account_id=account_id, investment_experience=investment_experience, master_account=master_account)

@bp.route('/ibkr/clp_capability', methods=['POST'])
@format_response
def add_clp_capability_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id')
    document_submission = payload.get('document_submission', None)
    master_account = payload.get('master_account', None)
    return add_clp_capability(account_id=account_id, document_submission=document_submission, master_account=master_account)

@bp.route('/ibkr/transfer_position_internally', methods=['POST'])
@format_response
def transfer_position_internally_route():
    payload = request.get_json(force=True)
    source_account_id = payload.get('source_account_id', None)
    target_account_id = payload.get('target_account_id', None)
    conid = payload.get('conid', None)
    transfer_quantity = payload.get('transfer_quantity', None)
    master_account = payload.get('master_account', None)
    if not source_account_id or not target_account_id or not conid or not transfer_quantity:
        return {"error": "Missing source_account_id, target_account_id, conid, or transfer_quantity"}, 400
    return transfer_position_internally(source_account_id=source_account_id, target_account_id=target_account_id, conid=conid, transfer_quantity=transfer_quantity, master_account=master_account)

@bp.route('/ibkr/transfer_position_externally', methods=['POST'])
@format_response
def transfer_position_externally_route():
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    client_instruction_id = payload.get('client_instruction_id', None)
    contra_broker_account_id = payload.get('contra_broker_account_id', None)
    contra_broker_dtc_code = payload.get('contra_broker_dtc_code', None)
    quantity = payload.get('quantity', None)
    conid = payload.get('conid', None)
    master_account = payload.get('master_account', None)
    if not account_id or not client_instruction_id or not contra_broker_account_id or not contra_broker_dtc_code or not quantity or not conid:
        return {"error": "Missing account_id, client_instruction_id, contra_broker_account_id, contra_broker_dtc_code, quantity, or conid"}, 400
    return transfer_position_externally(account_id=account_id, client_instruction_id=client_instruction_id, contra_broker_account_id=contra_broker_account_id, contra_broker_dtc_code=contra_broker_dtc_code, quantity=quantity, conid=conid, master_account=master_account)

@bp.route('/ibkr/deposit', methods=['POST'])
@format_response
def deposit_funds_route():
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    instruction = payload.get('instruction', None)
    account_id = payload.get('account_id', None)
    return deposit_funds(master_account=master_account, instruction=instruction, account_id=account_id)

@bp.route('/ibkr/withdraw', methods=['POST'])
@format_response
def withdraw_funds_route():
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    instruction = payload.get('instruction', None)
    account_id = payload.get('account_id', None)
    return withdraw_funds(master_account=master_account, instruction=instruction, account_id=account_id)

@bp.route('/ibkr/instructions', methods=['GET'])
@format_response
def get_status_of_instruction_route():
    client_instruction_id = request.args.get('client_instruction_id', None)
    if not client_instruction_id:
        return {"error": "Missing client_instruction_id"}, 400
    return get_status_of_instruction(client_instruction_id=client_instruction_id)

@bp.route('/ibkr/wire_instructions', methods=['POST'])
@format_response
def get_wire_instructions_route():
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    account_id = payload.get('account_id', None)
    currency = payload.get('currency', 'USD')
    if not master_account or not account_id or not currency:
        return {"error": "Missing master_account or account_id"}, 400
    return get_wire_instructions(master_account=master_account, account_id=account_id, currency=currency)

# Trading API
@bp.route('/ibkr/sso/create', methods=['POST'])
@format_response
def create_sso_session_route():
    payload = request.get_json(force=True)
    credential = payload.get('credential', None)
    ip = payload.get('ip', None)
    return create_sso_session(credential=credential, ip=ip)

@bp.route('/ibkr/sso/initialize', methods=['POST'])
@format_response
def initialize_brokerage_session_route():
    return initialize_brokerage_session()

@bp.route('/ibkr/sso/logout', methods=['POST'])
@format_response
def logout_of_brokerage_session_route():
    return logout_of_brokerage_session()

@bp.route('/ibkr/sso/accounts', methods=['GET'])
@format_response
def get_brokerage_accounts_route():
    return get_brokerage_accounts()

# Enums
@bp.route('/ibkr/security_questions', methods=['GET'])
@format_response
def get_security_questions_route():
    return get_security_questions()

@bp.route('/ibkr/forms', methods=['POST'])
@format_response
def get_forms_route():
    payload = request.get_json(force=True)
    forms_data = payload.get('forms', None)
    master_account = payload.get('master_account', None)
    return get_forms(forms=forms_data, master_account=master_account)

@bp.route('/ibkr/product_country_bundles', methods=['GET'])
@format_response
def get_product_country_bundles_route():
    return get_product_country_bundles()