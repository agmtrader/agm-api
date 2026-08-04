from flask import Blueprint, request

from src.components.clients.accounts import create_account, read_accounts, submit_documents, read_instructions, send_to_ibkr, send_account_credentials_email, send_transfer_instructions_email, send_welcome_email, send_funding_notification_email, send_missing_documents_email, link_account_contact, read_account_contacts, update_account_contact

from src.components.clients.accounts import read_account_details, get_forms, submit_documents, update_account, get_pending_tasks, apply_fee_template, add_trading_permissions, get_product_country_bundles, get_status_of_instruction, add_clp_capability, deposit_funds, get_wire_instructions, change_financial_information, change_account_holder_external_id, withdraw_funds, get_financial_ranges, get_business_and_occupation, view_active_bank_instructions, view_withdrawable_cash

from src.components.clients.accounts import get_account_statements

from src.utils.response import format_response

bp = Blueprint('accounts', __name__)

@bp.route('/contact', methods=['POST'])
@format_response
def link_account_contact_route():
    """Link a contact to an account."""
    payload = request.get_json(force=True)
    return link_account_contact(account_contact=payload.get('account_contact'))

@bp.route('/contacts', methods=['GET'])
@format_response
def read_account_contacts_route():
    """Read account-contact links filtered by account, contact, entity, or link ID."""
    query = {}
    for key in ('id', 'account_id', 'contact_id', 'entity_id'):
        value = request.args.get(key)
        if value:
            query[key] = value
    return read_account_contacts(query=query)

@bp.route('/contact/update', methods=['POST'])
@format_response
def update_account_contact_route():
    """Update an account-contact link."""
    payload = request.get_json(force=True)
    return update_account_contact(
        query=payload.get('query'),
        account_contact=payload.get('account_contact'),
    )

@bp.route('/create', methods=['POST'])
@format_response
def create_route():
    """Create an account record in the AGM database."""
    payload = request.get_json(force=True)
    account_data = payload.get('account', None)
    return create_account(account=account_data)

@bp.route('/read', methods=['GET'])
@format_response        
def read_route():
    """Read accounts from the database filtered by id, user_id, or advisor_code."""
    query = {}
    id = request.args.get('id', None)
    user_id = request.args.get('user_id', None)
    code = request.args.get('advisor_code', None)
    if id:
        query['id'] = id
    if user_id:
        query['user_id'] = user_id
    if code:
        query['advisor_code'] = code
    return read_accounts(query=query)


@bp.route('/update', methods=['POST'])
@format_response
def update_account_route():
    """Update account records selected by the provided query payload."""
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    account = payload.get('account', None)
    return update_account(query=query, account=account)

@bp.route('/send_credentials_email', methods=['POST'])
@format_response
def send_credentials_email_route():
    """Send the account credentials email flow for a client account."""
    payload = request.get_json(force=True)
    return send_account_credentials_email(
        account_id=payload.get('account_id'),
        client_email=payload.get('client_email'),
        lang=payload.get('lang', 'es'),
        cc=payload.get('cc', ''),
        send_welcome=payload.get('send_welcome', False),
        client_name=payload.get('client_name', ''),
    )

@bp.route('/send_transfer_instructions_email', methods=['POST'])
@format_response
def send_transfer_instructions_email_route():
    payload = request.get_json(force=True)
    return send_transfer_instructions_email(payload['content'], payload['client_email'], payload.get('lang', 'es'), payload.get('cc', ''), payload.get('initial', True))

@bp.route('/send_welcome_email', methods=['POST'])
@format_response
def send_welcome_email_route():
    payload = request.get_json(force=True)
    return send_welcome_email(payload['content'], payload['client_email'], payload.get('lang', 'es'), payload.get('cc', ''))

@bp.route('/send_funding_notification_email', methods=['POST'])
@format_response
def send_funding_notification_email_route():
    payload = request.get_json(force=True)
    return send_funding_notification_email(payload['content'], payload['client_email'], payload.get('lang', 'es'), payload.get('cc', ''), payload.get('days_since_opened'), payload.get('notice_number'))

@bp.route('/send_missing_documents_email', methods=['POST'])
@format_response
def send_missing_documents_email_route():
    payload = request.get_json(force=True)
    return send_missing_documents_email(payload['content'], payload['client_email'], payload.get('missing_type', 'multiple'), payload.get('lang', 'en'), payload.get('cc', ''))

@bp.route('/instructions', methods=['GET'])
@format_response
def read_instruction_route():
    """Read the account banking instructions stored in the database for an account."""
    query = {}  
    account_id = request.args.get('account_id', None)
    if account_id:  
        query['account_id'] = account_id
    return read_instructions(query=query)

@bp.route('/send_to_ibkr', methods=['POST'])
@format_response
def send_to_ibkr_route():
    """Submit an AGM account application to the IBKR onboarding flow."""
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    master_account = payload.get('master_account', None)
    application = payload.get('application', None)
    return send_to_ibkr(account_id=account_id, master_account=master_account, application=application)

# Account Management
@bp.route('/ibkr/details', methods=['GET'])
@format_response
def read_accounts_details_route():
    """Read detailed account information from the IBKR service."""
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    return read_account_details(account_id=account_id, master_account=master_account)

@bp.route('/ibkr/pending_tasks', methods=['GET'])
@format_response
def pending_tasks_route():
    """Read current IBKR pending tasks for an account."""
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_pending_tasks(account_id=account_id, master_account=master_account)

@bp.route('/ibkr/documents', methods=['POST'])
@format_response
def submit_documents_route():
    """Submit account documents to the IBKR service."""
    payload = request.get_json(force=True)
    document_submission_data = payload.get('document_submission', None)
    master_account = payload.get('master_account', None)
    return submit_documents(document_submission=document_submission_data, master_account=master_account)

@bp.route('/ibkr/fee_template', methods=['POST'])
@format_response
def apply_fee_template_route():
    """Apply an IBKR fee template to an account."""
    payload = request.get_json(force=True)
    account_id = payload.get('account_id')
    template_name = payload.get('template_name')
    master_account = payload.get('master_account', None)
    if not account_id or not template_name:
        return {"error": "Missing account_id or template_name"}, 400
    return apply_fee_template(account_id=account_id, template_name=template_name, master_account=master_account)

@bp.route('/ibkr/trading_permissions', methods=['POST'])
@format_response
def add_trading_permissions_route():
    """Add or update IBKR trading permissions for an account."""
    payload = request.get_json(force=True)
    account_id = payload.get('account_id')
    trading_permissions = payload.get('trading_permissions', [])
    master_account = payload.get('master_account', None)    
    return add_trading_permissions(account_id=account_id, trading_permissions=trading_permissions, master_account=master_account)

@bp.route('/ibkr/change_financial_information', methods=['POST'])
@format_response
def change_financial_information_route():
    """Update the financial information fields held by IBKR for an account."""
    payload = request.get_json(force=True)
    account_id = payload.get('account_id', None)
    master_account = payload.get('master_account', None)
    new_financial_information = payload.get('new_financial_information', None)

    if new_financial_information is None:
        field_map = {
            'investment_experience': 'investmentExperience',
            'investment_objectives': 'investmentObjectives',
            'additional_sources_of_income': 'additionalSourcesOfIncome',
            'sources_of_wealth': 'sourcesOfWealth',
            'net_worth': 'netWorth',
            'liquid_net_worth': 'liquidNetWorth',
            'annual_net_income': 'annualNetIncome',
            'total_assets': 'totalAssets',
            'source_of_funds': 'sourceOfFunds',
            'translated': 'translated',
        }
        new_financial_information = {}
        for payload_key, ibkr_key in field_map.items():
            if payload_key in payload:
                new_financial_information[ibkr_key] = payload.get(payload_key)

    return change_financial_information(
        account_id=account_id,
        new_financial_information=new_financial_information,
        master_account=master_account
    )

@bp.route('/ibkr/clp_capability', methods=['POST'])
@format_response
def add_clp_capability_route():
    """Enable CLP capability for an IBKR account, optionally with supporting documents."""
    payload = request.get_json(force=True)
    account_id = payload.get('account_id')
    document_submission = payload.get('document_submission', None)
    master_account = payload.get('master_account', None)
    return add_clp_capability(account_id=account_id, document_submission=document_submission, master_account=master_account)

@bp.route('/ibkr/deposit', methods=['POST'])
@format_response
def deposit_funds_route():
    """Create or submit an IBKR deposit instruction for an account."""
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    instruction = payload.get('instruction', None)
    account_id = payload.get('account_id', None)
    return deposit_funds(master_account=master_account, instruction=instruction, account_id=account_id)

@bp.route('/ibkr/withdraw', methods=['POST'])
@format_response
def withdraw_funds_route():
    """Create or submit an IBKR withdrawal instruction for an account."""
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    instruction = payload.get('instruction', None)
    account_id = payload.get('account_id', None)
    return withdraw_funds(master_account=master_account, instruction=instruction, account_id=account_id)

@bp.route('/ibkr/instructions', methods=['GET'])
@format_response
def get_status_of_instruction_route():
    """Read the current status of an IBKR cash instruction."""
    client_instruction_id = request.args.get('client_instruction_id', None)
    if not client_instruction_id:
        return {"error": "Missing client_instruction_id"}, 400
    return get_status_of_instruction(client_instruction_id=client_instruction_id)

@bp.route('/ibkr/active_bank_instructions', methods=['POST'])
@format_response
def view_active_bank_instructions_route():
    """Read the active bank instructions available for an IBKR cash instruction."""
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    account_id = payload.get('account_id', None)
    client_instruction_id = payload.get('client_instruction_id', None)
    bank_instruction_method = payload.get('bank_instruction_method', None)

    if not master_account or not account_id or not client_instruction_id or not bank_instruction_method:
        return {"error": "Missing master_account, account_id, client_instruction_id, or bank_instruction_method"}, 400

    return view_active_bank_instructions(
        master_account=master_account,
        account_id=account_id,
        client_instruction_id=client_instruction_id,
        bank_instruction_method=bank_instruction_method
    )

@bp.route('/ibkr/withdrawable_cash', methods=['POST'])
@format_response
def view_withdrawable_cash_route():
    """Read the withdrawable cash available for an IBKR account and instruction context."""
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    account_id = payload.get('account_id', None)
    client_instruction_id = payload.get('client_instruction_id', None)

    if not master_account or not account_id or not client_instruction_id:
        return {"error": "Missing master_account, account_id, or client_instruction_id"}, 400

    return view_withdrawable_cash(
        master_account=master_account,
        account_id=account_id,
        client_instruction_id=client_instruction_id
    )

@bp.route('/ibkr/wire_instructions', methods=['POST'])
@format_response
def get_wire_instructions_route():
    """Read IBKR wire instructions for an account and currency."""
    payload = request.get_json(force=True)
    master_account = payload.get('master_account', None)
    account_id = payload.get('account_id', None)
    currency = payload.get('currency', 'USD')
    if not master_account or not account_id or not currency:
        return {"error": "Missing master_account or account_id"}, 400
    return get_wire_instructions(master_account=master_account, account_id=account_id, currency=currency)

@bp.route('/ibkr/statements', methods=['POST'])
@format_response
def get_account_statements_route():
    """Read account statements from the IBKR service for a date range."""
    payload = request.get_json(force=True)

    account_id = payload.get('account_id', None)
    start_date = payload.get('start_date', None)
    end_date = payload.get('end_date', None)
    master_account = payload.get('master_account', None)
    language = payload.get('language', 'en')
    
    if not account_id or not start_date or not end_date or not master_account:
        return {"error": "Missing account_id, start_date, end_date, or master_account"}, 400

    if language not in {'en', 'es'}:
        return {"error": "Invalid language. Supported values: en, es"}, 400
        
    return get_account_statements(
        account_id=account_id,
        start_date=start_date,
        end_date=end_date,
        master_account=master_account,
        language=language,
    )

# Enums
@bp.route('/ibkr/forms', methods=['POST'])
@format_response
def get_forms_route():
    """Download the IBKR agreements and disclosure forms used during account opening."""
    payload = request.get_json(force=True)
    forms_data = payload.get('forms', None)
    master_account = payload.get('master_account', None)
    return get_forms(forms=forms_data, master_account=master_account)

@bp.route('/ibkr/product_country_bundles', methods=['GET'])
@format_response
def get_product_country_bundles_route():
    """Download the IBKR enum list of product-country bundles such as stocks or bonds by market."""
    return get_product_country_bundles()

@bp.route('/ibkr/financial_ranges', methods=['GET'])
@format_response
def get_financial_ranges_route():
    """Read the financial range types from the IBKR service."""
    return get_financial_ranges()

@bp.route('/ibkr/business_and_occupation', methods=['GET'])
@format_response
def get_business_and_occupation_route():
    """Read the business and occupation types from the IBKR service."""
    return get_business_and_occupation()
