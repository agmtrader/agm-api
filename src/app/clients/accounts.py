from flask import Blueprint, request

from src.components.clients.accounts import create_account, read_accounts, read_accounts_with_metadata, submit_documents, read_instructions, send_to_ibkr, read_account_contacts_and_screenings, send_account_credentials_email

from src.components.clients.accounts import read_account_details, get_forms, submit_documents, submit_all_agreements, update_account, get_pending_tasks, get_registration_tasks, apply_fee_template, update_account_email, add_trading_permissions, get_product_country_bundles, get_status_of_instruction, add_clp_capability, deposit_funds, get_wire_instructions, change_financial_information, change_account_holder_external_id, withdraw_funds, transfer_position_internally, transfer_position_externally, get_financial_ranges, get_business_and_occupation, view_active_bank_instructions, view_withdrawable_cash

from src.components.clients.accounts import logout_of_brokerage_session, initialize_brokerage_session, create_sso_session, get_brokerage_accounts, get_account_statements, get_available_statements, get_portfolio_analyst_performance, get_all_watchlists, get_watchlist_information, get_market_data_snapshot, get_market_scanner_params, run_market_scanner, get_historical_market_data, get_securities_by_symbol, get_security_info, get_all_conids_from_exchange, get_contract_info, place_order, reply_to_order, cancel_order, get_open_orders

from src.utils.response import format_response

bp = Blueprint('accounts', __name__)

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


@bp.route('/contacts_screenings_summary', methods=['GET'])
@format_response
def read_contacts_screenings_summary_route():
    """Read the contact and screening summary for a single account."""
    account_id = request.args.get('account_id', None)
    return read_account_contacts_and_screenings(account_id=account_id)


@bp.route('/with_metadata', methods=['GET'])
@bp.route('/with-metadata', methods=['GET'])
@format_response
def read_with_metadata_route():
    """Read accounts together with derived metadata and optional advisor details."""
    query = {}
    id = request.args.get('id', None)
    user_id = request.args.get('user_id', None)
    code = request.args.get('advisor_code', None)
    include_advisor = request.args.get('include_advisor', 'false').strip().lower() in ('1', 'true', 'yes')
    force_refresh = request.args.get('refresh', 'false').strip().lower() in ('1', 'true', 'yes')

    if id:
        query['id'] = id
    if user_id:
        query['user_id'] = user_id
    if code:
        query['advisor_code'] = code

    return read_accounts_with_metadata(
        query=query,
        include_advisor=include_advisor,
        force_refresh=force_refresh
    )

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

@bp.route('/ibkr/registration_tasks', methods=['GET'])
@format_response
def registration_tasks_route():
    """Read pending IBKR registration tasks for an account."""
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    if not account_id:
        return {"error": "Missing account_id"}, 400
    return get_registration_tasks(account_id=account_id, master_account=master_account)

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

@bp.route('/ibkr/submit_all_agreements', methods=['POST'])
@format_response
def submit_all_agreements_route():
    """Submit all pending IBKR agreements for the provided account context."""
    payload = request.get_json(silent=True) or {}
    master_account = payload.get('master_account', None)
    forms = payload.get('forms', None)
    return submit_all_agreements(master_account=master_account, forms=forms)

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

@bp.route('/ibkr/account_email', methods=['POST'])
@format_response
def update_account_email_route():
    """Update the email address associated with an IBKR account user."""
    payload = request.get_json(force=True)
    reference_user_name = payload.get('reference_user_name')
    new_email = payload.get('new_email')
    access = payload.get('access', True)
    master_account = payload.get('master_account', None)
    if not reference_user_name or new_email is None:
        return {"error": "Missing reference_user_name or new_email"}, 400
    return update_account_email(reference_user_name=reference_user_name, new_email=new_email, access=access, master_account=master_account)

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

@bp.route('/ibkr/transfer_position_internally', methods=['POST'])
@format_response
def transfer_position_internally_route():
    """Transfer a position between two internal IBKR accounts."""
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
    """Transfer a position from an IBKR account to an external broker account."""
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

@bp.route('/ibkr/statements/available', methods=['GET'])
@format_response
def get_available_statements_route():
    """Read the list of statement periods available in IBKR for an account."""
    account_id = request.args.get('account_id', None)
    master_account = request.args.get('master_account', None)
    
    if not account_id:
        return {"error": "Missing account_id"}, 400
        
    return get_available_statements(account_id=account_id, master_account=master_account)

# Trading API
@bp.route('/ibkr/sso/create', methods=['POST'])
@format_response
def create_sso_session_route():
    """Create an IBKR SSO session using the provided credential payload."""
    payload = request.get_json(force=True)
    credential = payload.get('credential', None)
    ip = payload.get('ip', None)
    return create_sso_session(credential=credential, ip=ip)

@bp.route('/ibkr/sso/initialize', methods=['POST'])
@format_response
def initialize_brokerage_session_route():
    """Initialize the current IBKR brokerage web session."""
    return initialize_brokerage_session()

@bp.route('/ibkr/sso/logout', methods=['POST'])
@format_response
def logout_of_brokerage_session_route():
    """Log out of the current IBKR brokerage web session."""
    return logout_of_brokerage_session()

@bp.route('/ibkr/sso/accounts', methods=['GET'])
@format_response
def get_brokerage_accounts_route():
    """Read the brokerage accounts available in the active IBKR web session."""
    return get_brokerage_accounts()

@bp.route('/ibkr/portfolio-analyst', methods=['GET', 'POST'])
@format_response
def get_portfolio_analyst_performance_route():
    """Read PortfolioAnalyst performance for one or more account ids and a required frequency."""
    if request.method == 'POST':
        payload = request.get_json(force=True) or {}
        raw_acct_ids = payload.get('acctIds', [])
        freq = payload.get('freq')
    else:
        query_params = request.args.to_dict(flat=True)
        raw_acct_ids = request.args.getlist('acctIds') or query_params.get('acctIds', '')
        freq = query_params.get('freq')

    if isinstance(raw_acct_ids, str):
        acct_ids = [acct_id.strip() for acct_id in raw_acct_ids.split(',') if acct_id.strip()]
    elif isinstance(raw_acct_ids, list):
        acct_ids = [str(acct_id).strip() for acct_id in raw_acct_ids if str(acct_id).strip()]
    else:
        acct_ids = []

    if not acct_ids or not freq:
        return {"error": "Missing acctIds or freq"}, 400

    return get_portfolio_analyst_performance(acct_ids=acct_ids, freq=freq)

@bp.route('/ibkr/watchlists', methods=['GET'])
@format_response
def get_all_watchlists_route():
    """Read all IBKR watchlists available in the active SSO session."""
    return get_all_watchlists()

@bp.route('/ibkr/watchlist', methods=['GET'])
@format_response
def get_watchlist_information_route():
    """Read one IBKR watchlist by id from the active SSO session."""
    watchlist_id = request.args.get('id', None)
    if not watchlist_id:
        return {"error": "Missing id"}, 400
    return get_watchlist_information(watchlist_id=watchlist_id)

@bp.route('/ibkr/marketdata/snapshot', methods=['GET'])
@format_response
def get_market_data_snapshot_route():
    """Read IBKR market data snapshots for one or more conids."""
    conids = request.args.get('conids', '').strip()
    if not conids:
        return {"error": "Missing conids"}, 400
    return get_market_data_snapshot(conids=conids)

@bp.route('/ibkr/marketdata/history', methods=['GET'])
@format_response
def get_historical_market_data_route():
    """Read IBKR historical market data for one conid."""
    conid = request.args.get('conid', '').strip()
    period = request.args.get('period', '').strip()
    bar = request.args.get('bar', '').strip()
    if not conid or not period or not bar:
        return {"error": "Missing conid, period, or bar"}, 400
    return get_historical_market_data(conid=conid, period=period, bar=bar)

@bp.route('/ibkr/scanner/params', methods=['GET'])
@format_response
def get_market_scanner_params_route():
    """Read the available IBKR scanner parameter catalog."""
    return get_market_scanner_params()

@bp.route('/ibkr/scanner/run', methods=['POST'])
@format_response
def run_market_scanner_route():
    """Run the IBKR market scanner for the active SSO session."""
    payload = request.get_json(force=True) or {}
    instrument = payload.get('instrument')
    scan_type = payload.get('type')
    location = payload.get('location')
    filters = payload.get('filter', [])
    if not instrument or not scan_type or not location:
        return {"error": "Missing instrument, type, or location"}, 400
    return run_market_scanner(
        instrument=instrument,
        scan_type=scan_type,
        location=location,
        filters=filters,
    )

@bp.route('/ibkr/secdef/search', methods=['POST'])
@format_response
def get_securities_by_symbol_route():
    """Search IBKR securities by symbol and security type."""
    payload = request.get_json(force=True) or {}
    symbol = payload.get('symbol')
    sec_type = payload.get('secType')
    if not symbol or not sec_type:
        return {"error": "Missing symbol or secType"}, 400
    return get_securities_by_symbol(symbol=symbol, sec_type=sec_type)

@bp.route('/ibkr/secdef/info', methods=['GET'])
@format_response
def get_security_info_route():
    """Read IBKR security details by issuer id and security type."""
    issuer_id = request.args.get('issuerId', '').strip()
    sec_type = request.args.get('secType', '').strip()
    if not issuer_id or not sec_type:
        return {"error": "Missing issuerId or secType"}, 400
    return get_security_info(issuer_id=issuer_id, sec_type=sec_type)

@bp.route('/ibkr/trsrv/all-conids', methods=['GET'])
@format_response
def get_all_conids_from_exchange_route():
    """Read all conids for a given exchange from IBKR."""
    exchange = request.args.get('exchange', '').strip()
    if not exchange:
        return {"error": "Missing exchange"}, 400
    return get_all_conids_from_exchange(exchange=exchange)

@bp.route('/ibkr/contract/<int:conid>', methods=['GET'])
@format_response
def get_contract_info_route(conid: int):
    """Read IBKR contract metadata for one conid."""
    return get_contract_info(conid=conid)

@bp.route('/ibkr/orders', methods=['GET', 'POST'])
@format_response
def orders_route():
    """Read open IBKR orders or place one or more new orders."""
    if request.method == 'GET':
        return get_open_orders()

    payload = request.get_json(force=True) or {}
    account_id = payload.get('account_id')
    orders = payload.get('orders', [])
    if not account_id or not isinstance(orders, list) or len(orders) == 0:
        return {"error": "Missing account_id or orders"}, 400
    return place_order(account_id=account_id, orders=orders)

@bp.route('/ibkr/orders/reply/<reply_id>', methods=['POST'])
@format_response
def reply_to_order_route(reply_id: str):
    """Reply to an IBKR order confirmation prompt."""
    payload = request.get_json(silent=True) or {}
    confirmed = payload.get('confirmed', True)
    return reply_to_order(reply_id=reply_id, confirmed=confirmed)

@bp.route('/ibkr/orders/<account_id>/<order_id>', methods=['DELETE'])
@format_response
def cancel_order_route(account_id: str, order_id: str):
    """Cancel an IBKR order."""
    return cancel_order(account_id=account_id, order_id=order_id)

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
