from flask import Blueprint, request, g, send_file
from io import BytesIO
from src.components.tools.public.reporting import get_clients_report, get_client_fees_report, get_nav_report, get_nav_report_monthly, get_bond_report, get_stocks_report, get_etfs_report, get_ust_bond_report, get_proposals_equity_report, get_open_positions_report, get_deposits_withdrawals, get_monthly_deposits_withdrawals, get_trades_report, get_brokerage_commissions, get_management_commissions, get_ending_balances_from_statements, get_ibkr_details, get_ibkr_statement_summary, get_ibkr_statement_transactions
from src.utils.response import format_response
from src.components.tools.public.trade_confirmations import get_trade_confirmations_report
from src.components.tools.public.account_performance import get_account_performance
from src.components.tools.public import accounting_statements as accounting
from src.utils.authz import require_scope
from src.utils.exception import ServiceError

bp = Blueprint('reporting', __name__)


@bp.route('/account_performance', methods=['GET'])
@require_scope('all')
@format_response
def account_performance_route():
    return get_account_performance(
        request.args.get('start_date'), request.args.get('end_date'),
    ), 200, {'Cache-Control': 'private, no-store'}


def _read_report_view(load, fields):
    view = request.args.get('view', 'full')
    if view not in ('full', 'summary'):
        raise ServiceError('view must be full or summary', 400)
    rows = load()
    if view == 'full':
        return rows
    return [{field: row.get(field) for field in fields} for row in rows]


def _accounting_payload():
    if request.content_length and request.content_length > 36 * 1024 * 1024:
        raise ServiceError('The accounting request exceeds its size limit.', 413)
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        raise ServiceError('Provide a JSON object for this accounting request.', 400)
    return payload


@bp.route('/accounting_statements', methods=['GET'])
@require_scope('all')
@format_response
def accounting_inventory_route():
    if request.args.get('refresh') == '1':
        accounting.clear_cache()
    return accounting.inventory_report(request.args.get('year'), request.args.get('account'))


@bp.route('/accounting_statements/detail', methods=['GET'])
@require_scope('all')
@format_response
def accounting_detail_route():
    row = accounting.analyze_month(request.args.get('account'), request.args.get('year'), request.args.get('month'))
    if request.args.get('transactions') == '0':
        row.pop('transactions', None)
    return row


@bp.route('/accounting_statements/accounts', methods=['GET', 'POST'])
@require_scope('all')
@format_response
def accounting_accounts_route():
    if request.method == 'GET':
        if request.args.get('refresh') == '1':
            accounting.clear_cache()
        return accounting.load_registry()
    payload = _accounting_payload()
    return accounting.save_registry(payload.get('accounts'), payload.get('revision'), g.current_principal_id)


@bp.route('/accounting_statements/upload', methods=['POST'])
@require_scope('all')
@format_response
def accounting_upload_route():
    payload = _accounting_payload()
    return accounting.upload_original(payload.get('account'), payload.get('year'), payload.get('month'),
                                     payload.get('filename', ''), payload.get('content'), g.current_principal_id)


@bp.route('/accounting_statements/download', methods=['POST'])
@require_scope('all')
@format_response
def accounting_download_route():
    payload = _accounting_payload()
    content, filename = accounting.download_original(payload.get('account'), payload.get('year'), payload.get('month'), payload.get('source_key'))
    response = send_file(BytesIO(content), as_attachment=True, download_name=filename, mimetype='application/octet-stream')
    response.headers['Cache-Control'] = 'private, no-store'
    return response


@bp.route('/accounting_statements/annual_download', methods=['POST'])
@require_scope('all')
@format_response
def accounting_annual_download_route():
    payload = _accounting_payload()
    content, filename = accounting.annual_bundle(payload.get('year'), payload.get('account'), payload.get('institution'))
    response = send_file(BytesIO(content), as_attachment=True, download_name=filename, mimetype='application/zip')
    response.headers['Cache-Control'] = 'private, no-store'
    return response

@bp.route('/trade_confirmations', methods=['GET'])
@require_scope('all')
@format_response
def get_trade_confirmations_route():
    """Read the grouped all-account trade confirmations from Flex Query 1215183."""
    return get_trade_confirmations_report()

# Clients
@bp.route('/clients', methods=['GET'])
@format_response
def get_clients_report_route():
    """Read the base clients reporting dataset."""
    return _read_report_view(get_clients_report, (
        'Account ID', 'Alias', 'Status', 'sheet_name', 'Title', 'SLS Devices', 'Email Address',
    ))

@bp.route('/clients/fees', methods=['GET'])
@format_response
def get_client_fees_report_route():
    """Read the client fees reporting dataset."""
    return get_client_fees_report()

@bp.route('/nav', methods=['GET'])
@format_response
def get_nav_report_route():
    """Read the NAV reporting dataset."""
    return _read_report_view(get_nav_report, ('ClientAccountID', 'Total', 'ReportDate'))

@bp.route('/nav/monthly', methods=['GET'])
@format_response
def get_nav_report_monthly_route():
    """Read monthly NAV figures filtered by one or more years and months."""
    years = request.args.get('years', request.args.get('year', '')).split(',')
    months = request.args.get('months', request.args.get('month', '')).split(',')

    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]

    return get_nav_report_monthly(years, months)

@bp.route('/open_positions', methods=['GET'])
@format_response
def get_open_positions_report_route():
    """Read the open positions reporting dataset."""
    account_ids = [value.strip() for value in request.args['account_ids'].split(',') if value.strip()] if 'account_ids' in request.args else None
    return get_open_positions_report(include_quotes=request.args.get('quotes') == '1', account_ids=account_ids)

@bp.route('/trades', methods=['GET'])
@format_response
def get_trades_report_route():
    """Read the trades reporting dataset filtered by one or more years and months."""
    years = request.args.get('years', '').split(',')
    months = request.args.get('months', '').split(',')
    
    # Clean empty strings if any
    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]
    
    return get_trades_report(years, months)

@bp.route('/ibkr_details', methods=['GET'])
@format_response
def get_ibkr_details_route():
    """Download the daily backup of IBKR account details, including account, financial, and account holder information."""
    return get_ibkr_details()

@bp.route('/deposits_withdrawals', methods=['GET'])
@format_response
def get_deposits_withdrawals_route():
    """Read the deposits and withdrawals reporting dataset."""
    return get_deposits_withdrawals()

@bp.route('/deposits_withdrawals/monthly', methods=['GET'])
@format_response
def get_monthly_deposits_withdrawals_route():
    """Read deposits and withdrawals filtered by date range or one or more years and months."""
    years = request.args.get('years', request.args.get('year', '')).split(',')
    months = request.args.get('months', request.args.get('month', '')).split(',')
    start_date = request.args.get('start_date', '').strip() or None
    end_date = request.args.get('end_date', '').strip() or None

    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]

    return get_monthly_deposits_withdrawals(years, months, start_date=start_date, end_date=end_date)

# Market data
@bp.route('/rtd', methods=['GET'])
@format_response
def get_bond_report_route():
    """Download the large RTD bond list dataset, similar in scope to the U.S. Treasury bond report."""
    return get_bond_report()

@bp.route('/stocks', methods=['GET'])
@format_response
def get_stocks_report_route():
    """Read the stocks reporting dataset."""
    return get_stocks_report()

@bp.route('/etfs', methods=['GET'])
@format_response
def get_etfs_report_route():
    """Read the ETFs reporting dataset."""
    return get_etfs_report()

@bp.route('/ust_bonds', methods=['GET'])
@format_response
def get_ust_bond_report_route():
    """Read the U.S. Treasury bonds reporting dataset."""
    return get_ust_bond_report()

# ?
@bp.route('/proposals_equity', methods=['GET'])
@format_response
def get_proposals_equity_report_route():
    """Read the proposals equity reporting dataset."""
    return get_proposals_equity_report()

# Commissions
@bp.route('/brokerage_commissions', methods=['GET'])
@format_response
def get_brokerage_commissions_route():
    """Read the brokerage commissions reporting dataset."""
    return get_brokerage_commissions()

@bp.route('/management_commissions', methods=['GET'])
@format_response
def get_management_commissions_route():
    """Read the management commissions reporting dataset."""
    return get_management_commissions()

@bp.route('/ending_balances_from_statements', methods=['GET'])
@format_response
def get_ending_balances_from_statements_route():
    """Read ending balances derived from account statements."""
    return get_ending_balances_from_statements()

@bp.route('/ibkr_statement_summary', methods=['GET'])
@format_response
def get_ibkr_statement_summary_route():
    """Read monthly IBKR account-value summaries from Drive statements."""
    accounts = [value.strip() for value in request.args.get('accounts', request.args.get('account', '')).split(',') if value.strip()]
    years = [value.strip() for value in request.args.get('years', request.args.get('year', '')).split(',') if value.strip()]
    months = [value.strip() for value in request.args.get('months', request.args.get('month', '')).split(',') if value.strip()]
    return get_ibkr_statement_summary(accounts, years, months)

@bp.route('/ibkr_statement_transactions', methods=['GET'])
@format_response
def get_ibkr_statement_transactions_route():
    """Read IBKR Base Currency Summary transactions from Drive statements."""
    accounts = [value.strip() for value in request.args.get('accounts', request.args.get('account', '')).split(',') if value.strip()]
    years = [value.strip() for value in request.args.get('years', request.args.get('year', '')).split(',') if value.strip()]
    months = [value.strip() for value in request.args.get('months', request.args.get('month', '')).split(',') if value.strip()]
    return get_ibkr_statement_transactions(accounts, years, months)
