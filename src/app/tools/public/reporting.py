from flask import Blueprint, request
from src.components.tools.public.reporting import get_clients_report, get_client_fees_report, get_monthly_client_fees, get_nav_report, get_nav_report_monthly, get_bond_report, get_stocks_report, get_etfs_report, get_ust_bond_report, get_proposals_equity_report, get_open_positions_report, get_deposits_withdrawals, get_monthly_deposits_withdrawals, get_trades_report, get_brokerage_commissions, get_management_commissions, get_ending_balances_from_statements, get_ibkr_details
from src.utils.response import format_response

bp = Blueprint('reporting', __name__)

@bp.route('/clients', methods=['GET'])
@format_response
def get_clients_report_route():
    """Read the base clients reporting dataset."""
    return get_clients_report()

@bp.route('/clients/fees', methods=['GET'])
@format_response
def get_client_fees_report_route():
    """Read the client fees reporting dataset."""
    return get_client_fees_report()

@bp.route('/clients/fees/monthly', methods=['GET'])
@format_response
def get_monthly_client_fees_route():
    """Read monthly client fees filtered by one or more years and months."""
    years = request.args.get('years', request.args.get('year', '')).split(',')
    months = request.args.get('months', request.args.get('month', '')).split(',')

    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]

    return get_monthly_client_fees(years, months)

@bp.route('/nav', methods=['GET'])
@format_response
def get_nav_report_route():
    """Read the NAV reporting dataset."""
    return get_nav_report()

@bp.route('/nav/monthly', methods=['GET'])
@format_response
def get_nav_report_monthly_route():
    """Read monthly NAV figures filtered by one or more years and months."""
    years = request.args.get('years', request.args.get('year', '')).split(',')
    months = request.args.get('months', request.args.get('month', '')).split(',')

    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]

    return get_nav_report_monthly(years, months)

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

@bp.route('/open_positions', methods=['GET'])
@format_response
def get_open_positions_report_route():
    """Read the open positions reporting dataset."""
    return get_open_positions_report()

@bp.route('/proposals_equity', methods=['GET'])
@format_response
def get_proposals_equity_report_route():
    """Read the proposals equity reporting dataset."""
    return get_proposals_equity_report()

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
