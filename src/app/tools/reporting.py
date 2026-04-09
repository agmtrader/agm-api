from flask import Blueprint, request
from src.components.tools.reporting import get_clients_report, get_nav_report, get_nav_report_monthly, get_bond_report, get_stocks_report, get_proposals_equity_report, get_open_positions_report, get_deposits_withdrawals, get_trades_report, send_unfunded_emails, update_account_aliases
from src.components.tools.reporting import run_clients_pipeline, run_market_data_pipeline, get_ibkr_details
from src.utils.response import format_response

bp = Blueprint('reporting', __name__)

@bp.route('/run/clients', methods=['GET'])
@format_response
def run_clients_pipeline_route():
    return run_clients_pipeline()

@bp.route('/run/market_data', methods=['GET'])
@format_response
def run_market_data_pipeline_route():
    return run_market_data_pipeline()

@bp.route('/clients', methods=['GET'])
@format_response
def get_clients_report_route():
    return get_clients_report()

@bp.route('/nav', methods=['GET'])
@format_response
def get_nav_report_route():
    return get_nav_report()

@bp.route('/nav/monthly', methods=['GET'])
@format_response
def get_nav_report_monthly_route():
    years = request.args.get('years', request.args.get('year', '')).split(',')
    months = request.args.get('months', request.args.get('month', '')).split(',')

    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]

    return get_nav_report_monthly(years, months)

@bp.route('/rtd', methods=['GET'])
@format_response
def get_bond_report_route():
    return get_bond_report()

@bp.route('/stocks', methods=['GET'])
@format_response
def get_stocks_report_route():
    return get_stocks_report()

@bp.route('/open_positions', methods=['GET'])
@format_response
def get_open_positions_report_route():
    return get_open_positions_report()

@bp.route('/proposals_equity', methods=['GET'])
@format_response
def get_proposals_equity_report_route():
    return get_proposals_equity_report()

@bp.route('/deposits_withdrawals', methods=['GET'])
@format_response
def get_deposits_withdrawals_route():
    return get_deposits_withdrawals()

@bp.route('/send_unfunded_emails', methods=['GET'])
@format_response
def send_unfunded_emails_route():
    return send_unfunded_emails()

@bp.route('/pending_alias', methods=['PATCH'])
@format_response
def update_pending_aliases_route():
    return update_account_aliases()

@bp.route('/trades', methods=['GET'])
@format_response
def get_trades_report_route():
    years = request.args.get('years', '').split(',')
    months = request.args.get('months', '').split(',')
    
    # Clean empty strings if any
    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]
    
    return get_trades_report(years, months)

@bp.route('/ibkr_details', methods=['GET'])
@format_response
def get_ibkr_details_route():
    return get_ibkr_details()