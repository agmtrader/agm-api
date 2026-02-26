from flask import Blueprint, request
from src.components.tools.reporting import get_clients_report, get_nav_report, get_rtd_report, get_proposals_equity_report, get_open_positions_report, get_ibkr_account_details, get_ibkr_account_pending_tasks, get_deposits_withdrawals, get_accounts_not_funded, get_trades_report
from src.components.tools.reporting import run_clients_pipeline, run_market_data_pipeline
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

@bp.route('/rtd', methods=['GET'])
@format_response
def get_rtd_report_route():
    return get_rtd_report()

@bp.route('/open_positions', methods=['GET'])
@format_response
def get_open_positions_report_route():
    return get_open_positions_report()

@bp.route('/proposals_equity', methods=['GET'])
@format_response
def get_proposals_equity_report_route():
    return get_proposals_equity_report()

@bp.route('/ibkr_account_details', methods=['GET'])
@format_response
def get_ibkr_account_details_route():
    return get_ibkr_account_details()

@bp.route('/ibkr_account_pending_tasks', methods=['GET'])
@format_response
def get_ibkr_account_pending_tasks_route():
    return get_ibkr_account_pending_tasks()

@bp.route('/deposits_withdrawals', methods=['GET'])
@format_response
def get_deposits_withdrawals_route():
    return get_deposits_withdrawals()

@bp.route('/accounts_not_funded', methods=['GET'])
@format_response
def get_accounts_not_funded_route():
    return get_accounts_not_funded()

@bp.route('/trades', methods=['GET'])
@format_response
def get_trades_report_route():
    years = request.args.get('years', '').split(',')
    months = request.args.get('months', '').split(',')
    
    # Clean empty strings if any
    years = [y.strip() for y in years if y.strip()]
    months = [m.strip() for m in months if m.strip()]
    
    return get_trades_report(years, months)