from flask import Blueprint, request
from src.components.tools.reporting import get_clients_report, get_nav_report, get_rtd_report, get_proposals_equity_report, get_open_positions_report, get_ibkr_account_details, get_ibkr_account_pending_tasks, get_deposits_withdrawals
from src.components.tools.reporting import run
from src.utils.response import format_response

bp = Blueprint('reporting', __name__)

@bp.route('/run', methods=['GET'])
@format_response
def run_route():
    return run()
    
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