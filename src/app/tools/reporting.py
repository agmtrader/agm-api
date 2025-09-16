from flask import Blueprint
from src.components.tools.reporting import get_clients_report, get_nav_report, get_rtd_report, get_open_positions_report, get_securities_bond_dictionary, get_client_fees, get_proposals_equity_report
from src.components.tools.reporting import run
from src.utils.managers.scope_manager import verify_scope
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

@bp.route('/client_fees', methods=['GET'])
@format_response
def get_client_fees_report_route():
    return get_client_fees()

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

@bp.route('/securities_bond', methods=['GET'])
@format_response
def get_securities_bond_dictionary_route():
    return get_securities_bond_dictionary()