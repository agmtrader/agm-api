from flask import Blueprint
from src.components.tools.reporting import get_clients_report, get_nav_report, run, get_dimensional_table, get_rtd_report, get_open_positions_report, get_securities_bond_dictionary, get_client_fees, get_proposals_equity_report
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('reporting', __name__)

@bp.route('/run', methods=['GET'])
@verify_scope('reporting/run')
@format_response
def run_route():
    return run()

@bp.route('/dimensional_table', methods=['GET'])
@verify_scope('reporting/dimensional_table')
@format_response
def get_dimensional_table_route():
    return get_dimensional_table()

@bp.route('/clients', methods=['GET'])
@verify_scope('reporting/clients')
@format_response
def get_clients_report_route():
    return get_clients_report()

@bp.route('/client_fees', methods=['GET'])
@verify_scope('reporting/client_fees')
@format_response
def get_client_fees_report_route():
    return get_client_fees()

@bp.route('/nav', methods=['GET'])
@verify_scope('reporting/nav')
@format_response
def get_nav_report_route():
    return get_nav_report()

@bp.route('/rtd', methods=['GET'])
@verify_scope('reporting/rtd')
@format_response
def get_rtd_report_route():
    return get_rtd_report()

@bp.route('/open_positions', methods=['GET'])
@verify_scope('reporting/open_positions')
@format_response
def get_open_positions_report_route():
    return get_open_positions_report()

@bp.route('/proposals_equity', methods=['GET'])
@verify_scope('reporting/proposals_equity')
@format_response
def get_proposals_equity_report_route():
    return get_proposals_equity_report()

@bp.route('/securities_bond', methods=['GET'])
@verify_scope('reporting/securities_bond')
@format_response
def get_securities_bond_dictionary_route():
    return get_securities_bond_dictionary()