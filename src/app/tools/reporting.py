from flask import Blueprint
from src.components.tools.reporting import get_clients_report, get_nav_report, get_market_data_snapshot
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

@bp.route('/market_data_snapshot', methods=['GET'])
@format_response
def get_market_data_snapshot_route():
    return get_market_data_snapshot()