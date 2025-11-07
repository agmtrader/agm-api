from flask import Blueprint
from src.components.tools.reporting import get_clients_report, get_nav_report, get_market_data_snapshot, get_rtd_report
from src.components.tools.reporting import run
from src.utils.response import format_response
import json

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

@bp.route('/market_data_snapshot', methods=['GET'])
@format_response
def get_market_data_snapshot_route():
    return get_market_data_snapshot()

@bp.route('/bvi', methods=['GET'])
@format_response
def bvi_inspection_route():
    with open('accounts.json', 'r') as f:
        accounts = json.load(f)
    return accounts