from flask import Blueprint
from src.components.tools.reporting import extract, transform, get_clients_report, get_cash_report, run
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('reporting', __name__)

@bp.route('/run', methods=['GET'])
@verify_scope('reporting/run')
@format_response
def run_route():
    return run()

@bp.route('/extract', methods=['GET'])
@verify_scope('reporting/extract')
@format_response
def extract_route():
    return extract()

@bp.route('/transform', methods=['GET'])
@verify_scope('reporting/transform')
@format_response
def transform_route():
    return transform()

@bp.route('/get_clients_report', methods=['GET'])
@verify_scope('reporting/get_clients_report')
@format_response
def get_clients_report_route():
    return get_clients_report()

@bp.route('/get_cash_report', methods=['GET'])
@verify_scope('reporting/get_cash_report')
@format_response
def get_cash_report_route():
    return get_cash_report()