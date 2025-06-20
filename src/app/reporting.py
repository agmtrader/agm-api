from flask import Blueprint
from src.components.reporting import extract, transform, get_clients_report, get_accrued_interest_report
from src.utils.managers.scope_manager import verify_scope

bp = Blueprint('reporting', __name__)

@bp.route('/extract', methods=['GET'])
@verify_scope('reporting/extract')
def extract_route():
    return extract()

@bp.route('/transform', methods=['GET'])
@verify_scope('reporting/transform')
def transform_route():
    return transform()

@bp.route('/get_clients_report', methods=['GET'])
@verify_scope('reporting/get_clients_report')
def get_clients_report_route():
    return get_clients_report()

@bp.route('/get_accrued_interest', methods=['GET'])
@verify_scope('reporting/get_accrued_interest')
def get_accrued_interest_route():
    return get_accrued_interest_report()