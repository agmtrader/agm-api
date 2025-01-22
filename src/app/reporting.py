from flask import Blueprint
from src.components.reporting import extract, transform, get_clients_report, get_accrued_interest_report

bp = Blueprint('reporting', __name__)

@bp.route('/extract', methods=['GET'])
def extract_route():
    return extract()

@bp.route('/transform', methods=['GET'])
def transform_route():
    return transform()

@bp.route('/get_clients_report', methods=['GET'])
def get_clients_report_route():
    return get_clients_report()

@bp.route('/get_accrued_interest', methods=['GET'])
def get_accrued_interest_route():
    return get_accrued_interest_report()