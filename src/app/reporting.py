from flask import Blueprint
from src.components.reporting import extract, transform, migrate

bp = Blueprint('reporting', __name__)

@bp.route('/extract', methods=['GET'])
def extract_route():
    response = extract()
    return response

@bp.route('/transform', methods=['GET'])
def transform_route():
    response = transform()
    return response

@bp.route('/migrate', methods=['POST'])
def migrate_route():
    response = migrate()
    return response