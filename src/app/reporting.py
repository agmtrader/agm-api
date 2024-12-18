from flask import Blueprint
from src.components.reporting import extract, transform

bp = Blueprint('reporting', __name__)

@bp.route('/extract', methods=['GET'])
def extract_route():
    return extract()

@bp.route('/transform', methods=['GET'])
def transform_route():
    return transform()