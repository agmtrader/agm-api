from flask import Blueprint, request
from app.modules.reporting import extract, transform, load

bp = Blueprint('reporting', __name__)

@bp.route('/extract', methods=['GET'])
def extract_route():
    response = extract()
    return response

@bp.route('/transform', methods=['GET'])
def transform_route():
    response = transform()
    return response

@bp.route('/load', methods=['GET'])
def load_route():
    response = load()
    return response