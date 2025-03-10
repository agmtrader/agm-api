from flask import Blueprint, request
from src.components.advisors import read_commissions, read_advisors
bp = Blueprint('advisors', __name__)

@bp.route('/commissions', methods=['GET'])
def commissions_route():
    return read_commissions()

@bp.route('/read', methods=['POST'])
def advisors_route():
    body = request.get_json(force=True)
    query = body['query']
    return read_advisors(query)