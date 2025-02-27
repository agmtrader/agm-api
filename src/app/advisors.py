from flask import Blueprint
from src.components.advisors import get_commissions
bp = Blueprint('advisors', __name__)

@bp.route('/get_commissions', methods=['GET'])
def get_commissions_route():
    return get_commissions()