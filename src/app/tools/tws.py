from flask import Blueprint
from src.components.tools.tws import get_account_summary

bp = Blueprint('tws', __name__)

@bp.route('/account/summary', methods=['POST'])
def get_account_summary_route():
    return get_account_summary()