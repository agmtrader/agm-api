from flask import Blueprint
from src.components.investment_proposals import backup_investment_proposals, read

bp = Blueprint('investment_proposals', __name__)

@bp.route('/read', methods=['GET'])
def read_route():
    return read()

@bp.route('/backup_investment_proposals', methods=['GET'])
def backup_investment_proposals_route():
    return backup_investment_proposals()
