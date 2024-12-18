from flask import Blueprint, request
from src.components.investment_proposals import backup_investment_proposals

bp = Blueprint('investment_proposals', __name__)

@bp.route('/backup_investment_proposals', methods=['GET'])
def backup_investment_proposals_route():
    return backup_investment_proposals()