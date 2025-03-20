from flask import Blueprint
from src.components.investment_proposals import backup_investment_proposals, read
from src.utils.scope_manager import verify_scope

bp = Blueprint('investment_proposals', __name__)

@bp.route('/read', methods=['GET'])
@verify_scope('investment_proposals/read')
def read_route():
    return read()

@bp.route('/backup_investment_proposals', methods=['GET'])
@verify_scope('investment_proposals/backup_investment_proposals')
def backup_investment_proposals_route():
    return backup_investment_proposals()
