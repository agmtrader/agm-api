from flask import Blueprint, request
from src.components.account_management import AccountManagement
from src.utils.scope_manager import verify_scope
bp = Blueprint('account_management', __name__)
account_management = AccountManagement()

@bp.route('/accounts', methods=['GET'])
@verify_scope('account_management/accounts')
def accounts_route():
    return account_management.get_accounts()