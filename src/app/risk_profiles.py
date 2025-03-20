
from flask import Blueprint, request
from src.components.risk_profiles import create_risk_profile, read_risk_profiles
from src.utils.scope_manager import verify_scope

bp = Blueprint('risk_profiles', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('risk_profiles/create')
def create():
    payload = request.get_json(force=True)
    data = payload['data']
    id = payload['id']
    return create_risk_profile(data=data, id=id)

@bp.route('/read', methods=['GET'])
@verify_scope('risk_profiles/read')
def read():
    return read_risk_profiles()