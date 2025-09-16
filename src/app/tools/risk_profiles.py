
from flask import Blueprint, request
from src.components.tools.risk_profiles import create_risk_profile, read_risk_profiles, list_risk_archetypes
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('risk_profiles', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create():
    payload = request.get_json(force=True)
    data = payload.get('data', None)
    return create_risk_profile(data=data)

@bp.route('/read', methods=['GET'])
@format_response
def read():
    query = {}
    id = request.args.get('id', None)
    if id:
        query['id'] = id
    return read_risk_profiles(query=query)

@bp.route('/list', methods=['GET'])
@format_response
def list():
    return list_risk_archetypes()