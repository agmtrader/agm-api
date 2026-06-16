
from flask import Blueprint, request
from src.components.clients.risk_profiles import create_risk_profile, read_risk_profiles, list_risk_archetypes
from src.utils.response import format_response

bp = Blueprint('risk_profiles', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create():
    """Create a risk profile assessment result."""
    payload = request.get_json(force=True)
    data = payload.get('data', None)
    return create_risk_profile(data=data)

@bp.route('/read', methods=['GET'])
@format_response
def read():
    """Read stored risk profile results filtered by id."""
    query = {}
    id = request.args.get('id', None)
    if id:
        query['id'] = id
    return read_risk_profiles(query=query)

@bp.route('/list', methods=['GET'])
@format_response
def list():
    """Read the available risk archetype definitions used to classify risk profiles."""
    return list_risk_archetypes()
