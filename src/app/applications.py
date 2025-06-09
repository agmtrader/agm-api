from flask import Blueprint, request
from src.components.applications import create_application, read_applications, send_to_ibkr
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('applications', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('applications/create')
@format_response
def create_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return create_application(application=application)

@bp.route('/read', methods=['POST'])
@verify_scope('applications/read')
@format_response
def read_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_applications(query=query)

@bp.route('/send_to_ibkr', methods=['POST'])
@verify_scope('applications/send')
@format_response
def send_to_ibkr_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return send_to_ibkr(application=application)