from flask import Blueprint, request
from src.components.applications import create_application, read_applications, send_to_ibkr, update_application
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response
from src.utils.logger import logger

bp = Blueprint('applications', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return create_application(application=application)

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    query = {}
    id = request.args.get('id', None)
    lead_id = request.args.get('lead_id', None)
    user_id = request.args.get('user_id', None)

    strip_application_param = request.args.get('strip_application', '0')
    if strip_application_param not in ['0', '1']:
        logger.error(f'Invalid strip_application parameter: {strip_application_param}')
        raise Exception('Invalid strip_application parameter')
    strip_application = strip_application_param == '1'

    if id:
        query['id'] = id
    if lead_id:
        query['lead_id'] = lead_id
    if user_id: 
        query['user_id'] = user_id
    applications = read_applications(query=query, strip_application=strip_application)

    return applications

@bp.route('/update', methods=['POST'])
@format_response
def update_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    query = payload.get('query', None)
    return update_application(application=application, query=query)

@bp.route('/send_to_ibkr', methods=['POST'])
@format_response
def send_to_ibkr_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    master_account = payload.get('master_account', 'ad')
    return send_to_ibkr(application=application, master_account=master_account)