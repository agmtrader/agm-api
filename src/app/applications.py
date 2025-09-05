from flask import Blueprint, request
from src.components.applications import create_application, read_applications, send_to_ibkr, update_application
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response
from src.utils.logger import logger

bp = Blueprint('applications', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('applications/create')
@format_response
def create_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return create_application(application=application)

@bp.route('/read', methods=['GET'])
@verify_scope('applications/read')
@format_response
def read_route():
    query = {}
    id = request.args.get('id', None)
    lead_id = request.args.get('lead_id', None)
    user_id = request.args.get('user_id', None)

    strip_application_param = request.args.get('strip_application', '0')
    if strip_application_param == '0':
        strip_application = False
    elif strip_application_param == '1':
        strip_application = True
    else:
        logger.error(f'Invalid strip_application parameter: {strip_application_param}')
        raise Exception('Invalid strip_application parameter')

    if id:
        query['id'] = id
    if lead_id:
        query['lead_id'] = lead_id
    if user_id: 
        query['user_id'] = user_id
    applications = read_applications(query=query)

    print(f"Strip application: {strip_application}")

    if strip_application == True and isinstance(applications, list):
        for application in applications:
            if isinstance(application, dict):
                application.pop('application', None)
    
    return applications

@bp.route('/update', methods=['POST'])
@verify_scope('applications/update')
@format_response
def update_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    query = payload.get('query', None)
    return update_application(application=application, query=query)

@bp.route('/send_to_ibkr', methods=['POST'])
@verify_scope('applications/send')
@format_response
def send_to_ibkr_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return send_to_ibkr(application=application)