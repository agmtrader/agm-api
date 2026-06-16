from flask import Blueprint, request
from src.components.clients.applications import create_application, read_applications, update_application
from src.utils.response import format_response
from src.utils.logger import logger

bp = Blueprint('applications', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_route():
    """Create an application record."""
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return create_application(application=application)

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    """Read applications by id or user_id, with an option to strip the raw application payload from the response."""
    query = {}
    id = request.args.get('id', None)
    user_id = request.args.get('user_id', None)

    strip_application_param = request.args.get('strip_application', '0')
    if strip_application_param not in ['0', '1']:
        logger.error(f'Invalid strip_application parameter: {strip_application_param}')
        raise Exception('Invalid strip_application parameter')
    strip_application = strip_application_param == '1'

    if id:
        query['id'] = id
    if user_id: 
        query['user_id'] = user_id
    applications = read_applications(query=query, strip_application=strip_application)

    return applications

@bp.route('/update', methods=['POST'])
@format_response
def update_route():
    """Update application records selected by the provided query payload."""
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    query = payload.get('query', None)
    return update_application(application=application, query=query)
