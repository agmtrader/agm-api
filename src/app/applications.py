from flask import Blueprint, request
from src.components.applications import create_application, read_applications, send_to_ibkr, get_forms
from src.utils.managers.scope_manager import verify_scope, enforce_user_filter
from src.utils.response import format_response

bp = Blueprint('applications', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('applications/create')
@enforce_user_filter()
@format_response
def create_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return create_application(application=application)

@bp.route('/read', methods=['POST'])
@verify_scope('applications/read')
@enforce_user_filter()
@format_response
def read_route():
    return read_applications()

@bp.route('/send_to_ibkr', methods=['POST'])
@verify_scope('applications/send')
@enforce_user_filter()
@format_response
def send_to_ibkr_route():
    payload = request.get_json(force=True)
    application = payload.get('application', None)
    return send_to_ibkr(application=application)

@bp.route('/forms', methods=['POST'])
@verify_scope('applications/forms')
@enforce_user_filter()
@format_response
def get_forms_route():
    payload = request.get_json(force=True)
    forms = payload.get('forms', None)
    return get_forms(forms=forms)