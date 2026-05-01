from flask import Blueprint, request
from src.components.tools.screenings import run_screenings
from src.components.tools.actions import send_unfunded_emails, update_account_aliases
from src.utils.response import format_response

bp = Blueprint('actions', __name__)

@bp.route('/send_unfunded_emails', methods=['GET'])
@format_response
def send_unfunded_emails_route():
    return send_unfunded_emails()

@bp.route('/update_pending_alias', methods=['PATCH'])
@format_response
def update_pending_aliases_route():
    return update_account_aliases()

@bp.route('/run_screening_process', methods=['GET'])
@format_response
def run_screenings_route():
    apply_screenings = request.args.get('apply_screenings', 'true').lower() == 'true'
    return run_screenings(apply_screenings=apply_screenings)
