from flask import Blueprint, request
from src.components.tools.private.screenings import run_screenings
from src.components.tools.private.actions import (
    send_unfunded_emails,
    update_account_aliases,
    send_compliance_manual_update_email,
)
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


@bp.route('/send_compliance_manual_update_email', methods=['POST'])
@format_response
def send_compliance_manual_update_email_route():
    return send_compliance_manual_update_email()
