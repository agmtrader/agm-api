from flask import Blueprint, request
from src.components.tools.email import Gmail
from src.utils.response import format_response

bp = Blueprint('email', __name__)
Email = Gmail()

@bp.route('/send_email/trade_ticket', methods=['POST'])
@format_response
def send_trade_ticket_email_route():
  payload = request.get_json(force=True)
  return Email.send_trade_ticket_email(payload['content'], payload['client_email'])

@bp.route('/send_email/email_confirmation', methods=['POST'])
@format_response
def send_confirmation_email_route():
  payload = request.get_json(force=True)
  return Email.send_email_confirmation(payload['content'], payload['client_email'], payload['lang'])

@bp.route('/send_email/application_link', methods=['POST'])
@format_response
def send_application_link_email_route():
  payload = request.get_json(force=True)
  return Email.send_application_link_email(payload['content'], payload['client_email'], payload['lang'])

@bp.route('/send_email/task_reminder', methods=['POST'])
@format_response
def send_task_reminder_email_route():
  payload = request.get_json(force=True)
  return Email.send_task_reminder_email(payload['content'], payload['agm_user_email'])

@bp.route('/send_email/lead_reminder', methods=['POST'])
@format_response
def send_lead_reminder_email_route():
  payload = request.get_json(force=True)
  return Email.send_lead_reminder_email(payload['content'], payload['agm_user_email'])

@bp.route('/send_email/credentials', methods=['POST'])
@format_response
def send_credentials_email_route():
  payload = request.get_json(force=True)
  return Email.send_credentials_email(payload['content'], payload['client_email'], payload['lang'])

@bp.route('/send_email/transfer_instructions', methods=['POST'])
@format_response
def send_transfer_instructions_email_route():
  payload = request.get_json(force=True)
  return Email.send_transfer_instructions_email(payload['content'], payload['client_email'], payload['lang'])

# ------------------------------------------------------------------
# NEW: Welcome email after account funding
# ------------------------------------------------------------------

@bp.route('/send_email/welcome', methods=['POST'])
@format_response
def send_welcome_email_route():
  """HTTP POST body must include: content (dict), client_email (str), lang ('en'|'es')"""
  payload = request.get_json(force=True)
  return Email.send_welcome_email(payload['content'], payload['client_email'], payload.get('lang', 'es'))