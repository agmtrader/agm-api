from flask import Blueprint, request
from src.components.tools.public.email import Email
from src.utils.response import format_response

bp = Blueprint('email', __name__)

@bp.route('/send_email/trade_ticket', methods=['POST'])
@format_response
def send_trade_ticket_email_route():
  """Send the generated trade ticket email to the client."""
  payload = request.get_json(force=True)
  return Email.send_trade_ticket_email(payload['content'], payload['client_email'])

@bp.route('/send_email/email_confirmation', methods=['POST'])
@format_response
def send_confirmation_email_route():
  """Send the application email confirmation message to a client."""
  payload = request.get_json(force=True)
  return Email.send_email_confirmation(payload['content'], payload['client_email'], payload['lang'])

@bp.route('/send_email/application_link', methods=['POST'])
@format_response
def send_application_link_email_route():
  """Send a client-facing application link email."""
  payload = request.get_json(force=True)
  return Email.send_application_link_email(payload['content'], payload['client_email'], payload['lang'])

@bp.route('/send_email/task_reminder', methods=['POST'])
@format_response
def send_task_reminder_email_route():
  """Send an internal task reminder email to an AGM user."""
  payload = request.get_json(force=True)
  return Email.send_task_reminder_email(payload['content'], payload['agm_user_email'])

@bp.route('/send_email/lead_reminder', methods=['POST'])
@format_response
def send_lead_reminder_email_route():
  """Send an internal lead reminder email to an AGM user."""
  payload = request.get_json(force=True)
  return Email.send_lead_reminder_email(payload['content'], payload['agm_user_email'])

@bp.route('/send_email/credentials', methods=['POST'])
@format_response
def send_credentials_email_route():
  """Send platform credential details to a client."""
  payload = request.get_json(force=True)
  return Email.send_credentials_email(content=payload['content'], client_email=payload['client_email'], lang=payload['lang'], cc=payload['cc'])

@bp.route('/send_email/transfer_instructions', methods=['POST'])
@format_response
def send_transfer_instructions_email_route():
  """Send transfer instructions to a client for an initial or existing funding flow."""
  payload = request.get_json(force=True)
  return Email.send_transfer_instructions_email(content=payload['content'], client_email=payload['client_email'], lang=payload['lang'], cc=payload['cc'], initial=payload['initial'])

@bp.route('/send_email/welcome', methods=['POST'])
@format_response
def send_welcome_email_route():
  """Send the welcome email to a new client."""
  payload = request.get_json(force=True)
  return Email.send_welcome_email(payload['content'], payload['client_email'], payload.get('lang', 'es'))

@bp.route('/send_email/funding_notification', methods=['POST'])
@format_response
def send_funding_notification_email_route():
  """Send a funding notification email to a client."""
  payload = request.get_json(force=True)
  return Email.send_funding_notification_email(payload['content'], payload['client_email'], payload.get('lang', 'es'), payload.get('cc', ''))

@bp.route('/send_email/missing_documents', methods=['POST'])
@format_response
def send_missing_documents_email_route():
  """Send a missing documents email to a client with the requested missing_type template."""
  payload = request.get_json(force=True)
  return Email.send_missing_documents_email(
    content=payload['content'],
    client_email=payload['client_email'],
    missing_type=payload.get('missing_type', 'multiple'),
    lang=payload.get('lang', 'en'),
    cc=payload.get('cc', ''),
  )
