from flask import Blueprint, request
from src.components.tools.email import Gmail
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('email', __name__)
Email = Gmail()

@bp.route('/send_email/trade_ticket', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_trade_ticket_email_route():
  payload = request.get_json(force=True)
  return Email.send_trade_ticket_email(payload['content'], payload['client_email'])

@bp.route('/send_email/email_confirmation', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_confirmation_email_route():
  payload = request.get_json(force=True)
  return Email.send_email_confirmation(payload['content'], payload['client_email'], payload['lang'])

@bp.route('/send_email/application_link', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_application_link_email_route():
  payload = request.get_json(force=True)
  return Email.send_application_link_email(payload['content'], payload['client_email'], payload['lang'])

@bp.route('/send_email/task_reminder', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_task_reminder_email_route():
  payload = request.get_json(force=True)
  return Email.send_task_reminder_email(payload['content'], payload['agm_user_email'])

@bp.route('/send_email/credentials', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_credentials_email_route():
  payload = request.get_json(force=True)
  return Email.send_credentials_email(payload['content'], payload['client_email'], payload['lang'])