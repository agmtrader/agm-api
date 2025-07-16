from flask import Blueprint, request
from src.components.tools.email import Gmail
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('email', __name__)
Email = Gmail()

""" Custom emails """
@bp.route('/send_email/account_access', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_account_access_route():
  payload = request.get_json(force=True)
  return Email.send_account_access_email(payload['content'], payload['client_email'])

@bp.route('/send_email/trade_ticket', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_trade_ticket_route():
  payload = request.get_json(force=True)
  return Email.send_trade_ticket_email(payload['content'], payload['client_email'])

@bp.route('/send_email/email_change', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_change_route():
  payload = request.get_json(force=True)
  client_email = payload['client_email']
  advisor_email = payload['advisor_email']
  return Email.send_email_change_email(client_email, advisor_email)

@bp.route('/send_email/two_factor_reminder', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_two_factor_reminder_route():
  payload = request.get_json(force=True)
  return Email.send_two_factor_reminder_email(payload['content'], payload['client_email'])