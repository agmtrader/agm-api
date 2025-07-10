from flask import Blueprint, request
from src.components.tools.email import Gmail
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('email', __name__)
Email = Gmail()

@bp.route('/send_email', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_route():
  payload = request.get_json(force=True)
  return Email.send_email(payload['content'], payload['client_email'], payload['subject'], payload['email_template'])


""" Custom emails """
@bp.route('/send_email/account_access', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_account_access_route():
  payload = request.get_json(force=True)
  return Email.send_email(payload['content'], payload['client_email'], 'Accesos a su nueva cuenta AGM', 'account_access')

@bp.route('/send_email/trade_ticket', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_trade_ticket_route():
  payload = request.get_json(force=True)
  return Email.send_email(payload['content'], payload['client_email'], 'Confirmación de Transacción', 'trade_ticket')

@bp.route('/send_email/email_change', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_change_route():
  payload = request.get_json(force=True)
  client_email = payload['client_email']
  advisor_email = payload['advisor_email']
  return Email.send_email("", client_email, 'Urgente: Actualización de Correo Electrónico', 'email_change', bcc="", cc="jc@agmtechnology.com,hc@agmtechnology.com," + advisor_email)

@bp.route('/send_email/two_factor_reminder', methods=['POST'])
@verify_scope('email/send_email')
@format_response
def send_email_two_factor_reminder_route():
  payload = request.get_json(force=True)
  return Email.send_email(payload['content'], payload['client_email'], 'Urgente: Activación de Autenticación de Dos Factores', 'two_factor_reminder', bcc="cr@agmtechnology.com,aa@agmtechnology.com,rc@agmtechnology.com", cc="jc@agmtechnology.com,hc@agmtechnology.com")