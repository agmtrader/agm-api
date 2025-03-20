from flask import Blueprint, request
from src.components.email import Gmail
from src.utils.scope_manager import verify_scope

bp = Blueprint('email', __name__)
Email = Gmail()

@bp.route('/send_email', methods=['POST'])
@verify_scope('email/send_email')
def send_email_route():
  payload = request.get_json(force=True)
  return Email.send_email(payload['content'], payload['client_email'], payload['subject'], payload['email_template'])