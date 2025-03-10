from flask import Blueprint, request
from src.components.email import Gmail

bp = Blueprint('email', __name__)
Email = Gmail()

@bp.route('/send_email', methods=['POST'])
def send_email_route():
  payload = request.get_json(force=True)
  return Email.send_email(payload['content'], payload['client_email'], payload['subject'], payload['email_template'])