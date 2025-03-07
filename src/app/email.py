from flask import Blueprint, request
from src.components.email import Gmail

bp = Blueprint('email', __name__)
Email = Gmail()

@bp.route('/send_client_email', methods=['POST'])
def send_client_email_route():
  payload = request.get_json(force=True)
  return Email.send_client_email(payload['plain_text'], payload['client_email'], payload['subject'])