from flask import Blueprint, request

from app.helpers.google import Gmail
from app.helpers.response import Response

bp = Blueprint('email', __name__)
Email = Gmail()

@bp.route('/send_client_email', methods=['POST'])
def send_client_email_route():
  payload = request.get_json(force=True)
  response = Email.sendClientEmail(payload['plain_text'], payload['client_email'], payload['subject'])
  return Response.success(response)