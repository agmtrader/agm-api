from flask import Blueprint, request
from src.utils.secret_manager import get_secret

bp = Blueprint('secret_manager', __name__)

@bp.route('/get_secret', methods=['POST'])
def get_secret_route():
    payload = request.get_json(force=True)
    secret_name = payload['secret_name']
    return get_secret(secret_name)