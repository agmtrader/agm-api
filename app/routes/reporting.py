from flask import Blueprint
from app.modules.reporting import generate

bp = Blueprint('reporting', __name__)

@bp.route('/generate', methods=['GET'])
def generate_route():
    response = generate()
    return response