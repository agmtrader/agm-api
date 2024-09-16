from flask import Blueprint
from app.modules.reporting import generate

bp = Blueprint('reporting', __name__)

@bp.route('/reporting/generate', methods=['POST'])
def generate_route():
    generate()
    return {'status': 'success'}