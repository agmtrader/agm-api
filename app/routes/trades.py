from flask import request, Blueprint
from app.modules.trades import generate

bp = Blueprint('trades', __name__)

@bp.route('/trades/generate', methods=['POST'])
def generate_route():
    body = request.get_json(force=True)
    indices = body['indices'].split(',')
    indices = [int(index) for index in indices]
    response = generate(flex_query=body['trade'], indices=indices)
    return response