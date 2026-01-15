from flask import request, Blueprint
from src.components.tools.trade_request import create_trade_request, read_trade_request
from src.utils.response import format_response

bp = Blueprint('trade_request', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_route():
    payload = request.get_json(force=True)
    side = payload['side']
    quantity = payload['quantity']
    order_type = payload['order_type']
    time_in_force = payload['time_in_force']
    trade_request = create_trade_request(side=side, quantity=quantity, order_type=order_type, time_in_force=time_in_force)
    return trade_request

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    id = request.args.get('id', None)
    trade_request = read_trade_request(id=id)
    return trade_request

