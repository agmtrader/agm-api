from flask import request, Blueprint
import requests as rq

from app.modules.trade_tickets import generate_trade_ticket, generate_client_confirmation_message


from app.helpers.logger import logger

bp = Blueprint('trade_tickets', __name__)
url = 'http://127.0.0.1:5001'

@bp.route('/generate_trade_ticket', methods=['POST'])
def generate_route():
    payload = request.get_json(force=True)
    indices = payload['indices'].split(',')
    indices = [int(index) for index in indices]
    flex_query_dict = payload['flex_query_dict']
    response = generate_trade_ticket(flex_query_dict=flex_query_dict, indices=indices)
    return response

@bp.route('/generate_client_confirmation_message', methods=['POST'])
def generate_client_confirmation_message_route():
    logger.info('Processing trade data.')
    payload = request.get_json(force=True)
    trade_data = payload['trade_data']
    response = generate_client_confirmation_message(trade_data)
    return response