from flask import Blueprint, request
from src.components.fee_template_requests import read_fee_template_requests, create_fee_template_request
from src.utils.response import format_response
from src.utils.logger import logger
from src.utils.exception import ServiceError

bp = Blueprint('fee_template_requests', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create():
    payload = request.get_json(force=True)
    fee_template_request = payload['fee_template_request']
    
    fee_template_request = create_fee_template_request(fee_template_request=fee_template_request)
    return fee_template_request

@bp.route('/read', methods=['GET'])
@format_response
def read_fee_template_requests_route():
    query = {}
    id = request.args.get('id', None)
    account_id = request.args.get('account_id', None)
    if id:
        query['id'] = id
    if account_id:
        query['account_id'] = account_id
    return read_fee_template_requests(query=query)